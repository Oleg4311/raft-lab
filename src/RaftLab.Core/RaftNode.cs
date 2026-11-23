namespace RaftLab.Core;

/// <summary>
/// Один узел Raft. Чистый конечный автомат: время и сообщения приходят снаружи (Tick, Receive, Propose), исходящие сообщения
/// складываются в список. Ни потоков, ни сети, ни часов: поэтому симулятор воспроизводит любой прогон по зерну.
/// Нумерация по статье «In Search of an Understandable Consensus Algorithm» (Ongaro, Ousterhout), рис. 2.
/// </summary>
public sealed class RaftNode
{
    private readonly int[] _all;
    private readonly PersistentState _p;
    private readonly RaftOptions _o;
    private readonly Random _rng;
    private readonly Dictionary<int, long> _next = new();
    private readonly Dictionary<int, long> _match = new();
    private readonly HashSet<int> _votes = new();
    private readonly List<AppliedEntry> _applied = new();
    private long _now;
    private long _electionDeadline;
    private long _nextHeartbeat;

    public RaftNode(int id, int[] allIds, PersistentState persistent, RaftOptions options, Random rng, long now)
    {
        Id = id;
        _all = allIds;
        _p = persistent;
        _o = options;
        _rng = rng;
        _now = now;
        Machine = new KvStateMachine((options.Bugs & RaftBug.NoDedup) == 0);
        ResetElectionTimer();
    }

    public int Id { get; }
    public Role Role { get; private set; } = Role.Follower;
    public int? LeaderHint { get; private set; }
    public long CommitIndex { get; private set; }
    public long LastApplied { get; private set; }
    public KvStateMachine Machine { get; }
    public long Term => _p.CurrentTerm;
    public int? VotedFor => _p.VotedFor;
    public IReadOnlyList<LogEntry> Log => _p.Log;
    public long LastIndex => _p.LastIndex;

    /// <summary>Вызывается, когда узел становится лидером (для проверки «не больше одного лидера в терме»).</summary>
    public Action<RaftNode>? OnBecameLeader { get; set; }

    public IReadOnlyDictionary<int, long> MatchIndex => _match;
    public IReadOnlyDictionary<int, long> NextIndex => _next;

    /// <summary>Применённые с прошлого вызова записи (и их результаты).</summary>
    public List<AppliedEntry> TakeApplied()
    {
        var copy = new List<AppliedEntry>(_applied);
        _applied.Clear();
        return copy;
    }

    private bool Has(RaftBug b) => (_o.Bugs & b) != 0;
    private int Majority => _all.Length / 2 + 1;
    private IEnumerable<int> Peers => _all.Where(x => x != Id);

    private void ResetElectionTimer() => _electionDeadline = _now + _rng.Next(_o.ElectionTimeoutMin, _o.ElectionTimeoutMax + 1);

    // ---------- время ----------

    public void Tick(long now, List<Message> outbox)
    {
        _now = now;
        if (Role == Role.Leader)
        {
            if (now >= _nextHeartbeat) BroadcastAppend(outbox);
        }
        else if (now >= _electionDeadline) StartElection(outbox);
    }

    private void StartElection(List<Message> outbox)
    {
        _p.CurrentTerm++;
        _p.VotedFor = Id;
        Role = Role.Candidate;
        LeaderHint = null;
        _votes.Clear();
        _votes.Add(Id);
        ResetElectionTimer();
        if (_all.Length == 1)
        {
            BecomeLeader(outbox);
            return;
        }
        foreach (var peer in Peers) outbox.Add(new RequestVote(Id, peer, _p.CurrentTerm, _p.LastIndex, _p.LastTerm));
    }

    private void BecomeLeader(List<Message> outbox)
    {
        Role = Role.Leader;
        LeaderHint = Id;
        foreach (var peer in Peers)
        {
            _next[peer] = _p.LastIndex + 1;
            _match[peer] = 0;
        }
        // пустая запись нового терма: только через неё можно безопасно закоммитить записи прошлых термов (раздел 5.4.2)
        _p.Log.Add(new LogEntry(_p.CurrentTerm, Command.Noop));
        OnBecameLeader?.Invoke(this);
        BroadcastAppend(outbox);
        AdvanceCommit();
    }

    private void StepDown(long term)
    {
        _p.CurrentTerm = term;
        _p.VotedFor = null;
        Role = Role.Follower;
        _votes.Clear();
    }

    // ---------- сообщения ----------

    public void Receive(Message m, long now, List<Message> outbox)
    {
        _now = now;
        if (m.Term > _p.CurrentTerm)
        {
            StepDown(m.Term);
            LeaderHint = null;
        }
        switch (m)
        {
            case RequestVote rv: HandleRequestVote(rv, outbox); break;
            case RequestVoteReply rr: HandleVoteReply(rr, outbox); break;
            case AppendEntries ae: HandleAppendEntries(ae, outbox); break;
            case AppendEntriesReply ar: HandleAppendReply(ar, outbox); break;
        }
    }

    private void HandleRequestVote(RequestVote m, List<Message> outbox)
    {
        var upToDate = Has(RaftBug.NoUpToDateCheck) || m.LastLogTerm > _p.LastTerm || (m.LastLogTerm == _p.LastTerm && m.LastLogIndex >= _p.LastIndex);
        var grant = m.Term == _p.CurrentTerm && (_p.VotedFor is null || _p.VotedFor == m.From) && upToDate;
        if (grant)
        {
            _p.VotedFor = m.From;
            ResetElectionTimer();
        }
        outbox.Add(new RequestVoteReply(Id, m.From, _p.CurrentTerm, grant));
    }

    private void HandleVoteReply(RequestVoteReply m, List<Message> outbox)
    {
        if (Role != Role.Candidate || m.Term != _p.CurrentTerm || !m.Granted) return;
        _votes.Add(m.From);
        if (_votes.Count >= Majority) BecomeLeader(outbox);
    }

    private void HandleAppendEntries(AppendEntries m, List<Message> outbox)
    {
        if (m.Term < _p.CurrentTerm)
        {
            outbox.Add(new AppendEntriesReply(Id, m.From, _p.CurrentTerm, false, 0, 0, 0));
            return;
        }
        Role = Role.Follower; // тот же терм: кандидат уступает настоящему лидеру
        LeaderHint = m.From;
        ResetElectionTimer();

        if (m.PrevLogIndex > _p.LastIndex)
        {
            outbox.Add(new AppendEntriesReply(Id, m.From, _p.CurrentTerm, false, 0, 0, _p.LastIndex + 1));
            return;
        }
        if (m.PrevLogIndex > 0 && _p.TermAt(m.PrevLogIndex) != m.PrevLogTerm)
        {
            var ct = _p.TermAt(m.PrevLogIndex);
            var first = m.PrevLogIndex;
            while (first > 1 && _p.TermAt(first - 1) == ct) first--;
            outbox.Add(new AppendEntriesReply(Id, m.From, _p.CurrentTerm, false, 0, ct, first));
            return;
        }

        if (Has(RaftBug.TruncateAlways) && m.Entries.Count > 0)
        {
            _p.Log.RemoveRange((int)m.PrevLogIndex, _p.Log.Count - (int)m.PrevLogIndex);
            _p.Log.AddRange(m.Entries);
        }
        else
        {
            var idx = m.PrevLogIndex;
            foreach (var e in m.Entries)
            {
                idx++;
                if (idx <= _p.LastIndex)
                {
                    if (_p.TermAt(idx) == e.Term) continue; // уже есть: ничего не трогаем
                    _p.Log.RemoveRange((int)idx - 1, _p.Log.Count - ((int)idx - 1)); // конфликт: удаляем запись и всё после неё
                }
                _p.Log.Add(e);
            }
        }
        var match = m.PrevLogIndex + m.Entries.Count;
        if (m.LeaderCommit > CommitIndex)
        {
            CommitIndex = Math.Min(m.LeaderCommit, match);
            ApplyCommitted();
        }
        outbox.Add(new AppendEntriesReply(Id, m.From, _p.CurrentTerm, true, match, 0, 0));
    }

    private void HandleAppendReply(AppendEntriesReply m, List<Message> outbox)
    {
        if (Role != Role.Leader || m.Term != _p.CurrentTerm) return;
        var peer = m.From;
        if (m.Success)
        {
            if (m.MatchIndex > _match[peer]) _match[peer] = m.MatchIndex;
            _next[peer] = Math.Max(_next[peer], _match[peer] + 1);
            AdvanceCommit();
            if (_next[peer] <= _p.LastIndex) SendAppend(peer, outbox);
            return;
        }
        long ni;
        if (m.ConflictTerm > 0)
        {
            long found = 0;
            for (var i = _p.LastIndex; i >= 1; i--)
                if (_p.TermAt(i) == m.ConflictTerm)
                {
                    found = i;
                    break;
                }
            ni = found > 0 ? found + 1 : m.ConflictIndex;
        }
        else ni = m.ConflictIndex;
        _next[peer] = Math.Max(_match[peer] + 1, Math.Max(1, Math.Min(ni, _next[peer] - 1)));
        SendAppend(peer, outbox);
    }

    // ---------- репликация и коммит ----------

    private void BroadcastAppend(List<Message> outbox)
    {
        foreach (var peer in Peers) SendAppend(peer, outbox);
        _nextHeartbeat = _now + _o.HeartbeatInterval;
    }

    private void SendAppend(int peer, List<Message> outbox)
    {
        var next = _next[peer];
        var prev = next - 1;
        var count = (int)Math.Max(0, Math.Min(_o.MaxBatch, _p.LastIndex - prev));
        var entries = count == 0 ? Array.Empty<LogEntry>() : _p.Log.GetRange((int)prev, count).ToArray();
        outbox.Add(new AppendEntries(Id, peer, _p.CurrentTerm, prev, _p.TermAt(prev), entries, CommitIndex));
    }

    private void AdvanceCommit()
    {
        for (var n = _p.LastIndex; n > CommitIndex; n--)
        {
            if (!Has(RaftBug.CommitPreviousTermByCount) && _p.TermAt(n) != _p.CurrentTerm) break; // записи прошлых термов коммитятся только косвенно
            var count = 1 + Peers.Count(p => _match[p] >= n);
            if (count >= Majority)
            {
                CommitIndex = n;
                break;
            }
        }
        ApplyCommitted();
    }

    private void ApplyCommitted()
    {
        while (LastApplied < CommitIndex)
        {
            LastApplied++;
            var e = _p.Log[(int)LastApplied - 1];
            _applied.Add(new AppliedEntry(LastApplied, e, Machine.Apply(e.Command)));
        }
    }

    // ---------- клиент ----------

    public ProposeResult Propose(Command c, List<Message> outbox)
    {
        if (Role != Role.Leader) return new NotLeader(LeaderHint);
        if (Has(RaftBug.LocalReads) && c.Kind == CommandKind.Get) return new LocalAnswer(Machine.Peek(c.Key));
        _p.Log.Add(new LogEntry(_p.CurrentTerm, c));
        var index = _p.LastIndex;
        BroadcastAppend(outbox);
        AdvanceCommit(); // для кластера из одного узла
        return new Accepted(index, _p.CurrentTerm);
    }
}
