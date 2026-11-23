using RaftLab.Core;

namespace RaftLab.Sim;

public sealed record SimConfig
{
    public long Seed { get; init; } = 1;
    public int Nodes { get; init; } = 5;
    public RaftOptions Raft { get; init; } = new();
    public int MinDelay { get; init; } = 1;
    public int MaxDelay { get; init; } = 4;
    public double DropProb { get; init; } = 0.02;
    public double DuplicateProb { get; init; } = 0.02;
    public int Clients { get; init; } = 3;
    /// <summary>Вероятность, что свободный клиент начнёт новую операцию в этом тике.</summary>
    public double OpProb { get; init; } = 0.15;
    public int ClientTimeout { get; init; } = 50;
    public int Keys { get; init; } = 3;
    public int MaxOpsPerKey { get; init; } = 40;
    /// <summary>Тики хаоса (случайные разрывы сети и падения узлов). После них всё чинится и кластер должен успокоиться.</summary>
    public int ChaosTicks { get; init; } = 1500;
    public double FaultProb { get; init; } = 0.012;
    /// <summary>Тики спокойного периода после починки: клиенты заканчивают операции, узлы сходятся.</summary>
    public int SettleTicks { get; init; } = 500;
}

public enum Fate { InFlight, Delivered, Dropped, Cut, Dead }

public sealed class InFlight
{
    public required int Id { get; init; }
    public required Message Message { get; init; }
    public required long SentAt { get; init; }
    public required long DeliverAt { get; init; }
    public bool Duplicate { get; init; }
    public Fate Fate { get; set; } = Fate.InFlight;
}

/// <summary>Одна операция клиента в истории: от первой отправки до первого полученного ответа (или без ответа).</summary>
public sealed class HistoryOp
{
    public required int Client { get; init; }
    public required long Seq { get; init; }
    public required CommandKind Kind { get; init; }
    public required string Key { get; init; }
    public string? Value { get; init; }
    public string? Expected { get; init; }
    public required long Invoke { get; init; }
    public long Return { get; set; } = -1;
    public CommandResult? Result { get; set; }
    public bool Completed => Return >= 0;

    public override string ToString() => $"c{Client}#{Seq} {Kind} {Key}" + (Kind == CommandKind.Cas ? $" {Expected ?? "∅"}→{Value}" : Kind == CommandKind.Put ? $"={Value}" : "") + $" [{Invoke}..{(Completed ? Return : "∞")}]" + (Completed ? $" → {(Result!.Ok ? "ok" : "no")}:{Result.Value ?? "∅"}" : "");
}

/// <summary>Кластер из N узлов Raft в дискретном времени с управляемой сетью и сбоями. Всё определяется зерном.</summary>
public sealed class Cluster
{
    private sealed class Client
    {
        public required int Index { get; init; }
        public long Seq { get; set; }
        public HistoryOp? Op { get; set; }
        public Command? Cmd { get; set; }
        public long LastAttempt { get; set; }
        public int Target { get; set; }
    }

    private readonly SimConfig _cfg;
    private readonly int[] _ids;
    private readonly PersistentState[] _persist;
    private readonly RaftNode?[] _nodes;
    private readonly int[] _group;
    private readonly SimRandom _net;
    private readonly SimRandom _fault;
    private readonly SimRandom _work;
    private readonly PriorityQueue<InFlight, (long, long)> _queue = new();
    private readonly Dictionary<(int Node, long Index, long Term), Client> _pending = new();
    private readonly List<Client> _clients = new();
    private readonly Dictionary<string, int> _opsPerKey = new();
    private readonly Dictionary<long, Action> _script = new();
    private int _msgId;
    private long _qSeq;
    private bool _clientsActive = true;

    public Cluster(SimConfig cfg)
    {
        _cfg = cfg;
        _ids = Enumerable.Range(0, cfg.Nodes).ToArray();
        _persist = _ids.Select(_ => new PersistentState()).ToArray();
        _nodes = new RaftNode?[cfg.Nodes];
        _group = new int[cfg.Nodes];
        _net = SimRandom.Derive(cfg.Seed, 1);
        _fault = SimRandom.Derive(cfg.Seed, 2);
        _work = SimRandom.Derive(cfg.Seed, 3);
        for (var i = 0; i < cfg.Clients; i++) _clients.Add(new Client { Index = i, Target = _work.Next(cfg.Nodes) });
        foreach (var id in _ids) Boot(id);
        Checks = new Invariants(this);
    }

    public SimConfig Config => _cfg;
    public long Now { get; private set; }
    public Invariants Checks { get; }
    public List<HistoryOp> History { get; } = new();
    public TraceRecorder? Trace { get; set; }
    public IReadOnlyList<RaftNode?> Nodes => _nodes;
    public IReadOnlyList<PersistentState> Persistent => _persist;
    public IReadOnlyList<int> Groups => _group;
    public IReadOnlyCollection<InFlight> InFlightMessages => _queue.UnorderedItems.Select(x => x.Element).ToList();
    public bool IsAlive(int id) => _nodes[id] is not null;
    public int Majority => _cfg.Nodes / 2 + 1;
    public int PendingClientOps => _clients.Count(c => c.Op is not null);

    private void Boot(int id)
    {
        var node = new RaftNode(id, _ids, _persist[id], _cfg.Raft, SimRandom.Derive(_cfg.Seed, 100 + (ulong)id * 7919 + (ulong)Now), Now);
        node.OnBecameLeader = n => Checks.OnLeader(n);
        _nodes[id] = node;
    }

    // ---------- сбои ----------

    public void Crash(int id)
    {
        if (_nodes[id] is null) return;
        _nodes[id] = null;
        foreach (var k in _pending.Keys.Where(k => k.Node == id).ToList()) _pending.Remove(k);
        Trace?.Event(Now, $"узел {id} упал");
    }

    public void Restart(int id)
    {
        if (_nodes[id] is not null) return;
        if ((_cfg.Raft.Bugs & RaftBug.VoteNotPersisted) != 0) _persist[id].VotedFor = null; // ошибка: голос не дожил до перезапуска
        Boot(id);
        Trace?.Event(Now, $"узел {id} перезапущен (журнал {_persist[id].Log.Count} зап., терм {_persist[id].CurrentTerm})");
    }

    /// <summary>Разбивает сеть на группы: сообщения между разными группами не доходят.</summary>
    public void Partition(params int[][] groups)
    {
        Array.Fill(_group, 0);
        for (var g = 0; g < groups.Length; g++)
            foreach (var n in groups[g]) _group[n] = g + 1;
        Trace?.Event(Now, "сеть разделена: " + string.Join(" | ", groups.Select(g => "{" + string.Join(",", g) + "}")));
    }

    public void Heal()
    {
        if (_group.All(g => g == 0)) return;
        Array.Fill(_group, 0);
        Trace?.Event(Now, "сеть починена");
    }

    public void At(long tick, Action action) => _script[tick] = action;

    public int? CurrentLeader()
    {
        RaftNode? best = null;
        foreach (var n in _nodes)
            if (n is { Role: Role.Leader } && (best is null || n.Term > best.Term)) best = n;
        return best?.Id;
    }

    // ---------- сеть ----------

    private string Describe(Message m) => m switch
    {
        RequestVote rv => $"RequestVote t={rv.Term} last={rv.LastLogIndex}/{rv.LastLogTerm}",
        RequestVoteReply rr => $"Vote {(rr.Granted ? "за" : "против")} t={rr.Term}",
        AppendEntries ae => ae.Entries.Count == 0 ? $"Heartbeat t={ae.Term} commit={ae.LeaderCommit}" : $"AppendEntries t={ae.Term} prev={ae.PrevLogIndex} +{ae.Entries.Count} commit={ae.LeaderCommit}",
        AppendEntriesReply ar => ar.Success ? $"Ack t={ar.Term} match={ar.MatchIndex}" : $"Nack t={ar.Term} конфликт с {ar.ConflictIndex}",
        _ => m.GetType().Name,
    };

    private void Send(Message m)
    {
        var dropped = _net.Chance(_cfg.DropProb);
        var delay = _net.Next(_cfg.MinDelay, _cfg.MaxDelay + 1);
        var dup = _net.Chance(_cfg.DuplicateProb);
        var dupDelay = _net.Next(_cfg.MinDelay, _cfg.MaxDelay * 3 + 2);
        var msg = new InFlight { Id = _msgId++, Message = m, SentAt = Now, DeliverAt = Now + delay };
        if (dropped || _group[m.From] != _group[m.To])
        {
            msg.Fate = dropped ? Fate.Dropped : Fate.Cut;
            Trace?.Message(msg, Describe(m));
            return;
        }
        Enqueue(msg);
        Trace?.Message(msg, Describe(m));
        if (dup)
        {
            var copy = new InFlight { Id = _msgId++, Message = m, SentAt = Now, DeliverAt = Now + dupDelay, Duplicate = true };
            Enqueue(copy);
            Trace?.Message(copy, Describe(m) + " (дубль)");
        }
    }

    private void Enqueue(InFlight f) => _queue.Enqueue(f, (f.DeliverAt, _qSeq++));

    private void Dispatch(List<Message> outbox)
    {
        foreach (var m in outbox) Send(m);
        outbox.Clear();
    }

    // ---------- шаг ----------

    public void StopClients() => _clientsActive = false;

    /// <summary>Один тик: сценарные действия, доставка сообщений, часы узлов, клиенты, проверки.</summary>
    public void Step()
    {
        var outbox = new List<Message>();
        if (_script.Remove(Now, out var act)) act();

        while (_queue.TryPeek(out var top, out var pr) && pr.Item1 <= Now)
        {
            _queue.Dequeue();
            var m = top.Message;
            var node = _nodes[m.To];
            if (node is null) top.Fate = Fate.Dead;
            else if (_group[m.From] != _group[m.To]) top.Fate = Fate.Cut;
            else
            {
                top.Fate = Fate.Delivered;
                node.Receive(m, Now, outbox);
                Dispatch(outbox);
                Drain(node);
            }
            Trace?.Fate(top);
        }

        foreach (var node in _nodes)
        {
            if (node is null) continue;
            node.Tick(Now, outbox);
            Dispatch(outbox);
            Drain(node);
        }

        StepClients();
        Checks.EndOfTick();
        Trace?.Frame(this);
        Now++;
    }

    /// <summary>Случайные сбои: разделение сети, починка, падение и перезапуск узлов.</summary>
    public void Chaos()
    {
        if (!_fault.Chance(_cfg.FaultProb)) return;
        switch (_fault.Next(5))
        {
            case 0:
            {
                var order = _ids.OrderBy(_ => _fault.NextU64()).ToArray();
                var cut = 1 + _fault.Next(_cfg.Nodes - 1);
                Partition(order.Take(cut).ToArray(), order.Skip(cut).ToArray());
                break;
            }
            case 1:
                Heal();
                break;
            case 2:
                Crash(_fault.Next(_cfg.Nodes));
                break;
            case 3:
            case 4:
            {
                var dead = _ids.Where(i => _nodes[i] is null).ToArray();
                if (dead.Length > 0) Restart(dead[_fault.Next(dead.Length)]);
                break;
            }
        }
    }

    public void HealEverything()
    {
        Heal();
        foreach (var id in _ids) Restart(id);
    }

    private void Drain(RaftNode node)
    {
        foreach (var a in node.TakeApplied())
        {
            Checks.OnApplied(node, a);
            if (_pending.Remove((node.Id, a.Index, a.Entry.Term), out var client) && client.Op is { } op && client.Cmd is { } cmd && a.Entry.Command == cmd)
                Complete(client, op, a.Result);
        }
    }

    private void Complete(Client client, HistoryOp op, CommandResult result)
    {
        op.Return = Now;
        op.Result = result;
        client.Op = null;
        client.Cmd = null;
        foreach (var k in _pending.Where(kv => kv.Value == client).Select(kv => kv.Key).ToList()) _pending.Remove(k);
        Trace?.Event(Now, $"клиент {client.Index}: {op.Kind} {op.Key} → {(result.Ok ? "ok" : "нет")}{(result.Value is null ? "" : ":" + result.Value)}");
    }

    // ---------- клиенты ----------

    private void StepClients()
    {
        foreach (var c in _clients)
        {
            if (c.Op is null)
            {
                if (!_clientsActive || !_work.Chance(_cfg.OpProb)) continue;
                var key = PickKey();
                if (key is null) continue;
                c.Seq++;
                var kind = _work.Next(10) switch { < 4 => CommandKind.Put, < 8 => CommandKind.Get, _ => CommandKind.Cas };
                var value = $"c{c.Index}-{c.Seq}";
                string? expected = null;
                if (kind == CommandKind.Cas)
                {
                    var seen = History.Where(h => h.Key == key && h.Kind is CommandKind.Put or CommandKind.Cas).ToList();
                    expected = seen.Count > 0 && _work.Chance(0.7) ? seen[_work.Next(seen.Count)].Value : null;
                }
                var cmd = new Command(kind, key, kind == CommandKind.Get ? null : value, kind == CommandKind.Cas ? expected : null, $"c{c.Index}", c.Seq);
                c.Cmd = cmd;
                c.Op = new HistoryOp { Client = c.Index, Seq = c.Seq, Kind = kind, Key = key, Value = cmd.Value, Expected = cmd.Expected, Invoke = Now };
                History.Add(c.Op);
                _opsPerKey[key] = _opsPerKey.GetValueOrDefault(key) + 1;
                Attempt(c);
            }
            else if (Now - c.LastAttempt >= _cfg.ClientTimeout)
            {
                c.Target = _work.Next(_cfg.Nodes); // повтор той же операции (тот же Seq) на другой узел
                Attempt(c);
            }
            else if (Now - c.LastAttempt >= 3 && !_pending.Values.Contains(c)) Attempt(c); // последний узел отказал: пробуем другой
        }
    }

    private string? PickKey()
    {
        var open = Enumerable.Range(0, _cfg.Keys).Select(i => "k" + i).Where(k => _opsPerKey.GetValueOrDefault(k) < _cfg.MaxOpsPerKey).ToList();
        return open.Count == 0 ? null : open[_work.Next(open.Count)];
    }

    private void Attempt(Client c)
    {
        c.LastAttempt = Now;
        var node = _nodes[c.Target];
        if (node is null)
        {
            c.Target = _work.Next(_cfg.Nodes);
            return;
        }
        var outbox = new List<Message>();
        var r = node.Propose(c.Cmd!, outbox);
        Dispatch(outbox);
        switch (r)
        {
            case Accepted a:
                _pending[(node.Id, a.Index, a.Term)] = c;
                break;
            case LocalAnswer la:
                Complete(c, c.Op!, la.Result);
                break;
            case NotLeader nl:
                c.Target = nl.Hint ?? _work.Next(_cfg.Nodes);
                break;
        }
        Drain(node); // после регистрации ожидания: в кластере из одного узла запись применяется сразу
    }

    public IEnumerable<RaftNode> LiveNodes => _nodes.Where(n => n is not null)!;
}
