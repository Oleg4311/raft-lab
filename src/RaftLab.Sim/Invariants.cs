using RaftLab.Core;

namespace RaftLab.Sim;

public sealed record Violation(string Name, string Message, long Tick);

/// <summary>
/// Свойства безопасности Raft (рис. 3 статьи), проверяемые на каждом шаге симуляции:
/// Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety.
/// </summary>
public sealed class Invariants
{
    private readonly Cluster _c;
    private readonly Dictionary<long, int> _leaderOfTerm = new();
    private readonly Dictionary<long, LogEntry> _committed = new();
    private readonly Dictionary<(int Node, long Term), List<LogEntry>> _leaderLogs = new();

    public Invariants(Cluster c) => _c = c;

    public List<Violation> Violations { get; } = new();
    public bool Failed => Violations.Count > 0;
    public int LeadersElected { get; private set; }
    public long CommittedCount => _committed.Count;

    private void Fail(string name, string message)
    {
        if (Violations.Any(v => v.Name == name)) return; // одно сообщение на вид нарушения
        Violations.Add(new Violation(name, message, _c.Now));
        _c.Trace?.Event(_c.Now, $"НАРУШЕНИЕ {name}: {message}");
    }

    public void OnLeader(RaftNode n)
    {
        LeadersElected++;
        if (_leaderOfTerm.TryGetValue(n.Term, out var other) && other != n.Id)
            Fail("ElectionSafety", $"в терме {n.Term} два лидера: узлы {other} и {n.Id}");
        else _leaderOfTerm[n.Term] = n.Id;

        // Leader Completeness: у нового лидера есть все записи, закоммиченные раньше
        foreach (var (idx, e) in _committed)
        {
            if (n.Log.Count < idx || n.Log[(int)idx - 1] != e)
            {
                Fail("LeaderCompleteness", $"лидер {n.Id} терма {n.Term} не содержит закоммиченную запись {idx} ({e.Command}, терм {e.Term})");
                break;
            }
        }
    }

    public void OnApplied(RaftNode node, AppliedEntry a)
    {
        if (_committed.TryGetValue(a.Index, out var known))
        {
            if (known != a.Entry)
                Fail("StateMachineSafety", $"на индексе {a.Index} узел {node.Id} применил «{a.Entry.Command}» (терм {a.Entry.Term}), а раньше был применён «{known.Command}» (терм {known.Term})");
        }
        else _committed[a.Index] = a.Entry;
    }

    public void EndOfTick()
    {
        if (_c.Now % 5 != 0) return;
        CheckLogMatching();
        CheckLeaderAppendOnly();
        CheckCommittedPrefixes();
    }

    public void CheckLogMatching()
    {
        var logs = _c.Persistent;
        for (var a = 0; a < logs.Count; a++)
            for (var b = a + 1; b < logs.Count; b++)
            {
                var la = logs[a].Log;
                var lb = logs[b].Log;
                var top = Math.Min(la.Count, lb.Count);
                var anchor = -1;
                for (var i = top - 1; i >= 0; i--)
                    if (la[i].Term == lb[i].Term)
                    {
                        anchor = i;
                        break;
                    }
                for (var i = 0; i <= anchor; i++)
                    if (la[i] != lb[i])
                    {
                        Fail("LogMatching", $"журналы узлов {a} и {b} совпадают по терму на индексе {anchor + 1}, но различаются на индексе {i + 1}");
                        return;
                    }
            }
    }

    private void CheckLeaderAppendOnly()
    {
        foreach (var node in _c.LiveNodes)
        {
            if (node.Role != Role.Leader) continue;
            var key = (node.Id, node.Term);
            if (_leaderLogs.TryGetValue(key, out var before))
            {
                var cur = node.Log;
                var ok = cur.Count >= before.Count;
                for (var i = 0; ok && i < before.Count; i++) ok = cur[i] == before[i];
                if (!ok) Fail("LeaderAppendOnly", $"лидер {node.Id} терма {node.Term} изменил или потерял записи своего журнала");
            }
            _leaderLogs[key] = node.Log.ToList();
        }
    }

    /// <summary>Конец прогона: закоммиченное на одном узле не должно отличаться от закоммиченного на другом.</summary>
    public void CheckCommittedPrefixes()
    {
        for (var i = 0; i < _c.Persistent.Count; i++)
        {
            var alive = _c.Nodes[i];
            if (alive is null) continue;
            var log = _c.Persistent[i].Log;
            foreach (var (idx, e) in _committed)
            {
                if (alive.CommitIndex < idx) continue;
                if (log.Count < idx || log[(int)idx - 1] != e)
                {
                    Fail("CommittedPrefix", $"узел {i} считает запись {idx} закоммиченной, но она отличается от общей ({e.Command})");
                    return;
                }
            }
        }
    }
}
