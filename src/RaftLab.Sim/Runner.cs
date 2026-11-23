using System.Text;
using RaftLab.Core;

namespace RaftLab.Sim;

public sealed record RunResult(
    long Seed,
    IReadOnlyList<Violation> Violations,
    int Operations,
    int Completed,
    int LeadersElected,
    long Committed,
    long Ticks,
    ulong Digest)
{
    public bool Ok => Violations.Count == 0;
    /// <summary>Нарушения безопасности и линеаризуемости (без живости: она зависит от параметров сети и сроков).</summary>
    public IReadOnlyList<Violation> Safety => Violations.Where(v => v.Name != "Liveness").ToList();
    public string Summary => Ok ? $"зерно {Seed}: ок ({Operations} операций, {Completed} завершено, лидеров {LeadersElected}, закоммичено {Committed})" : $"зерно {Seed}: " + string.Join("; ", Violations.Select(v => $"{v.Name} на тике {v.Tick}: {v.Message.Split('\n')[0]}"));
}

public static class Runner
{
    /// <summary>
    /// Полный прогон: период хаоса, починка, спокойный период. Проверяются безопасность Raft на каждом шаге,
    /// живость после починки (есть лидер, узлы сошлись, клиенты получили ответы) и линеаризуемость истории.
    /// </summary>
    public static RunResult Run(SimConfig cfg, TraceRecorder? trace = null, bool stopAtViolation = true)
    {
        var c = new Cluster(cfg) { Trace = trace };
        var total = cfg.ChaosTicks + cfg.SettleTicks;
        for (var t = 0; t < total; t++)
        {
            if (t < cfg.ChaosTicks) c.Chaos();
            else if (t == cfg.ChaosTicks) c.HealEverything();
            if (t == cfg.ChaosTicks + cfg.SettleTicks / 5) c.StopClients();
            c.Step();
            if (stopAtViolation && c.Checks.Failed) break;
        }
        // льготный период: выборы могут идти ровно в конце, дайте кластеру ещё немного времени, прежде чем считать это нарушением живости
        for (var g = 0; g < GraceTicks && !c.Checks.Failed && LivenessProblem(c) is not null; g++) c.Step();
        return Finish(c, cfg);
    }

    /// <summary>Сколько тиков сверх спокойного периода даётся на то, чтобы закончились выборы и узлы сошлись.</summary>
    public const int GraceTicks = 400;

    /// <summary>Что мешает считать кластер здоровым после починки (null: всё в порядке).</summary>
    private static string? LivenessProblem(Cluster c)
    {
        var live = c.LiveNodes.ToList();
        if (c.CurrentLeader() is null) return "после починки сети нет лидера";
        if (live.Select(n => n.CommitIndex).Distinct().Count() != 1 || live.Select(n => n.Machine.Snapshot()).Distinct().Count() != 1)
            return "после починки узлы не сошлись: " + string.Join(", ", live.Select(n => $"{n.Id}:commit={n.CommitIndex}"));
        if (c.PendingClientOps > 0) return $"после починки остались клиенты без ответа: {c.PendingClientOps} ({c.PendingDescription()})";
        return null;
    }

    private static RunResult Finish(Cluster c, SimConfig cfg)
    {
        var checks = c.Checks;
        var violations = new List<Violation>(checks.Violations);
        if (!checks.Failed)
        {
            checks.CheckLogMatching();
            checks.CheckCommittedPrefixes();
            violations = new List<Violation>(checks.Violations);
        }
        if (violations.Count == 0)
        {
            if (LivenessProblem(c) is { } problem) violations.Add(new Violation("Liveness", problem, c.Now));
        }
        if (violations.Count == 0 || violations.All(v => v.Name is "Liveness"))
        {
            if (!Linearizability.Check(c.History).Ok)
            {
                var (tick, detail) = Linearizability.FirstViolation(c.History);
                violations.Add(new Violation("Linearizability", detail.Explanation, tick));
            }
        }
        return new RunResult(cfg.Seed, violations, c.History.Count, c.History.Count(h => h.Completed), checks.LeadersElected, checks.CommittedCount, c.Now, Digest(c));
    }

    private static ulong Digest(Cluster c)
    {
        var h = 14695981039346656037UL;
        void Mix(string s)
        {
            foreach (var b in Encoding.UTF8.GetBytes(s))
            {
                h ^= b;
                h *= 1099511628211UL;
            }
        }
        foreach (var op in c.History) Mix(op.ToString());
        foreach (var n in c.LiveNodes) Mix($"{n.Id}:{n.Term}:{n.CommitIndex}:{n.Machine.Snapshot()}");
        Mix(c.Now.ToString());
        return h;
    }

    /// <summary>Перебор зёрен: возвращает все результаты (для статистики) в порядке зёрен.</summary>
    public static IEnumerable<RunResult> Fuzz(SimConfig baseCfg, long fromSeed, long toSeed)
    {
        for (var s = fromSeed; s <= toSeed; s++) yield return Run(baseCfg with { Seed = s });
    }
}
