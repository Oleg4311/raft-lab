using RaftLab.Core;

namespace RaftLab.Sim;

/// <summary>
/// Встроенные ошибки и зёрна, на которых проверки их ловят. Зёрна найдены командой «fuzz --bug ... --stop-first»
/// и закреплены тестами: если реализация или проверки изменятся, тест покажет, что ошибка перестала ловиться.
/// </summary>
public static class KnownBugs
{
    /// <summary>Ошибка, параметры прогона и вид нарушения, которое должно найтись.</summary>
    public static IEnumerable<(RaftBug Bug, SimConfig Config, string Expected)> Runs()
    {
        var shortRun = new SimConfig { ChaosTicks = 400, SettleTicks = 700 };
        var harsh = shortRun with { FaultProb = 0.08, DropProb = 0.05, Raft = new RaftOptions { MaxBatch = 1, ElectionTimeoutMin = 8, ElectionTimeoutMax = 12 } };
        var voteCfg = shortRun with { FaultProb = 0.1, DropProb = 0.1, Raft = new RaftOptions { ElectionTimeoutMin = 6, ElectionTimeoutMax = 9 } };
        yield return (RaftBug.NoUpToDateCheck, With(shortRun, RaftBug.NoUpToDateCheck, 11), "LeaderCompleteness");
        yield return (RaftBug.TruncateAlways, With(shortRun, RaftBug.TruncateAlways, 1), "CommittedPrefix");
        yield return (RaftBug.LocalReads, With(shortRun, RaftBug.LocalReads, 455), "Linearizability");
        yield return (RaftBug.NoDedup, With(shortRun, RaftBug.NoDedup, 104), "Linearizability");
        yield return (RaftBug.CommitPreviousTermByCount, With(harsh, RaftBug.CommitPreviousTermByCount, 240), "LeaderCompleteness");
        yield return (RaftBug.VoteNotPersisted, With(voteCfg, RaftBug.VoteNotPersisted, 770), "ElectionSafety");
    }

    private static SimConfig With(SimConfig cfg, RaftBug bug, long seed) => cfg with { Seed = seed, Raft = cfg.Raft with { Bugs = bug } };
}
