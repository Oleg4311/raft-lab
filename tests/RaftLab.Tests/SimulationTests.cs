using System.Text.Json;
using RaftLab.Core;
using RaftLab.Sim;

namespace RaftLab.Tests;

public class SimRandomTests
{
    [Fact]
    public void Matches_reference_splitmix64_vectors()
    {
        // опубликованные значения SplitMix64 для зерна 1234567
        var r = new SimRandom(1234567);
        Assert.Equal(new ulong[] { 6457827717110365317UL, 3203168211198807973UL, 9817491932198370423UL, 4593380528125082431UL, 16408922859458223821UL },
            Enumerable.Range(0, 5).Select(_ => r.NextU64()).ToArray());
    }

    [Fact]
    public void Ranges_and_chances_behave()
    {
        var r = new SimRandom(7);
        for (var i = 0; i < 10000; i++)
        {
            var v = r.Next(3, 9);
            Assert.InRange(v, 3, 8);
            Assert.InRange(r.NextDouble(), 0.0, 0.9999999999999999);
        }
        Assert.Equal(5, r.Next(5, 5));
        Assert.False(new SimRandom(1).Chance(0));
        var hits = Enumerable.Range(0, 20000).Count(_ => r.Chance(0.25));
        Assert.InRange(hits, 4600, 5400);
    }

    [Fact]
    public void Derived_streams_differ()
    {
        Assert.NotEqual(SimRandom.Derive(1, 1).NextU64(), SimRandom.Derive(1, 2).NextU64());
        Assert.NotEqual(SimRandom.Derive(1, 1).NextU64(), SimRandom.Derive(2, 1).NextU64());
        Assert.Equal(SimRandom.Derive(9, 3).NextU64(), SimRandom.Derive(9, 3).NextU64());
    }
}

public class SimulationTests
{
    [Fact]
    public void Same_seed_gives_same_run_and_the_digest_is_stable_across_machines()
    {
        // эти отпечатки получены на Windows и обязаны совпасть в Linux-CI: симуляция не зависит от ОС и версии .NET
        Assert.Equal(0xb37bde7a9a60b2a0UL, Runner.Run(new SimConfig { Seed = 1 }).Digest);
        Assert.Equal(0x6ea0d32b0fa8d67fUL, Runner.Run(new SimConfig { Seed = 2 }).Digest);
        Assert.Equal(0xd1e5f70f3bba0596UL, Runner.Run(new SimConfig { Seed = 5, Nodes = 3 }).Digest);
        var a = Runner.Run(new SimConfig { Seed = 77 });
        var b = Runner.Run(new SimConfig { Seed = 77 });
        Assert.Equal(a.Digest, b.Digest);
        Assert.NotEqual(a.Digest, Runner.Run(new SimConfig { Seed = 78 }).Digest);
    }

    [Fact]
    public void Correct_implementation_passes_300_random_runs()
    {
        var bad = Runner.Fuzz(new SimConfig(), 1, 300).Where(r => !r.Ok).Select(r => r.Summary).ToList();
        Assert.Empty(bad);
    }

    [Fact]
    public void Correct_implementation_passes_under_harsh_network_and_tiny_batches()
    {
        var cfg = new SimConfig { FaultProb = 0.08, DropProb = 0.05, SettleTicks = 2500, Raft = new RaftOptions { MaxBatch = 1, ElectionTimeoutMin = 8, ElectionTimeoutMax = 12 } };
        var bad = Runner.Fuzz(cfg, 1, 60).Where(r => !r.Ok).Select(r => r.Summary).ToList();
        Assert.Empty(bad);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(7)]
    public void Cluster_sizes_one_three_and_seven_pass(int nodes)
    {
        var bad = Runner.Fuzz(new SimConfig { Nodes = nodes }, 1, 60).Where(r => !r.Ok).Select(r => r.Summary).ToList();
        Assert.Empty(bad);
    }

    [Fact]
    public void Runs_actually_exercise_faults_and_elections()
    {
        var r = Runner.Run(new SimConfig { Seed = 3 });
        Assert.True(r.LeadersElected >= 3, "без смены лидеров прогон ничего не проверяет");
        Assert.True(r.Operations >= 100);
        Assert.Equal(r.Operations, r.Completed);
        Assert.True(r.Committed > r.Operations);
    }

    [Fact]
    public void Each_known_bug_is_caught_at_its_pinned_seed_with_the_expected_violation()
    {
        foreach (var (bug, cfg, expected) in KnownBugs.Runs())
        {
            var r = Runner.Run(cfg);
            Assert.Contains(r.Safety, v => v.Name == expected);
            // та же симуляция без ошибки проходит: нарушение вызвано именно ею
            var clean = Runner.Run(cfg with { Raft = cfg.Raft with { Bugs = RaftBug.None } });
            Assert.True(clean.Safety.Count == 0, $"{bug}: без ошибки на том же зерне нарушение «{string.Join(",", clean.Safety.Select(v => v.Name))}»");
        }
    }

    [Fact]
    public void Every_bug_flag_has_a_known_catching_run()
    {
        var covered = KnownBugs.Runs().Select(x => x.Bug).ToHashSet();
        foreach (var b in Enum.GetValues<RaftBug>().Where(b => b != RaftBug.None)) Assert.Contains(b, covered);
    }

    [Fact]
    public void Minority_leader_cannot_commit_and_majority_elects_a_new_one()
    {
        var c = new Cluster(new SimConfig { Seed = 9, DropProb = 0, DuplicateProb = 0, Clients = 0 });
        for (var i = 0; i < 100; i++) c.Step();
        var old = c.CurrentLeader()!.Value;
        var friend = (old + 1) % 5;
        var rest = Enumerable.Range(0, 5).Where(i => i != old && i != friend).ToArray();
        c.Partition(new[] { old, friend }, rest);

        var outbox = new List<Message>();
        var proposal = c.Nodes[old]!.Propose(new Command(CommandKind.Put, "k", "lost", null, "x", 1), outbox);
        var accepted = Assert.IsType<Accepted>(proposal);
        for (var i = 0; i < 200; i++) c.Step();

        Assert.True(c.Nodes[old]!.CommitIndex < accepted.Index, "меньшинство не должно коммитить");
        var fresh = rest.Select(i => c.Nodes[i]!).FirstOrDefault(n => n.Role == Role.Leader);
        Assert.NotNull(fresh);
        Assert.True(fresh!.Term > c.Nodes[old]!.Term - 1 || fresh.Term >= 2);

        c.Heal();
        for (var i = 0; i < 200; i++) c.Step();
        var leader = c.Nodes[c.CurrentLeader()!.Value]!;
        Assert.All(c.LiveNodes, n => Assert.Equal(leader.CommitIndex, n.CommitIndex));
        Assert.DoesNotContain(c.Nodes[old]!.Log, e => e.Command.Value == "lost"); // непринятая запись старого лидера затёрта
        Assert.False(c.Checks.Failed);
    }

    [Fact]
    public void Crashed_node_keeps_log_and_term_and_catches_up_after_restart()
    {
        var c = new Cluster(new SimConfig { Seed = 12, DropProb = 0, DuplicateProb = 0, Clients = 2, OpProb = 0.2 });
        for (var i = 0; i < 120; i++) c.Step();
        var victim = (c.CurrentLeader()!.Value + 1) % 5;
        var logBefore = c.Persistent[victim].Log.Count;
        var termBefore = c.Persistent[victim].CurrentTerm;
        c.Crash(victim);
        for (var i = 0; i < 100; i++) c.Step();
        Assert.False(c.IsAlive(victim));
        Assert.Equal(logBefore, c.Persistent[victim].Log.Count);
        c.Restart(victim);
        Assert.Equal(termBefore, c.Nodes[victim]!.Term);
        c.StopClients();
        for (var i = 0; i < 300; i++) c.Step();
        Assert.Equal(c.Nodes[c.CurrentLeader()!.Value]!.CommitIndex, c.Nodes[victim]!.CommitIndex);
        Assert.False(c.Checks.Failed);
    }

    [Fact]
    public void Partitioned_messages_are_dropped_and_traced()
    {
        var trace = new TraceRecorder();
        var c = new Cluster(new SimConfig { Seed = 4, DropProb = 0, DuplicateProb = 0, Clients = 0 }) { Trace = trace };
        c.Partition(new[] { 0 }, new[] { 1, 2, 3, 4 });
        for (var i = 0; i < 80; i++) c.Step();
        var json = JsonDocument.Parse(trace.ToJson("t", "t", "", 5, null));
        var cut = json.RootElement.GetProperty("messages").EnumerateArray().Count(m => m.GetProperty("fate").GetString() == "cut");
        Assert.True(cut > 0);
        Assert.Equal(Role.Candidate, c.Nodes[0]!.Role); // изолированный узел безуспешно проводит выборы
        Assert.True(c.Nodes[0]!.Term > c.Nodes[1]!.Term || c.Nodes[0]!.Term >= 2);
    }
}

public class InvariantCheckerTests
{
    private static Cluster Fresh() => new(new SimConfig { Seed = 1, Clients = 0 });

    [Fact]
    public void Two_leaders_in_one_term_are_reported()
    {
        var c = Fresh();
        var a = new RaftNode(0, new[] { 0, 1, 2 }, new PersistentState { CurrentTerm = 5 }, new RaftOptions(), new SimRandom(1), 0);
        var b = new RaftNode(1, new[] { 0, 1, 2 }, new PersistentState { CurrentTerm = 5 }, new RaftOptions(), new SimRandom(2), 0);
        c.Checks.OnLeader(a);
        Assert.False(c.Checks.Failed);
        c.Checks.OnLeader(b);
        Assert.Contains(c.Checks.Violations, v => v.Name == "ElectionSafety");
    }

    [Fact]
    public void Same_leader_in_a_term_twice_is_not_a_violation()
    {
        var c = Fresh();
        var a = new RaftNode(0, new[] { 0, 1, 2 }, new PersistentState { CurrentTerm = 5 }, new RaftOptions(), new SimRandom(1), 0);
        c.Checks.OnLeader(a);
        c.Checks.OnLeader(a);
        Assert.False(c.Checks.Failed);
    }

    [Fact]
    public void Different_entry_at_the_same_applied_index_is_reported()
    {
        var c = Fresh();
        var node = c.Nodes[0]!;
        var e1 = new LogEntry(1, new Command(CommandKind.Put, "k", "a", null, "c", 1));
        var e2 = new LogEntry(1, new Command(CommandKind.Put, "k", "b", null, "c", 1));
        c.Checks.OnApplied(node, new AppliedEntry(1, e1, new CommandResult(true, null)));
        c.Checks.OnApplied(node, new AppliedEntry(1, e1, new CommandResult(true, null))); // то же самое повторно: можно
        Assert.False(c.Checks.Failed);
        c.Checks.OnApplied(c.Nodes[1]!, new AppliedEntry(1, e2, new CommandResult(true, null)));
        Assert.Contains(c.Checks.Violations, v => v.Name == "StateMachineSafety");
    }

    [Fact]
    public void New_leader_without_a_committed_entry_is_reported()
    {
        var c = Fresh();
        var committed = new LogEntry(2, new Command(CommandKind.Put, "k", "a", null, "c", 1));
        c.Checks.OnApplied(c.Nodes[0]!, new AppliedEntry(1, committed, new CommandResult(true, null)));
        var p = new PersistentState { CurrentTerm = 3 };
        p.Log.Add(new LogEntry(1, Command.Noop)); // на индексе 1 другая запись
        var leader = new RaftNode(1, new[] { 0, 1, 2 }, p, new RaftOptions(), new SimRandom(1), 0);
        c.Checks.OnLeader(leader);
        Assert.Contains(c.Checks.Violations, v => v.Name == "LeaderCompleteness");
    }

    [Fact]
    public void Logs_that_agree_on_term_but_differ_in_content_are_reported()
    {
        var c = Fresh();
        c.Persistent[0].Log.Add(new LogEntry(1, new Command(CommandKind.Put, "k", "a", null, "c", 1)));
        c.Persistent[1].Log.Add(new LogEntry(1, new Command(CommandKind.Put, "k", "b", null, "c", 1)));
        c.Checks.CheckLogMatching();
        Assert.Contains(c.Checks.Violations, v => v.Name == "LogMatching");
    }

    [Fact]
    public void Logs_that_differ_only_in_uncommitted_tails_are_fine()
    {
        var c = Fresh();
        var common = new LogEntry(1, Command.Noop);
        c.Persistent[0].Log.Add(common);
        c.Persistent[1].Log.Add(common);
        c.Persistent[0].Log.Add(new LogEntry(2, new Command(CommandKind.Put, "k", "a", null, "c", 1)));
        c.Persistent[1].Log.Add(new LogEntry(3, new Command(CommandKind.Put, "k", "b", null, "c", 1))); // разные термы: допустимо
        c.Checks.CheckLogMatching();
        Assert.False(c.Checks.Failed);
    }
}

public class TraceTests
{
    [Fact]
    public void Trace_json_is_deterministic_and_well_formed()
    {
        string One()
        {
            var t = new TraceRecorder();
            var r = Runner.Run(new SimConfig { Seed = 3, ChaosTicks = 120, SettleTicks = 60 }, t);
            return t.ToJson("n", "title", "d", 5, null);
        }
        var a = One();
        Assert.Equal(a, One());
        var doc = JsonDocument.Parse(a).RootElement;
        Assert.Equal(5, doc.GetProperty("nodes").GetInt32());
        Assert.Equal(doc.GetProperty("frames").GetArrayLength(), doc.GetProperty("ticks").GetInt32());
        Assert.All(doc.GetProperty("frames").EnumerateArray(), f => Assert.Equal(5, f.GetProperty("nodes").GetArrayLength()));
        Assert.All(doc.GetProperty("messages").EnumerateArray(), m => Assert.Contains(m.GetProperty("fate").GetString(), new[] { "delivered", "dropped", "cut", "dead", "inflight" }));
    }

    [Fact]
    public void Windowed_trace_starts_with_complete_logs()
    {
        var t = new TraceRecorder();
        var cfg = new SimConfig { Seed = 3, ChaosTicks = 200, SettleTicks = 50 };
        Runner.Run(cfg, t);
        var doc = JsonDocument.Parse(t.ToJson("n", "t", "d", 5, null, 120, 180)).RootElement;
        Assert.Equal(120, doc.GetProperty("start").GetInt64());
        var first = doc.GetProperty("frames")[0];
        Assert.All(first.GetProperty("nodes").EnumerateArray(), n => Assert.True(n.TryGetProperty("log", out _), "в первом кадре окна должны быть журналы всех узлов"));
        Assert.Equal(180, doc.GetProperty("frames").EnumerateArray().Last().GetProperty("t").GetInt64());
    }
}
