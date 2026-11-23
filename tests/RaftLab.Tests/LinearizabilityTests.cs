using RaftLab.Core;
using RaftLab.Sim;

namespace RaftLab.Tests;

public class StateMachineTests
{
    private static Command C(CommandKind k, string key, string? v = null, string? exp = null, string client = "a", long seq = 1) => new(k, key, v, exp, client, seq);

    [Fact]
    public void Put_get_cas_semantics()
    {
        var m = new KvStateMachine();
        Assert.Equal(new CommandResult(false, null), m.Apply(C(CommandKind.Get, "k", seq: 1)));
        Assert.Equal(new CommandResult(true, null), m.Apply(C(CommandKind.Put, "k", "1", seq: 2)));
        Assert.Equal(new CommandResult(true, "1"), m.Apply(C(CommandKind.Get, "k", seq: 3)));
        Assert.Equal(new CommandResult(false, "1"), m.Apply(C(CommandKind.Cas, "k", "2", "x", seq: 4))); // ожидалось x
        Assert.Equal(new CommandResult(true, "1"), m.Apply(C(CommandKind.Cas, "k", "2", "1", seq: 5)));
        Assert.Equal("2", m.Data["k"]);
        Assert.Equal(new CommandResult(true, null), m.Apply(C(CommandKind.Cas, "new", "v", null, seq: 6))); // null: ключа ещё нет
    }

    [Fact]
    public void Repeated_command_returns_stored_result_and_is_not_executed_twice()
    {
        var m = new KvStateMachine();
        var cas = C(CommandKind.Cas, "k", "2", null, seq: 1);
        var first = m.Apply(cas);
        Assert.True(first.Ok);
        m.Apply(C(CommandKind.Put, "k", "other", client: "b", seq: 1));
        var again = m.Apply(cas); // повтор: тот же результат, значение не перезаписывается
        Assert.Equal(first, again);
        Assert.Equal("other", m.Data["k"]);
    }

    [Fact]
    public void Dedup_is_per_client_and_ignores_old_sequence_numbers()
    {
        var m = new KvStateMachine();
        m.Apply(C(CommandKind.Put, "k", "a", client: "x", seq: 5));
        Assert.False(m.Apply(C(CommandKind.Put, "k", "stale", client: "x", seq: 3)).Ok);
        Assert.Equal("a", m.Data["k"]);
        m.Apply(C(CommandKind.Put, "k", "b", client: "y", seq: 1)); // у другого клиента свои номера
        Assert.Equal("b", m.Data["k"]);
    }

    [Fact]
    public void Without_dedup_repeated_command_is_applied_again()
    {
        var m = new KvStateMachine(dedup: false);
        var cas = C(CommandKind.Cas, "k", "2", null, seq: 1);
        Assert.True(m.Apply(cas).Ok);
        Assert.False(m.Apply(cas).Ok); // второй раз «ключ уже есть»: клиент получил бы другой ответ, чем при первом применении
    }

    [Fact]
    public void Noop_changes_nothing()
    {
        var m = new KvStateMachine();
        Assert.True(m.Apply(Command.Noop).Ok);
        Assert.Empty(m.Data);
    }
}

public class LinearizabilityTests
{
    private static HistoryOp Op(int client, CommandKind kind, long invoke, long ret, string? value = null, string? expected = null, bool? ok = null, string? result = null) => new()
    {
        Client = client,
        Seq = invoke,
        Kind = kind,
        Key = "k",
        Value = value,
        Expected = expected,
        Invoke = invoke,
        Return = ret,
        Result = ret >= 0 ? new CommandResult(ok ?? true, result) : null,
    };

    private static HistoryOp Put(int c, long inv, long ret, string v) => Op(c, CommandKind.Put, inv, ret, v);
    private static HistoryOp Get(int c, long inv, long ret, string? seen) => Op(c, CommandKind.Get, inv, ret, ok: seen is not null, result: seen);

    private static bool Lin(params HistoryOp[] ops) => Linearizability.Check(ops).Ok;

    [Fact]
    public void Empty_and_sequential_histories_are_linearizable()
    {
        Assert.True(Lin());
        Assert.True(Lin(Put(0, 1, 2, "a"), Get(0, 3, 4, "a"), Put(0, 5, 6, "b"), Get(0, 7, 8, "b")));
        Assert.True(Lin(Get(0, 1, 2, null), Put(0, 3, 4, "a")));
    }

    [Fact]
    public void Stale_read_after_completed_write_is_not_linearizable()
    {
        Assert.False(Lin(Put(0, 1, 2, "a"), Put(0, 3, 4, "b"), Get(1, 5, 6, "a")));
        Assert.False(Lin(Put(0, 1, 2, "a"), Get(1, 3, 4, null)));
    }

    [Fact]
    public void Read_of_a_value_nobody_wrote_is_not_linearizable()
    {
        Assert.False(Lin(Put(0, 1, 2, "a"), Get(1, 3, 4, "zzz")));
    }

    [Fact]
    public void Concurrent_operations_may_be_ordered_either_way()
    {
        Assert.True(Lin(Put(0, 1, 10, "a"), Get(1, 2, 3, "a")));   // чтение внутри записи видит новое
        Assert.True(Lin(Put(0, 1, 10, "a"), Get(1, 2, 3, null)));  // или старое
        Assert.True(Lin(Put(0, 1, 10, "a"), Put(1, 2, 9, "b"), Get(2, 11, 12, "a")));
        Assert.True(Lin(Put(0, 1, 10, "a"), Put(1, 2, 9, "b"), Get(2, 11, 12, "b")));
        Assert.False(Lin(Put(0, 1, 10, "a"), Put(1, 2, 9, "b"), Get(2, 11, 12, "c")));
    }

    [Fact]
    public void Two_readers_must_agree_on_an_order()
    {
        // оба читателя видят обе записи, но в противоположном порядке: так не бывает
        Assert.False(Lin(Put(0, 1, 20, "a"), Put(1, 1, 20, "b"), Get(2, 2, 3, "a"), Get(2, 4, 5, "b"), Get(3, 2, 3, "b"), Get(3, 4, 5, "a")));
    }

    [Fact]
    public void Operation_without_response_may_or_may_not_have_happened()
    {
        Assert.True(Lin(Put(0, 1, -1, "a"), Get(1, 5, 6, "a")));   // запись без ответа всё же сработала
        Assert.True(Lin(Put(0, 1, -1, "a"), Get(1, 5, 6, null)));  // или нет
        Assert.True(Lin(Put(0, 1, -1, "a"), Get(1, 5, 6, null), Get(1, 7, 8, "a"))); // сработала между чтениями
        Assert.False(Lin(Put(0, 1, -1, "a"), Get(1, 5, 6, "a"), Get(1, 7, 8, null))); // но не «исчезла» после того, как её увидели
    }

    [Fact]
    public void Cas_semantics_are_checked()
    {
        Assert.True(Lin(Op(0, CommandKind.Cas, 1, 2, "a", null, true, null), Get(1, 3, 4, "a")));
        Assert.False(Lin(Op(0, CommandKind.Cas, 1, 2, "a", "zzz", true, null))); // обмен при несовпадении ожидаемого
        Assert.True(Lin(Op(0, CommandKind.Cas, 1, 2, "a", "zzz", false, null)));
        // два одновременных обмена «с пустого» не могут оба удасться
        Assert.False(Lin(Op(0, CommandKind.Cas, 1, 5, "a", null, true, null), Op(1, CommandKind.Cas, 1, 5, "b", null, true, null)));
        Assert.True(Lin(Op(0, CommandKind.Cas, 1, 5, "a", null, true, null), Op(1, CommandKind.Cas, 1, 5, "b", null, false, "a")));
    }

    [Fact]
    public void Keys_are_checked_independently()
    {
        var other = new HistoryOp { Client = 0, Seq = 1, Kind = CommandKind.Put, Key = "other", Value = "x", Invoke = 1, Return = 2, Result = new CommandResult(true, null) };
        var read = new HistoryOp { Client = 1, Seq = 1, Kind = CommandKind.Get, Key = "k", Invoke = 3, Return = 4, Result = new CommandResult(false, null) };
        Assert.True(Linearizability.Check(new[] { other, read }).Ok); // запись в другой ключ не мешает «пустому» чтению
    }

    [Fact]
    public void Explanation_names_the_key_and_lists_operations()
    {
        var r = Linearizability.Check(new[] { Put(0, 1, 2, "a"), Get(1, 3, 4, "zzz") });
        Assert.False(r.Ok);
        Assert.Equal("k", r.Key);
        Assert.Contains("Get k", r.Explanation);
        Assert.Contains("Put k=a", r.Explanation);
    }

    /// <summary>Строит историю из настоящего последовательного выполнения со случайными интервалами вокруг точек линеаризации.</summary>
    private static List<HistoryOp> RandomLinearizable(int seed, int ops, int clients = 3)
    {
        var rnd = new SimRandom((ulong)seed);
        string? state = null;
        var history = new List<HistoryOp>();
        var seq = new long[clients];
        for (var i = 0; i < ops; i++)
        {
            var point = i * 10 + 5; // момент «вступления в силу»
            var client = rnd.Next(clients);
            var inv = point - rnd.Next(1, 25);
            var ret = point + rnd.Next(1, 25);
            var kind = rnd.Next(3);
            HistoryOp op;
            if (kind == 0)
            {
                var v = "v" + i;
                state = v;
                op = new HistoryOp { Client = client, Seq = ++seq[client], Kind = CommandKind.Put, Key = "k", Value = v, Invoke = inv, Return = ret, Result = new CommandResult(true, null) };
            }
            else if (kind == 1) op = new HistoryOp { Client = client, Seq = ++seq[client], Kind = CommandKind.Get, Key = "k", Invoke = inv, Return = ret, Result = new CommandResult(state is not null, state) };
            else
            {
                var expected = rnd.Chance(0.5) ? state : "nope";
                var swap = expected == state;
                var v = "c" + i;
                op = new HistoryOp { Client = client, Seq = ++seq[client], Kind = CommandKind.Cas, Key = "k", Value = v, Expected = expected, Invoke = inv, Return = ret, Result = new CommandResult(swap, state) };
                if (swap) state = v;
            }
            history.Add(op);
        }
        return history;
    }

    [Fact]
    public void Histories_built_from_a_real_sequential_execution_are_always_accepted()
    {
        for (var seed = 1; seed <= 300; seed++)
            Assert.True(Linearizability.Check(RandomLinearizable(seed, 25)).Ok, $"seed {seed}");
    }

    [Fact]
    public void Reading_a_never_written_value_is_always_caught()
    {
        var caught = 0;
        for (var seed = 1; seed <= 300; seed++)
        {
            var h = RandomLinearizable(seed, 25);
            var reads = h.Where(o => o.Kind == CommandKind.Get && o.Completed).ToList();
            if (reads.Count == 0) continue;
            reads[reads.Count / 2].Result = new CommandResult(true, "never-written");
            Assert.False(Linearizability.Check(h).Ok, $"seed {seed}");
            caught++;
        }
        Assert.True(caught > 250);
    }

    [Fact]
    public void Swapping_results_of_ordered_reads_is_caught_most_of_the_time()
    {
        // чтение «из прошлого»: строго после записи, но видит значение до неё
        var caught = 0;
        var tried = 0;
        for (var seed = 1; seed <= 300; seed++)
        {
            var h = RandomLinearizable(seed, 30);
            var put = h.Where(o => o.Kind == CommandKind.Put && o.Completed).ToList();
            var late = h.Where(o => o.Kind == CommandKind.Get && o.Completed && put.Count(p => p.Return < o.Invoke) >= 2).ToList();
            if (late.Count == 0) continue;
            var read = late[0];
            var earlier = put.Where(p => p.Return < read.Invoke).OrderBy(p => p.Return).First();
            read.Result = new CommandResult(true, earlier.Value);
            tried++;
            if (!Linearizability.Check(h).Ok) caught++;
        }
        Assert.True(tried > 200);
        Assert.True(caught >= tried * 0.7, $"поймано {caught} из {tried}");
    }

    [Fact]
    public void First_violation_tick_is_minimal()
    {
        var h = new List<HistoryOp> { Put(0, 1, 2, "a"), Put(0, 3, 4, "b"), Get(1, 5, 6, "b"), Get(1, 7, 8, "a"), Put(0, 9, 10, "c") };
        Assert.False(Linearizability.Check(h).Ok);
        var (tick, res) = Linearizability.FirstViolation(h);
        Assert.Equal(8, tick); // читатель увидел «a» после «b»: нарушение появляется, когда это чтение вернулось
        Assert.False(res.Ok);
        Assert.True(Linearizability.Check(Linearizability.Truncate(h, tick - 1)).Ok);
        Assert.DoesNotContain("Put k=c", res.Explanation); // операции позже момента нарушения в объяснение не попадают
    }
}
