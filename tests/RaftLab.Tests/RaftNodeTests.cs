using RaftLab.Core;
using RaftLab.Sim;

namespace RaftLab.Tests;

public class RaftNodeTests
{
    private static readonly int[] Ids = { 0, 1, 2, 3, 4 };

    private static RaftNode Node(int id = 0, PersistentState? p = null, RaftBug bugs = RaftBug.None, int[]? ids = null, long now = 0) =>
        new(id, ids ?? Ids, p ?? new PersistentState(), new RaftOptions { Bugs = bugs }, new SimRandom(42), now);

    private static Command Put(string k, string v, string client = "c", long seq = 1) => new(CommandKind.Put, k, v, null, client, seq);

    private static LogEntry E(long term, string v = "x") => new(term, Put("k", v));

    private static PersistentState Log(long term, params long[] terms)
    {
        var p = new PersistentState { CurrentTerm = term };
        foreach (var t in terms) p.Log.Add(E(t, "v" + p.Log.Count));
        return p;
    }

    /// <summary>Прокручивает часы, пока узел не станет кандидатом; возвращает отправленные им сообщения.</summary>
    private static List<Message> Elect(RaftNode n, ref long now)
    {
        var outbox = new List<Message>();
        while (n.Role == Role.Follower && now < 1000) n.Tick(++now, outbox);
        return outbox;
    }

    private static RaftNode Leader(PersistentState? p = null, int id = 0, RaftBug bugs = RaftBug.None)
    {
        var n = Node(id, p, bugs);
        long now = 0;
        var outbox = Elect(n, ref now);
        var term = n.Term;
        foreach (var peer in Ids.Where(x => x != id).Take(2)) n.Receive(new RequestVoteReply(peer, id, term, true), now, outbox);
        Assert.Equal(Role.Leader, n.Role);
        return n;
    }

    [Fact]
    public void Follower_times_out_and_becomes_candidate()
    {
        var n = Node();
        long now = 0;
        var outbox = Elect(n, ref now);
        Assert.Equal(Role.Candidate, n.Role);
        Assert.Equal(1, n.Term);
        Assert.Equal(0, n.VotedFor);
        Assert.Equal(4, outbox.OfType<RequestVote>().Count());
        Assert.All(outbox.OfType<RequestVote>(), rv => Assert.Equal((1L, 0L, 0L), (rv.Term, rv.LastLogIndex, rv.LastLogTerm)));
        Assert.InRange(now, 15, 30);
    }

    [Fact]
    public void Election_timeouts_are_randomized_across_nodes()
    {
        var times = new HashSet<long>();
        for (var seed = 1; seed <= 20; seed++)
        {
            var n = new RaftNode(0, Ids, new PersistentState(), new RaftOptions(), new SimRandom((ulong)seed), 0);
            long now = 0;
            Elect(n, ref now);
            times.Add(now);
        }
        Assert.True(times.Count > 5);
    }

    [Fact]
    public void Candidate_becomes_leader_with_majority_and_appends_noop()
    {
        var n = Node();
        long now = 0;
        var outbox = Elect(n, ref now);
        outbox.Clear();
        n.Receive(new RequestVoteReply(1, 0, 1, true), now, outbox);
        Assert.Equal(Role.Candidate, n.Role); // 2 голоса из 5: мало
        n.Receive(new RequestVoteReply(2, 0, 1, true), now, outbox);
        Assert.Equal(Role.Leader, n.Role);
        Assert.Equal(1, n.LastIndex);
        Assert.Equal(CommandKind.Noop, n.Log[0].Command.Kind);
        Assert.Equal(1, n.Log[0].Term);
        Assert.Equal(4, outbox.OfType<AppendEntries>().Count());
    }

    [Fact]
    public void Refused_votes_and_votes_from_old_terms_do_not_count()
    {
        var n = Node();
        long now = 0;
        var outbox = Elect(n, ref now);
        n.Receive(new RequestVoteReply(1, 0, 1, false), now, outbox);
        n.Receive(new RequestVoteReply(2, 0, 1, false), now, outbox);
        n.Receive(new RequestVoteReply(3, 0, 1, false), now, outbox);
        Assert.Equal(Role.Candidate, n.Role);
        n.Receive(new RequestVoteReply(1, 0, 0, true), now, outbox); // ответ из старого терма
        n.Receive(new RequestVoteReply(1, 0, 1, true), now, outbox);
        n.Receive(new RequestVoteReply(1, 0, 1, true), now, outbox); // повтор того же голоса не считается дважды
        Assert.Equal(Role.Candidate, n.Role);
    }

    [Fact]
    public void Single_node_cluster_elects_itself_and_commits_immediately()
    {
        var n = Node(0, null, RaftBug.None, new[] { 0 });
        long now = 0;
        var outbox = new List<Message>();
        while (n.Role != Role.Leader && now < 100) n.Tick(++now, outbox);
        Assert.Equal(Role.Leader, n.Role);
        var r = Assert.IsType<Accepted>(n.Propose(Put("a", "1"), outbox));
        Assert.Equal(2, r.Index);
        Assert.Equal(2, n.CommitIndex);
        Assert.Equal("1", n.Machine.Data["a"]);
    }

    [Theory]
    [InlineData(0, 5, 5, 0, true)]   // равный журнал: голос отдаётся
    [InlineData(0, 6, 5, 0, true)]   // длиннее при том же терме
    [InlineData(0, 4, 5, 0, false)]  // короче при том же терме
    [InlineData(0, 1, 6, 0, true)]   // новее по терму последней записи, пусть и короче
    [InlineData(0, 9, 4, 0, false)]  // длиннее, но терм последней записи старее
    [InlineData(1, 5, 5, 0, false)]  // уже голосовал за другого в этом терме
    [InlineData(1, 5, 5, 1, true)]   // повторный запрос от того же кандидата
    public void Vote_rules(int votedFor, long lastIndex, long lastTerm, int candidate, bool expected)
    {
        var p = Log(1, 1, 2, 3, 4, 5); // пять записей, последняя терма 5
        p.VotedFor = votedFor == 0 ? null : votedFor;
        var n = Node(2, p);
        var outbox = new List<Message>();
        n.Receive(new RequestVote(candidate == 0 ? 3 : candidate, 2, 1, lastIndex, lastTerm), 1, outbox);
        var reply = Assert.IsType<RequestVoteReply>(Assert.Single(outbox));
        Assert.Equal(expected, reply.Granted);
        if (expected) Assert.Equal(candidate == 0 ? 3 : candidate, n.VotedFor);
    }

    [Fact]
    public void Old_term_request_is_refused_with_current_term()
    {
        var n = Node(2, new PersistentState { CurrentTerm = 5 });
        var outbox = new List<Message>();
        n.Receive(new RequestVote(3, 2, 4, 0, 0), 1, outbox);
        var reply = Assert.IsType<RequestVoteReply>(Assert.Single(outbox));
        Assert.False(reply.Granted);
        Assert.Equal(5, reply.Term);
    }

    [Fact]
    public void Higher_term_makes_leader_step_down_and_forget_vote()
    {
        var n = Leader();
        var outbox = new List<Message>();
        n.Receive(new RequestVote(3, 0, n.Term + 2, 99, 99), 50, outbox);
        Assert.Equal(Role.Follower, n.Role);
        Assert.Equal(3, n.VotedFor); // и тут же проголосовал в новом терме
        Assert.Equal(3, n.Term);
    }

    [Fact]
    public void Granting_vote_resets_election_timer()
    {
        // контрольный узел с тем же зерном показывает, когда сработал бы исходный тайм-аут
        var control = Node(2);
        long t = 0;
        Elect(control, ref t);
        var n = Node(2);
        var outbox = new List<Message>();
        n.Receive(new RequestVote(3, 2, 1, 0, 0), t - 1, outbox); // за такт до тайм-аута
        outbox.Clear();
        n.Tick(t, outbox);
        Assert.Equal(Role.Follower, n.Role); // таймер сдвинулся вперёд
        n.Tick(t + 30, outbox);
        Assert.Equal(Role.Candidate, n.Role); // но не навсегда
    }

    // ---------- AppendEntries ----------

    private static AppendEntries AE(long term, long prevIdx, long prevTerm, long commit, params LogEntry[] entries) => new(1, 0, term, prevIdx, prevTerm, entries, commit);

    [Fact]
    public void Append_rejects_old_term()
    {
        var n = Node(0, new PersistentState { CurrentTerm = 3 });
        var outbox = new List<Message>();
        n.Receive(AE(2, 0, 0, 0, E(2)), 1, outbox);
        var r = Assert.IsType<AppendEntriesReply>(Assert.Single(outbox));
        Assert.False(r.Success);
        Assert.Equal(3, r.Term);
        Assert.Empty(n.Log);
    }

    [Fact]
    public void Append_with_missing_prefix_reports_where_log_ends()
    {
        var n = Node(0, Log(1, 1, 1));
        var outbox = new List<Message>();
        n.Receive(AE(1, 5, 1, 0, E(1)), 1, outbox);
        var r = Assert.IsType<AppendEntriesReply>(Assert.Single(outbox));
        Assert.False(r.Success);
        Assert.Equal((0L, 3L), (r.ConflictTerm, r.ConflictIndex));
    }

    [Fact]
    public void Append_with_term_mismatch_reports_first_index_of_conflicting_term()
    {
        var n = Node(0, Log(3, 1, 2, 2, 2));
        var outbox = new List<Message>();
        n.Receive(AE(3, 4, 3, 0, E(3)), 1, outbox);
        var r = Assert.IsType<AppendEntriesReply>(Assert.Single(outbox));
        Assert.False(r.Success);
        Assert.Equal((2L, 2L), (r.ConflictTerm, r.ConflictIndex));
    }

    [Fact]
    public void Conflicting_suffix_is_replaced_and_matching_prefix_kept()
    {
        var n = Node(0, Log(3, 1, 2, 2));
        var outbox = new List<Message>();
        n.Receive(AE(3, 1, 1, 0, E(3, "a"), E(3, "b")), 1, outbox);
        Assert.Equal(new long[] { 1, 3, 3 }, n.Log.Select(e => e.Term));
        var r = Assert.IsType<AppendEntriesReply>(Assert.Single(outbox));
        Assert.True(r.Success);
        Assert.Equal(3, r.MatchIndex);
    }

    [Fact]
    public void Stale_shorter_append_does_not_truncate_matching_tail()
    {
        // запоздавший и дублированный AppendEntries с меньшим числом записей не должен укорачивать журнал
        var p = Log(2, 1, 1, 2, 2);
        var n = Node(0, p);
        var before = p.Log.ToList();
        var outbox = new List<Message>();
        n.Receive(AE(2, 1, 1, 0, p.Log[1]), 1, outbox);
        Assert.Equal(before, n.Log.ToList());
        Assert.True(Assert.IsType<AppendEntriesReply>(Assert.Single(outbox)).Success);
    }

    [Fact]
    public void TruncateAlways_bug_loses_matching_tail_exactly_in_that_case()
    {
        var p = Log(2, 1, 1, 2, 2);
        var n = Node(0, p, RaftBug.TruncateAlways);
        n.Receive(AE(2, 1, 1, 0, p.Log[1]), 1, new List<Message>());
        Assert.Equal(2, n.Log.Count);
    }

    [Fact]
    public void Commit_index_follows_leader_but_not_beyond_what_was_sent()
    {
        var n = Node(0);
        var outbox = new List<Message>();
        n.Receive(AE(1, 0, 0, 10, E(1), E(1)), 1, outbox); // лидер закоммитил 10, но прислал только 2 записи
        Assert.Equal(2, n.CommitIndex);
        Assert.Equal(2, n.LastApplied);
        n.Receive(AE(1, 2, 1, 1, E(1)), 2, outbox); // индекс коммита не откатывается назад
        Assert.Equal(2, n.CommitIndex);
    }

    [Fact]
    public void Candidate_steps_down_for_leader_of_same_term()
    {
        var n = Node(0);
        long now = 0;
        Elect(n, ref now);
        Assert.Equal(Role.Candidate, n.Role);
        n.Receive(AE(n.Term, 0, 0, 0), now, new List<Message>());
        Assert.Equal(Role.Follower, n.Role);
        Assert.Equal(1, n.LeaderHint);
    }

    // ---------- лидер ----------

    [Fact]
    public void Leader_commits_entry_only_with_majority_acknowledgement()
    {
        var n = Leader();
        var outbox = new List<Message>();
        var a = Assert.IsType<Accepted>(n.Propose(Put("k", "v"), outbox));
        Assert.Equal(2, a.Index); // индекс 1 занят пустой записью
        Assert.Equal(0, n.CommitIndex);
        n.Receive(new AppendEntriesReply(1, 0, n.Term, true, 2, 0, 0), 60, outbox);
        Assert.Equal(0, n.CommitIndex); // с самим лидером это 2 из 5
        n.Receive(new AppendEntriesReply(2, 0, n.Term, true, 2, 0, 0), 60, outbox);
        Assert.Equal(2, n.CommitIndex);
        Assert.Equal("v", n.Machine.Data["k"]);
        Assert.Contains(n.TakeApplied(), x => x.Index == 2 && x.Result.Ok);
    }

    [Fact]
    public void Leader_does_not_commit_old_term_entries_by_counting_figure8()
    {
        // журнал лидера: две записи терма 2; лидер сейчас в терме 4. Большинство уже имеет запись 2 (терма 2).
        var p = Log(3, 2, 2);
        var n = Leader(p);
        Assert.Equal(4, n.Term);
        Assert.Equal(3, n.LastIndex); // + пустая запись терма 4
        var outbox = new List<Message>();
        // два последователя подтверждают только первые две записи (терма 2)
        n.Receive(new AppendEntriesReply(1, 0, 4, true, 2, 0, 0), 60, outbox);
        n.Receive(new AppendEntriesReply(2, 0, 4, true, 2, 0, 0), 60, outbox);
        Assert.Equal(0, n.CommitIndex); // нельзя: записи прошлых термов не коммитятся по счётчику
        n.Receive(new AppendEntriesReply(1, 0, 4, true, 3, 0, 0), 61, outbox);
        n.Receive(new AppendEntriesReply(2, 0, 4, true, 3, 0, 0), 61, outbox);
        Assert.Equal(3, n.CommitIndex); // запись текущего терма закоммичена, а с ней и всё, что до неё
    }

    [Fact]
    public void CommitPreviousTermByCount_bug_commits_old_entry_early()
    {
        var n = Leader(Log(3, 2, 2), 0, RaftBug.CommitPreviousTermByCount);
        var outbox = new List<Message>();
        n.Receive(new AppendEntriesReply(1, 0, 4, true, 2, 0, 0), 60, outbox);
        n.Receive(new AppendEntriesReply(2, 0, 4, true, 2, 0, 0), 60, outbox);
        Assert.Equal(2, n.CommitIndex); // ровно то, чего статья запрещает
    }

    [Fact]
    public void Leader_backs_off_next_index_using_conflict_hint_and_retries()
    {
        var p = Log(3, 1, 1, 2, 2, 2);
        var n = Leader(p);
        var outbox = new List<Message>();
        // у последователя журнал короче: он закончился на индексе 3 (конфликтный терм 0)
        n.Receive(new AppendEntriesReply(1, 0, n.Term, false, 0, 0, 3), 60, outbox);
        var ae = outbox.OfType<AppendEntries>().Last();
        Assert.Equal(1, ae.To);
        Assert.Equal(2, ae.PrevLogIndex);
        outbox.Clear();
        // у последователя запись терма 2 на месте, которое у лидера принадлежит терму 1: пропускаем весь терм одним шагом
        n.Receive(new AppendEntriesReply(2, 0, n.Term, false, 0, 1, 1), 61, outbox);
        var ae2 = outbox.OfType<AppendEntries>().Last();
        Assert.Equal(2, ae2.To);
        Assert.Equal(2, ae2.PrevLogIndex); // последняя запись терма 1 у лидера на индексе 2, дальше шлём с индекса 3
    }

    [Fact]
    public void Stale_or_duplicate_replies_never_move_match_index_backwards()
    {
        var n = Leader();
        var outbox = new List<Message>();
        n.Propose(Put("k", "1"), outbox);
        n.Receive(new AppendEntriesReply(1, 0, n.Term, true, 2, 0, 0), 60, outbox);
        n.Receive(new AppendEntriesReply(1, 0, n.Term, true, 1, 0, 0), 61, outbox); // запоздавший
        Assert.Equal(2, n.MatchIndex[1]);
        n.Receive(new AppendEntriesReply(1, 0, n.Term, false, 0, 0, 1), 62, outbox); // отказ не должен откатить ниже подтверждённого
        Assert.True(n.NextIndex[1] > n.MatchIndex[1]);
    }

    [Fact]
    public void Follower_proposal_is_refused_with_leader_hint()
    {
        var n = Node(0);
        n.Receive(AE(1, 0, 0, 0), 1, new List<Message>());
        var r = Assert.IsType<NotLeader>(n.Propose(Put("k", "v"), new List<Message>()));
        Assert.Equal(1, r.Hint);
    }

    [Fact]
    public void Leader_sends_heartbeats_on_schedule()
    {
        var n = Leader();
        var outbox = new List<Message>();
        n.Tick(1000, outbox);
        Assert.Equal(4, outbox.OfType<AppendEntries>().Count());
        outbox.Clear();
        n.Tick(1001, outbox);
        Assert.Empty(outbox);
        n.Tick(1005, outbox);
        Assert.Equal(4, outbox.OfType<AppendEntries>().Count());
    }

    [Fact]
    public void Batches_are_capped()
    {
        var p = new PersistentState { CurrentTerm = 1 };
        for (var i = 0; i < 30; i++) p.Log.Add(E(1, i.ToString()));
        var n = new RaftNode(0, Ids, p, new RaftOptions { MaxBatch = 8 }, new SimRandom(1), 0);
        long now = 0;
        var outbox = Elect(n, ref now);
        foreach (var peer in new[] { 1, 2 }) n.Receive(new RequestVoteReply(peer, 0, n.Term, true), now, outbox);
        outbox.Clear();
        n.Tick(now + 100, outbox);
        Assert.All(outbox.OfType<AppendEntries>(), a => Assert.True(a.Entries.Count <= 8));
    }
}
