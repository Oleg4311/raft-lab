namespace RaftLab.Core;

public enum Role { Follower, Candidate, Leader }

public enum CommandKind { Noop, Put, Get, Cas }

/// <summary>Команда клиента. Пара (ClientId, Seq) нужна для «ровно одного применения» при повторах.</summary>
public sealed record Command(CommandKind Kind, string Key, string? Value, string? Expected, string ClientId, long Seq)
{
    public static readonly Command Noop = new(CommandKind.Noop, "", null, null, "", 0);

    public override string ToString() => Kind switch
    {
        CommandKind.Noop => "noop",
        CommandKind.Put => $"put {Key}={Value}",
        CommandKind.Get => $"get {Key}",
        _ => $"cas {Key} {Expected ?? "∅"}→{Value}",
    };
}

public sealed record LogEntry(long Term, Command Command);

/// <summary>Результат применения команды. Для Get: Ok = ключ существует, Value = значение. Для Cas: Ok = обмен выполнен, Value = прежнее значение.</summary>
public sealed record CommandResult(bool Ok, string? Value);

public sealed record AppliedEntry(long Index, LogEntry Entry, CommandResult Result);

public abstract record Message(int From, int To, long Term);

public sealed record RequestVote(int From, int To, long Term, long LastLogIndex, long LastLogTerm) : Message(From, To, Term);

public sealed record RequestVoteReply(int From, int To, long Term, bool Granted) : Message(From, To, Term);

public sealed record AppendEntries(int From, int To, long Term, long PrevLogIndex, long PrevLogTerm, IReadOnlyList<LogEntry> Entries, long LeaderCommit) : Message(From, To, Term);

/// <summary>ConflictTerm/ConflictIndex ускоряют откат nextIndex при несовпадении (раздел 5.3 статьи, оптимизация).</summary>
public sealed record AppendEntriesReply(int From, int To, long Term, bool Success, long MatchIndex, long ConflictTerm, long ConflictIndex) : Message(From, To, Term);

/// <summary>Намеренно внесённые ошибки: нужны, чтобы показать, что проверки их ловят.</summary>
[Flags]
public enum RaftBug
{
    None = 0,
    /// <summary>Фиг. 8 статьи: запись прошлого терма считается закоммиченной по числу копий.</summary>
    CommitPreviousTermByCount = 1,
    /// <summary>Голос отдаётся без проверки «свежести» журнала кандидата.</summary>
    NoUpToDateCheck = 2,
    /// <summary>Голос не сохраняется на диск: после перезапуска узел голосует повторно в том же терме.</summary>
    VoteNotPersisted = 4,
    /// <summary>Любой AppendEntries с записями обрезает журнал после prevLogIndex, даже если хвост уже совпадает.</summary>
    TruncateAlways = 8,
    /// <summary>Лидер отвечает на чтение из своего состояния, не проверяя, что он всё ещё лидер.</summary>
    LocalReads = 16,
    /// <summary>Автомат не отсеивает повторы команд (нет «ровно одного применения»).</summary>
    NoDedup = 32,
}

public sealed record RaftOptions
{
    public int ElectionTimeoutMin { get; init; } = 15;
    public int ElectionTimeoutMax { get; init; } = 30;
    public int HeartbeatInterval { get; init; } = 5;
    public int MaxBatch { get; init; } = 8;
    public RaftBug Bugs { get; init; } = RaftBug.None;
}

public abstract record ProposeResult;
public sealed record Accepted(long Index, long Term) : ProposeResult;
public sealed record NotLeader(int? Hint) : ProposeResult;
/// <summary>Используется только при ошибке LocalReads: ответ без прохода через журнал.</summary>
public sealed record LocalAnswer(CommandResult Result) : ProposeResult;
