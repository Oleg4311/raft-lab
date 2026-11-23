namespace RaftLab.Core;

/// <summary>Состояние, которое по статье обязано переживать перезапуск: терм, голос и журнал. Индексы журнала начинаются с 1.</summary>
public sealed class PersistentState
{
    public long CurrentTerm { get; set; }
    public int? VotedFor { get; set; }
    public List<LogEntry> Log { get; } = new();

    public long LastIndex => Log.Count;
    public long LastTerm => Log.Count == 0 ? 0 : Log[^1].Term;

    public long TermAt(long index) => index <= 0 || index > Log.Count ? 0 : Log[(int)index - 1].Term;
}
