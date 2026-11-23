using System.Text;

namespace RaftLab.Core;

/// <summary>Реплицируемый автомат «ключ-значение» с таблицей сессий: повтор команды (тот же ClientId и Seq) возвращает прежний результат.</summary>
public sealed class KvStateMachine
{
    private readonly SortedDictionary<string, string> _data = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (long Seq, CommandResult Result)> _sessions = new();
    private readonly bool _dedup;

    public KvStateMachine(bool dedup = true) => _dedup = dedup;

    public IReadOnlyDictionary<string, string> Data => _data;

    public CommandResult Apply(Command c)
    {
        if (c.Kind == CommandKind.Noop) return new CommandResult(true, null);
        if (_dedup && _sessions.TryGetValue(c.ClientId, out var s))
        {
            if (c.Seq == s.Seq) return s.Result;
            if (c.Seq < s.Seq) return new CommandResult(false, null); // давно устаревший повтор: клиент его уже не ждёт
        }
        var r = Execute(c);
        if (_dedup) _sessions[c.ClientId] = (c.Seq, r);
        return r;
    }

    /// <summary>Чтение без изменения (используется только в сломанном режиме LocalReads).</summary>
    public CommandResult Peek(string key) => _data.TryGetValue(key, out var v) ? new CommandResult(true, v) : new CommandResult(false, null);

    private CommandResult Execute(Command c)
    {
        _data.TryGetValue(c.Key, out var cur);
        switch (c.Kind)
        {
            case CommandKind.Put:
                _data[c.Key] = c.Value!;
                return new CommandResult(true, null);
            case CommandKind.Get:
                return new CommandResult(cur is not null, cur);
            case CommandKind.Cas:
                if (cur == c.Expected)
                {
                    _data[c.Key] = c.Value!;
                    return new CommandResult(true, cur);
                }
                return new CommandResult(false, cur);
            default:
                throw new InvalidOperationException();
        }
    }

    public string Snapshot()
    {
        var sb = new StringBuilder();
        foreach (var (k, v) in _data) sb.Append(k).Append('=').Append(v).Append(';');
        return sb.ToString();
    }
}
