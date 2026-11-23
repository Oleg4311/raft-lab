using RaftLab.Core;

namespace RaftLab.Sim;

public sealed record LinResult(bool Ok, string? Key, string Explanation);

/// <summary>
/// Проверка линеаризуемости истории клиентов (Wing и Gong с запоминанием состояний, по Lowe).
/// Каждый ключ независим, поэтому проверяется отдельно. Операция без ответа (клиент не дождался) могла как выполниться в любой момент
/// после вызова, так и не выполниться совсем.
/// </summary>
public static class Linearizability
{
    public const int MaxOpsPerKey = 62;

    public static LinResult Check(IReadOnlyList<HistoryOp> history)
    {
        foreach (var g in history.GroupBy(h => h.Key).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            var ops = g.OrderBy(o => o.Invoke).ThenBy(o => o.Client).ToList();
            if (ops.Count > MaxOpsPerKey) throw new InvalidOperationException($"ключ {g.Key}: {ops.Count} операций, предел {MaxOpsPerKey}");
            if (!CheckKey(ops)) return new LinResult(false, g.Key, $"История ключа {g.Key} не линеаризуема:\n" + string.Join("\n", ops.Select(o => "  " + o)));
        }
        return new LinResult(true, null, "линеаризуема");
    }

    /// <summary>
    /// Самый ранний момент, к которому история уже нелинеаризуема: операции, не завершённые к этому моменту, считаются незавершёнными.
    /// Префикс линеаризуемой истории линеаризуем, поэтому момент ищется бинарным поиском по моментам завершения операций.
    /// </summary>
    public static (long Tick, LinResult Result) FirstViolation(IReadOnlyList<HistoryOp> history)
    {
        var returns = history.Where(h => h.Completed).Select(h => h.Return).Distinct().OrderBy(x => x).ToList();
        var lo = 0;
        var hi = returns.Count - 1;
        while (lo < hi)
        {
            var mid = (lo + hi) / 2;
            if (Check(Truncate(history, returns[mid])).Ok) lo = mid + 1;
            else hi = mid;
        }
        var tick = returns.Count == 0 ? 0 : returns[lo];
        return (tick, Check(Truncate(history, tick)));
    }

    public static List<HistoryOp> Truncate(IReadOnlyList<HistoryOp> history, long tick) =>
        history.Where(h => h.Invoke <= tick).Select(h => h.Return is >= 0 && h.Return <= tick
            ? h
            : new HistoryOp { Client = h.Client, Seq = h.Seq, Kind = h.Kind, Key = h.Key, Value = h.Value, Expected = h.Expected, Invoke = h.Invoke }).ToList();

    private static bool CheckKey(List<HistoryOp> ops)
    {
        var n = ops.Count;
        var needMask = 0UL; // все завершённые операции обязаны попасть в порядок
        for (var i = 0; i < n; i++)
            if (ops[i].Completed) needMask |= 1UL << i;
        var failed = new HashSet<(ulong, string?)>();

        bool Search(ulong done, string? state)
        {
            if ((done & needMask) == needMask) return true; // незавершённые можно не включать
            if (!failed.Add((done, state))) return false;
            // операция i может идти следующей, если ни одна не включённая завершённая операция не закончилась до её начала
            var minReturn = long.MaxValue;
            for (var j = 0; j < n; j++)
                if ((done & (1UL << j)) == 0 && ops[j].Completed) minReturn = Math.Min(minReturn, ops[j].Return);
            for (var i = 0; i < n; i++)
            {
                if ((done & (1UL << i)) != 0) continue;
                if (ops[i].Invoke > minReturn) break; // дальше по списку только операции, начавшиеся ещё позже
                if (TryApply(ops[i], state, out var next) && Search(done | (1UL << i), next)) return true;
            }
            return false;
        }

        return Search(0, null);
    }

    /// <summary>Допустима ли операция в данном состоянии регистра, и каким оно станет.</summary>
    private static bool TryApply(HistoryOp op, string? state, out string? next)
    {
        next = state;
        switch (op.Kind)
        {
            case CommandKind.Put:
                next = op.Value;
                return !op.Completed || op.Result!.Ok;
            case CommandKind.Get:
                return !op.Completed || (op.Result!.Ok == (state is not null) && op.Result.Value == state);
            case CommandKind.Cas:
                var swap = state == op.Expected;
                if (op.Completed && (op.Result!.Ok != swap || op.Result.Value != state)) return false;
                if (swap) next = op.Value;
                return true;
            default:
                return false;
        }
    }
}
