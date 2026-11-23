using System.Diagnostics;
using RaftLab.Core;
using RaftLab.Sim;

return Cli.Entry(args);

internal static class Cli
{
    private const string Usage = """
        raftlab: симулятор Raft с проверкой безопасности и линеаризуемости

        Команды:
          fuzz   [--from 1] [--to 200] [--nodes 5] [--bug ИМЯ] [--chaos 1500] [--settle 500] [--fault 0.012] [--drop 0.02]
                 [--batch 8] [--etmin 15] [--etmax 30] [--stop-first]
                 перебор зёрен; код возврата 0, если нарушений нет (при --bug: 0, если ошибка найдена)
          run    --seed N [--nodes 5] [--bug ИМЯ] [--trace файл.json]
                 один прогон с подробным отчётом, по желанию с записью трассы для просмотрщика
          traces --out каталог
                 записать сценарии для просмотрщика (обычная работа, падение лидера, разделение сети, пойманные ошибки)
          bugs   список встроенных ошибок
        """;

    public static int Entry(string[] args)
    {
        if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
        {
            Console.WriteLine(Usage);
            return args.Length == 0 ? 2 : 0;
        }
        try
        {
            var opts = Parse(args.Skip(1).ToArray());
            return args[0] switch
            {
                "fuzz" => Fuzz(opts),
                "run" => RunOne(opts),
                "traces" => Traces.Write(opts.GetValueOrDefault("out") ?? throw new ArgumentException("нужен --out")),
                "bugs" => Bugs(),
                _ => Fail($"неизвестная команда «{args[0]}»"),
            };
        }
        catch (ArgumentException e)
        {
            return Fail(e.Message);
        }
    }

    private static int Fail(string m)
    {
        Console.Error.WriteLine("ошибка: " + m);
        Console.Error.WriteLine(Usage);
        return 2;
    }

    private static Dictionary<string, string> Parse(string[] a)
    {
        var d = new Dictionary<string, string>();
        for (var i = 0; i < a.Length; i++)
        {
            if (!a[i].StartsWith("--")) throw new ArgumentException($"лишний аргумент «{a[i]}»");
            var key = a[i][2..];
            d[key] = i + 1 < a.Length && !a[i + 1].StartsWith("--") ? a[++i] : "true";
        }
        return d;
    }

    public static RaftBug ParseBug(string? name)
    {
        if (name is null) return RaftBug.None;
        return Enum.TryParse<RaftBug>(name, true, out var b) && b != RaftBug.None ? b : throw new ArgumentException($"неизвестная ошибка «{name}», список: raftlab bugs");
    }

    private static int Int(Dictionary<string, string> o, string k, int def) => o.TryGetValue(k, out var v) ? (int.TryParse(v, out var n) && n > 0 ? n : throw new ArgumentException($"--{k} должно быть положительным числом")) : def;

    private static SimConfig Config(Dictionary<string, string> o) => new()
    {
        Nodes = Int(o, "nodes", 5),
        ChaosTicks = Int(o, "chaos", 1500),
        SettleTicks = Int(o, "settle", 500),
        FaultProb = Dbl(o, "fault", 0.012),
        DropProb = Dbl(o, "drop", 0.02),
        Raft = new RaftOptions
        {
            Bugs = ParseBug(o.GetValueOrDefault("bug")),
            MaxBatch = Int(o, "batch", 8),
            ElectionTimeoutMin = Int(o, "etmin", 15),
            ElectionTimeoutMax = Int(o, "etmax", 30),
        },
    };

    private static double Dbl(Dictionary<string, string> o, string k, double def) =>
        o.TryGetValue(k, out var v) ? (double.TryParse(v, System.Globalization.CultureInfo.InvariantCulture, out var d) && d >= 0 && d <= 1 ? d : throw new ArgumentException($"--{k} должно быть числом от 0 до 1")) : def;

    private static int Bugs()
    {
        foreach (var b in Enum.GetValues<RaftBug>().Where(b => b != RaftBug.None)) Console.WriteLine($"{b,-28} {Traces.BugDescription(b)}");
        return 0;
    }

    private static int Fuzz(Dictionary<string, string> o)
    {
        var cfg = Config(o);
        var from = Int(o, "from", 1);
        var to = Int(o, "to", 200);
        var buggy = cfg.Raft.Bugs != RaftBug.None;
        var sw = Stopwatch.StartNew();
        var failed = new List<RunResult>();
        var byKind = new Dictionary<string, int>();
        long ops = 0, completed = 0, leaders = 0, committed = 0, ticks = 0;
        var n = 0;
        foreach (var r in Runner.Fuzz(cfg, from, to))
        {
            n++;
            ops += r.Operations;
            completed += r.Completed;
            leaders += r.LeadersElected;
            committed += r.Committed;
            ticks += r.Ticks;
            if (buggy ? r.Safety.Count == 0 : r.Ok) continue; // у заведомо сломанной версии доказательство только безопасность и линеаризуемость
            failed.Add(r);
            foreach (var v in buggy ? r.Safety : r.Violations) byKind[v.Name] = byKind.GetValueOrDefault(v.Name) + 1;
            if (!buggy || o.ContainsKey("stop-first")) Console.WriteLine("  " + r.Summary);
            if (o.ContainsKey("stop-first")) break;
        }
        Console.WriteLine($"прогонов {n} за {sw.Elapsed.TotalSeconds:F1} с: нарушений в {failed.Count}; тиков {ticks}, операций {ops} (завершено {completed}), выборов лидера {leaders}, закоммичено записей {committed}");
        if (failed.Count > 0) Console.WriteLine("виды нарушений: " + string.Join(", ", byKind.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key} ×{kv.Value}")));
        if (buggy)
        {
            Console.WriteLine(failed.Count > 0 ? $"ошибка {cfg.Raft.Bugs} поймана: первое зерно {failed[0].Seed}" : $"ошибка {cfg.Raft.Bugs} НЕ поймана на {n} зёрнах");
            return failed.Count > 0 ? 0 : 1;
        }
        return failed.Count == 0 ? 0 : 1;
    }

    private static int RunOne(Dictionary<string, string> o)
    {
        var seed = o.TryGetValue("seed", out var s) && long.TryParse(s, out var sv) ? sv : throw new ArgumentException("нужен --seed");
        var cfg = Config(o) with { Seed = seed };
        var trace = o.ContainsKey("trace") ? new TraceRecorder() : null;
        var r = Runner.Run(cfg, trace);
        Console.WriteLine(r.Summary);
        foreach (var v in r.Violations) Console.WriteLine($"  [{v.Name}] тик {v.Tick}:\n    {v.Message.Replace("\n", "\n    ")}");
        Console.WriteLine($"отпечаток прогона: {r.Digest:x16}");
        if (trace is not null)
        {
            File.WriteAllText(o["trace"], trace.ToJson($"seed-{seed}", $"Прогон {seed}", cfg.Raft.Bugs == RaftBug.None ? "случайный прогон" : $"ошибка: {cfg.Raft.Bugs}", cfg.Nodes, r.Violations.FirstOrDefault()?.Message));
            Console.WriteLine("трасса записана: " + o["trace"]);
        }
        return r.Ok ? 0 : 1;
    }
}
