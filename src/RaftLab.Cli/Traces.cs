using System.Text;
using System.Text.Json;
using RaftLab.Core;
using RaftLab.Sim;

/// <summary>Сценарии для просмотрщика: записываются в JSON и воспроизводятся в браузере. Всё детерминировано: CI проверяет, что трассы в репозитории совпадают с пересчитанными.</summary>
internal static class Traces
{
    public static string BugDescription(RaftBug b) => b switch
    {
        RaftBug.CommitPreviousTermByCount => "запись прошлого терма коммитится по числу копий (рис. 8 статьи)",
        RaftBug.NoUpToDateCheck => "голос отдаётся без проверки свежести журнала кандидата",
        RaftBug.VoteNotPersisted => "голос не сохраняется: после перезапуска узел голосует повторно в том же терме",
        RaftBug.TruncateAlways => "AppendEntries с записями всегда обрезает хвост журнала, даже совпадающий",
        RaftBug.LocalReads => "лидер читает из своего состояния без подтверждения лидерства",
        RaftBug.NoDedup => "нет защиты от повторного применения одной и той же команды",
        _ => b.ToString(),
    };

    private sealed record Item(string Name, string Title, string Description, string? Bug);

    private static SimConfig Calm(long seed) => new()
    {
        Seed = seed,
        DropProb = 0,
        DuplicateProb = 0,
        Clients = 2,
        OpProb = 0.06,
        ClientTimeout = 40,
    };

    public static int Write(string dir)
    {
        Directory.CreateDirectory(dir);
        var items = new List<Item>();

        void Save(string name, string title, string desc, string? bug, string json)
        {
            File.WriteAllText(Path.Combine(dir, name + ".json"), json, new UTF8Encoding(false));
            items.Add(new Item(name, title, desc, bug));
            Console.WriteLine($"  {name}.json: {json.Length / 1024} КБ");
        }

        Save("normal", "Обычная работа", "Пять узлов выбирают лидера, клиенты пишут и читают. Каждая запись реплицируется на большинство, и только потом считается принятой.", null,
            Scripted("normal", 160, Calm(7), c => c.At(120, c.StopClients)));

        Save("leader-crash", "Падение лидера", "Лидер падает, остальные ждут тайм-аут, выбирают нового, а вернувшийся узел догоняет журнал. Ни одна принятая запись не теряется.", null,
            Scripted("leader-crash", 260, Calm(11), c =>
            {
                var victim = -1;
                c.At(90, () =>
                {
                    victim = c.CurrentLeader() ?? 0;
                    c.Crash(victim);
                });
                c.At(170, () => c.Restart(victim));
                c.At(220, c.StopClients);
            }));

        Save("partition", "Разделение сети", "Лидер с одним последователем отрезан от остальных трёх. Меньшинство не может принять запись, большинство выбирает нового лидера. После починки старый лидер уступает и теряет свои непринятые записи.", null,
            Scripted("partition", 280, Calm(5), c =>
            {
                c.At(90, () =>
                {
                    var l = c.CurrentLeader() ?? 0;
                    var friend = (l + 1) % c.Config.Nodes;
                    var minority = new[] { l, friend };
                    c.Partition(minority, Enumerable.Range(0, c.Config.Nodes).Where(i => !minority.Contains(i)).ToArray());
                });
                c.At(190, c.Heal);
                c.At(240, c.StopClients);
            }));

        foreach (var (bug, cfg, _) in KnownBugs.Runs())
        {
            var trace = new TraceRecorder();
            var r = Runner.Run(cfg, trace);
            if (r.Safety.Count == 0) throw new InvalidOperationException($"для {bug} не найдено нарушение на зерне {cfg.Seed}: параметры устарели");
            var v = r.Safety[0];
            var title = "Ошибка: " + BugTitle(bug);
            var desc = $"{BugDescription(bug)}. Проверка поймала нарушение «{v.Name}» на зерне {cfg.Seed}.";
            Save("bug-" + bug.ToString().ToLowerInvariant(), title, desc, bug.ToString(), trace.ToJson("bug-" + bug.ToString().ToLowerInvariant(), title, desc, cfg.Nodes, $"{v.Name}: {v.Message}", Math.Max(0, v.Tick - 160), v.Tick + 3));
        }

        var index = items.Select(i => new { name = i.Name, title = i.Title, description = i.Description, bug = i.Bug, file = i.Name + ".json" });
        var indexJson = JsonSerializer.Serialize(index, new JsonSerializerOptions { WriteIndented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }).Replace("\r\n", "\n") + "\n";
        File.WriteAllText(Path.Combine(dir, "index.json"), indexJson, new UTF8Encoding(false)); // одинаковые переводы строк на любой ОС: CI сверяет файлы побайтно
        return 0;
    }

    private static string BugTitle(RaftBug b) => b switch
    {
        RaftBug.CommitPreviousTermByCount => "коммит старого терма (рис. 8)",
        RaftBug.NoUpToDateCheck => "голос без проверки свежести журнала",
        RaftBug.VoteNotPersisted => "голос не сохраняется на диск",
        RaftBug.TruncateAlways => "лишнее обрезание журнала",
        RaftBug.LocalReads => "чтение без подтверждения лидерства",
        RaftBug.NoDedup => "повторное применение команд",
        _ => b.ToString(),
    };

    private static string Scripted(string name, int ticks, SimConfig cfg, Action<Cluster> script)
    {
        var trace = new TraceRecorder();
        var c = new Cluster(cfg) { Trace = trace };
        script(c);
        for (var t = 0; t < ticks; t++) c.Step();
        if (c.Checks.Failed) throw new InvalidOperationException($"сценарий {name}: нарушение " + c.Checks.Violations[0].Message);
        var lin = Linearizability.Check(c.History);
        if (!lin.Ok) throw new InvalidOperationException($"сценарий {name}: " + lin.Explanation);
        var item = name;
        return trace.ToJson(item, item, "", cfg.Nodes, null);
    }
}
