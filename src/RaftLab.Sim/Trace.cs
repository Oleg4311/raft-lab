using System.Text.Json;
using System.Text.Json.Serialization;
using RaftLab.Core;

namespace RaftLab.Sim;

public sealed record TraceEntry(long Term, string Cmd);
/// <summary>Log: последние записи журнала (null: без изменений с прошлого кадра), LogBase: сколько записей слева обрезано.</summary>
public sealed record TraceNode(int Id, bool Alive, string Role, long Term, long Commit, long Applied, int? VotedFor, int Group, List<TraceEntry>? Log, long LogBase);
public sealed record TraceFrame(long T, List<TraceNode> Nodes);
public sealed class TraceMessage
{
    public int Id { get; set; }
    public int From { get; set; }
    public int To { get; set; }
    public string Kind { get; set; } = "";
    public string Text { get; set; } = "";
    public long Sent { get; set; }
    public long Deliver { get; set; }
    public string Fate { get; set; } = "inflight";
    public bool Dup { get; set; }
}
public sealed record TraceEvent(long T, string Text);
public sealed record TraceFile(string Name, string Title, string Description, int Nodes, long Start, long Ticks, string? Violation, List<TraceFrame> Frames, List<TraceMessage> Messages, List<TraceEvent> Events);

/// <summary>Записывает прогон в компактный JSON для просмотрщика: состояния узлов по тикам, сообщения (с судьбой) и события.</summary>
public sealed class TraceRecorder
{
    private readonly List<TraceFrame> _frames = new();
    private readonly Dictionary<int, TraceMessage> _messages = new();
    private readonly List<TraceEvent> _events = new();
    private string?[] _lastLog = Array.Empty<string?>();
    private const int LogTail = 24;
    private const int MaxPerTick = 40;

    public void Event(long t, string text) => _events.Add(new TraceEvent(t, text));

    public void Message(InFlight f, string text)
    {
        _messages[f.Id] = new TraceMessage
        {
            Id = f.Id,
            From = f.Message.From,
            To = f.Message.To,
            Kind = f.Message switch { RequestVote => "vote", RequestVoteReply => "voteReply", AppendEntries => "append", _ => "appendReply" },
            Text = text,
            Sent = f.SentAt,
            Deliver = f.DeliverAt,
            Fate = FateName(f.Fate),
            Dup = f.Duplicate,
        };
    }

    public void Fate(InFlight f)
    {
        if (_messages.TryGetValue(f.Id, out var m)) m.Fate = FateName(f.Fate);
    }

    private static string FateName(Fate f) => f switch { Sim.Fate.Delivered => "delivered", Sim.Fate.Dropped => "dropped", Sim.Fate.Cut => "cut", Sim.Fate.Dead => "dead", _ => "inflight" };

    public void Frame(Cluster c)
    {
        if (_lastLog.Length != c.Config.Nodes) _lastLog = new string?[c.Config.Nodes];
        var nodes = new List<TraceNode>();
        for (var i = 0; i < c.Config.Nodes; i++)
        {
            var node = c.Nodes[i];
            var p = c.Persistent[i];
            var sig = string.Join("|", p.Log.Select(e => $"{e.Term}:{e.Command}"));
            List<TraceEntry>? log = null;
            var skip = Math.Max(0, p.Log.Count - LogTail);
            if (sig != _lastLog[i])
            {
                log = p.Log.Skip(skip).Select(e => new TraceEntry(e.Term, e.Command.ToString())).ToList();
                _lastLog[i] = sig;
            }
            nodes.Add(new TraceNode(i, node is not null, node?.Role.ToString().ToLowerInvariant() ?? "dead", p.CurrentTerm, node?.CommitIndex ?? 0, node?.LastApplied ?? 0, p.VotedFor, c.Groups[i], log, skip));
        }
        _frames.Add(new TraceFrame(c.Now, nodes));
    }

    /// <summary>JSON трассы. С fromTick берётся только хвост прогона: первый кадр окна несёт полные журналы, чтобы окно можно было играть отдельно.</summary>
    public string ToJson(string name, string title, string description, int nodes, string? violation, long fromTick = 0, long toTick = long.MaxValue)
    {
        var frames = _frames.Where(f => f.T >= fromTick && f.T <= toTick).ToList();
        if (frames.Count > 0 && fromTick > 0)
        {
            var current = new TraceNode[nodes];
            foreach (var f in _frames.TakeWhile(f => f.T < fromTick))
                foreach (var n in f.Nodes)
                    if (n.Log is not null || current[n.Id] is null) current[n.Id] = n;
            var first = frames[0];
            frames[0] = first with
            {
                Nodes = first.Nodes.Select(n => n.Log is not null || current[n.Id] is null ? n : n with { Log = current[n.Id].Log, LogBase = current[n.Id].LogBase }).ToList(),
            };
        }
        // при «штормах» сообщений (бесконечные отказы и повторы) просмотрщику хватает первых MaxPerTick сообщений каждого тика
        var perTick = new Dictionary<long, int>();
        var messages = _messages.Values.Where(m => m.Deliver >= fromTick && m.Sent <= toTick).OrderBy(m => m.Id)
            .Where(m => perTick.TryGetValue(m.Sent, out var n) ? ++perTick[m.Sent] <= MaxPerTick : perTick.TryAdd(m.Sent, 1)).ToList();
        var events = _events.Where(e => e.T >= fromTick && e.T <= toTick).ToList();
        var file = new TraceFile(name, title, description, nodes, frames.Count > 0 ? frames[0].T : 0, frames.Count, violation, frames, messages, events);
        return JsonSerializer.Serialize(file, Options);
    }

    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };
}
