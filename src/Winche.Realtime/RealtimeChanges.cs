using System.Text.Json.Nodes;
using Winche.Core;

namespace Winche.Realtime;

/// <summary>Realtime's changes, for webhooks and functions' triggers (source <c>realtime</c>): a list closed (by hand or idle).</summary>
public sealed class RealtimeChanges(IEnumerable<IWincheChangeSink> sinks)
{
    public static readonly WincheChangeSource Source = new("realtime", "Realtime", ["closed"], HasPaths: true, Variables: ["path"]);

    /// <summary>The list closed: its path, how much it holds, when.</summary>
    public Task ClosedAsync(RealtimeListInfo list, CancellationToken ct) =>
        WincheChanges.PublishAsync(sinks, new WincheChange("realtime", "closed", list.Path, list.ClosedAt ?? DateTimeOffset.UtcNow, new JsonObject
        {
            ["path"] = list.Path, ["count"] = list.Count, ["bytes"] = list.Bytes,
            ["openedAt"] = list.OpenedAt.ToString("O"), ["closedAt"] = list.ClosedAt?.ToString("O"),
        }, null, new Dictionary<string, string> { ["path"] = list.Path }), ct);
}
