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
        }, null, new Dictionary<string, string> { ["path"] = list.Path }) { Id = IdOf(list) }, ct);

    /// <summary>
    /// The same for every time this list's closing is published (a resend after a failure included), so receivers can
    /// drop a repeat: from its path and when it closed.
    /// </summary>
    internal static string IdOf(RealtimeListInfo list) => Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(
        System.Text.Encoding.UTF8.GetBytes($"realtime.closed\n{list.Path}\n{list.ClosedAt?.ToUnixTimeMilliseconds()}")))[..32];
}
