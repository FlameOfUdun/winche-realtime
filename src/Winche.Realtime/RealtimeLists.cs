using Winche.Rules;
using Winche.Rules.Evaluation;
using Winche.Values;

namespace Winche.Realtime;

/// <summary>
/// Lists as the project's callers use them: every append, read and subscription checked against the project's rules
/// first (default deny), then done by the <see cref="RealtimeStore"/>; appends and state changes announced on the
/// <see cref="Notifier"/>.
/// </summary>
/// <remarks>
/// Rules use the shared operations: <c>get</c> (read and subscribe), <c>create</c> (append), <c>update</c> (open and
/// close). <c>resource</c> is the list as it is (<c>state</c>, <c>count</c>, <c>bytes</c>), or null when there's none;
/// for an append, <c>request.resource</c> is the batch: <c>count</c>, <c>bytes</c>, <c>keys</c>, <c>devices</c> (each
/// key's part before its first <c>:</c>) and <c>device</c> (the one device all its keys belong to, or null), so a rule can
/// say <c>request.resource.device == request.auth.uid</c>. Payloads are never shown to rules.
/// </remarks>
public sealed class RealtimeLists(RealtimeStore store, RuleEngine rules, RealtimeNotifier notifier, RealtimeChanges? changes = null)
{
    public RealtimeStore Store { get; } = store;
    public RealtimeNotifier Notifier { get; } = notifier;

    /// <summary>Lists with their own store, rules (from <paramref name="options"/>) and notifier: for tests and simple hosts.</summary>
    public static RealtimeLists Create(WincheRealtimeOptions options) =>
        new(new RealtimeStore(options), new RuleEngine(new MutableRuleSetRepository(RuleSet.Merge(options.Rulesets))), new RealtimeNotifier());

    /// <summary>Appends, if the rules allow <c>create</c>: the items' positions.</summary>
    public async Task<IReadOnlyList<long>> AppendAsync(IReadOnlyDictionary<string, object?>? caller, string path, IReadOnlyList<RealtimeItemIn> items, CancellationToken ct = default)
    {
        RealtimeStore.CheckPath(path);
        var list = await Store.InfoAsync(path, ct);
        await RequireAsync(RuleOperation.Create, "append to", path, caller, list, Batch(items), ct);
        var positions = await Store.AppendAsync(path, items, WincheCaller.UidOf(caller), ct);
        Notifier.Changed(path, positions.Max());
        return positions;
    }

    /// <summary>A page of the list, if the rules allow <c>get</c>.</summary>
    public async Task<RealtimePage> ReadAsync(IReadOnlyDictionary<string, object?>? caller, string path, long after, int limit, CancellationToken ct = default)
    {
        await CanReadAsync(caller, path, ct);
        return await Store.ReadAsync(path, after, limit, ct);
    }

    /// <summary>Whether the caller may read (and subscribe to) the list: refused with a <see cref="RealtimeException"/> if not; the list, or null.</summary>
    public async Task<RealtimeListInfo?> CanReadAsync(IReadOnlyDictionary<string, object?>? caller, string path, CancellationToken ct = default)
    {
        RealtimeStore.CheckPath(path);
        var list = await Store.InfoAsync(path, ct);
        await RequireAsync(RuleOperation.Get, "read", path, caller, list, null, ct);
        return list;
    }

    /// <summary>Opens a list, if the rules allow <c>update</c>.</summary>
    public async Task<RealtimeListInfo> OpenAsync(IReadOnlyDictionary<string, object?>? caller, string path, CancellationToken ct = default)
    {
        RealtimeStore.CheckPath(path);
        await RequireAsync(RuleOperation.Update, "open", path, caller, await Store.InfoAsync(path, ct), null, ct);
        var list = await Store.OpenAsync(path, ct);
        Notifier.Changed(path, list.Count);
        return list;
    }

    /// <summary>Closes a list, if the rules allow <c>update</c>.</summary>
    public async Task<RealtimeListInfo> CloseAsync(IReadOnlyDictionary<string, object?>? caller, string path, CancellationToken ct = default)
    {
        RealtimeStore.CheckPath(path);
        await RequireAsync(RuleOperation.Update, "close", path, caller, await Store.InfoAsync(path, ct), null, ct);
        var list = await Store.CloseAsync(path, ct);
        Notifier.Changed(path, list.Count);
        if (changes is not null) await changes.ClosedAsync(list, ct);
        return list;
    }

    /// <summary>Whether the caller may append <paramref name="items"/> to <paramref name="path"/>: refused if not (nothing appended).</summary>
    internal async Task CanAppendAsync(IReadOnlyDictionary<string, object?>? caller, string path, IReadOnlyList<RealtimeItemIn> items, CancellationToken ct)
    {
        RealtimeStore.CheckPath(path);
        await RequireAsync(RuleOperation.Create, "append to", path, caller, await Store.InfoAsync(path, ct), Batch(items), ct);
    }

    private static Dictionary<string, object?> Batch(IReadOnlyList<RealtimeItemIn> items)
    {
        var devices = items.Select(i => i.Key is { } k && k.IndexOf(':') is var at and > 0 ? k[..at] : i.Key).Distinct().ToList();
        return new Dictionary<string, object?>
        {
            ["count"] = (long)items.Count,
            ["bytes"] = items.Sum(i => (long)(i.Payload?.Length ?? 0)),
            ["keys"] = items.Select(i => (object?)i.Key).ToList(),
            ["devices"] = devices.Select(d => (object?)d).ToList(),
            ["device"] = devices.Count == 1 ? devices[0] : null,   // one device's batch, or null when mixed
        };
    }

    /// <summary>The rules' word on <paramref name="operation"/> at <paramref name="rulePath"/> (channels and presence use their own paths).</summary>
    internal Task RequireAsync(RuleOperation operation, string verb, string rulePath, IReadOnlyDictionary<string, object?>? caller,
        IReadOnlyDictionary<string, object?>? resource, IReadOnlyDictionary<string, object?>? incoming, CancellationToken ct) =>
        CheckAsync(operation, verb, rulePath, caller, resource is null ? RuleValues.Null : Value.FromObject(resource), incoming, ct);

    private Task RequireAsync(RuleOperation operation, string verb, string path, IReadOnlyDictionary<string, object?>? caller,
        RealtimeListInfo? list, IReadOnlyDictionary<string, object?>? incoming, CancellationToken ct) =>
        CheckAsync(operation, verb, path, caller, list is null ? RuleValues.Null : Value.FromObject(new Dictionary<string, object?>
        {
            ["state"] = list.State, ["count"] = list.Count, ["bytes"] = list.Bytes,
        }), incoming, ct);

    private async Task CheckAsync(RuleOperation operation, string verb, string path, IReadOnlyDictionary<string, object?>? caller,
        Value resource, IReadOnlyDictionary<string, object?>? incoming, CancellationToken ct)
    {
        var request = new RuleRequest
        {
            Resource = resource,
            Request = RuleValues.Request(caller, operation.ToString().ToLowerInvariant(), incoming is null ? RuleValues.Null : Value.FromObject(incoming)),
        };
        if (!await rules.AllowsAsync(operation, path, request, ct))
            throw new RealtimeException(WincheErrorCode.PermissionDenied, $"The project's rules don't let you {verb} {path}.", "denied");
    }
}
