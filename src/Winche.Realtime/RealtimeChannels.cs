using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using Npgsql;
using Winche.Rules;

namespace Winche.Realtime;

/// <summary>A message on a channel: never stored, only passed to whoever listens now.</summary>
public sealed record RealtimeMessage(string Path, string Channel, byte[] Payload, string? By, DateTimeOffset At);

/// <summary>This server, as the others know it: an id for the messages and presence it owns.</summary>
public sealed class RealtimeNode
{
    public string Id { get; } = Guid.NewGuid().ToString("N");
}

/// <summary>
/// Channels: messages published at a path (<c>sessions/s1</c>, channel <c>telemetry</c>) reach everyone listening
/// there, on every server, once, and are never stored. Rules see them at <c>{path}/channels/{channel}</c>:
/// <c>create</c> to publish (<c>request.resource.bytes</c>), <c>get</c> to listen.
/// </summary>
public sealed class RealtimeChannels(RealtimeLists lists, RealtimeNode node, WincheRealtimeOptions options)
{
    /// <summary>The Postgres channel messages cross servers on.</summary>
    public const string Channel = "winche_realtime_messages";

    /// <summary>The most a notification carries: Postgres refuses more than 8,000 bytes.</summary>
    internal const int MaxNotificationBytes = 7900;

    // Base64's + and / as themselves: escaped, each is six characters and a 4 KB message no longer fits.
    private static readonly System.Text.Json.JsonSerializerOptions Unescaped = new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Action<RealtimeMessage>>> _listeners = new(StringComparer.Ordinal);

    /// <summary>Publishes <paramref name="payload"/>, if the rules allow: delivered here at once, and to other servers through Postgres.</summary>
    public async Task PublishAsync(IReadOnlyDictionary<string, object?>? caller, string path, string channel, byte[] payload, CancellationToken ct = default)
    {
        Check(path, channel);
        if (payload.Length > options.MaxMessageBytes)
            throw new RealtimeException(WincheErrorCode.InvalidArgument, $"A message is at most {options.MaxMessageBytes} bytes (they cross servers through Postgres).");
        await lists.RequireAsync(RuleOperation.Create, "publish on", RulePath(path, channel), caller, null,
            new Dictionary<string, object?> { ["bytes"] = (long)payload.Length }, ct);
        await SendAsync(caller, path, channel, payload, ct);
    }

    /// <summary>Publishes with the server's rights (functions, the admin SDK from a server): no rules.</summary>
    public async Task ServerPublishAsync(string path, string channel, byte[] payload, CancellationToken ct = default)
    {
        Check(path, channel);
        if (payload.Length > options.MaxMessageBytes)
            throw new RealtimeException(WincheErrorCode.InvalidArgument, $"A message is at most {options.MaxMessageBytes} bytes (they cross servers through Postgres).");
        await SendAsync(null, path, channel, payload, ct);
    }

    private async Task SendAsync(IReadOnlyDictionary<string, object?>? caller, string path, string channel, byte[] payload, CancellationToken ct)
    {
        var message = new RealtimeMessage(path, channel, payload, WincheCaller.UidOf(caller), options.Time.GetUtcNow());
        var wire = new JsonObject
        {
            ["o"] = node.Id, ["p"] = path, ["c"] = channel, ["b"] = message.By, ["d"] = Convert.ToBase64String(payload), ["t"] = message.At.ToUnixTimeMilliseconds(),
        }.ToJsonString(Unescaped);
        // Checked before it goes anywhere: delivered on every server or on none.
        if (System.Text.Encoding.UTF8.GetByteCount(wire) > MaxNotificationBytes)
            throw new RealtimeException(WincheErrorCode.InvalidArgument, "The message is too big to cross servers: make it smaller (about 5 KB at most, less with a long path).");
        Deliver(message);
        await using var cmd = lists.Store.DataSource.CreateCommand("SELECT pg_notify($1, $2)");
        cmd.Parameters.Add(new() { Value = Channel });
        cmd.Parameters.Add(new() { Value = wire });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Whether the caller may listen: refused with a <see cref="RealtimeException"/> if not.</summary>
    public async Task CanListenAsync(IReadOnlyDictionary<string, object?>? caller, string path, string channel, CancellationToken ct = default)
    {
        Check(path, channel);
        await lists.RequireAsync(RuleOperation.Get, "listen on", RulePath(path, channel), caller, null, null, ct);
    }

    /// <summary>Calls <paramref name="heard"/> with every message on the channel from now on, until disposed.</summary>
    public IDisposable Listen(string path, string channel, Action<RealtimeMessage> heard)
    {
        var key = path + "#" + channel;
        var id = Guid.NewGuid();
        _listeners.GetOrAdd(key, _ => new())[id] = heard;
        return new Undo(() =>
        {
            if (_listeners.TryGetValue(key, out var those))
            {
                those.TryRemove(id, out _);
                if (those.IsEmpty) _listeners.TryRemove(new KeyValuePair<string, ConcurrentDictionary<Guid, Action<RealtimeMessage>>>(key, those));
            }
        });
    }

    /// <summary>A message from Postgres: delivered here unless this server published it (it did, already).</summary>
    internal void Heard(string payload)
    {
        if (JsonNode.Parse(payload) is not JsonObject w || (string?)w["o"] == node.Id) return;
        if ((string?)w["p"] is not { } path || (string?)w["c"] is not { } channel || (string?)w["d"] is not { } data) return;
        Deliver(new RealtimeMessage(path, channel, Convert.FromBase64String(data), (string?)w["b"],
            w["t"] is JsonValue t && t.TryGetValue<long>(out var ms) ? DateTimeOffset.FromUnixTimeMilliseconds(ms) : options.Time.GetUtcNow()));
    }

    private void Deliver(RealtimeMessage message)
    {
        if (!_listeners.TryGetValue(message.Path + "#" + message.Channel, out var those)) return;
        foreach (var heard in those.Values)
        {
            try { heard(message); } catch { /* a listener's own failure is its own */ }
        }
    }

    internal static string RulePath(string path, string channel) => $"{path}/channels/{channel}";

    private static void Check(string path, string channel)
    {
        RealtimeStore.CheckPath(path);
        if (channel.Length is 0 or > 100 || !channel.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.'))
            throw new RealtimeException(WincheErrorCode.InvalidArgument, $"'{channel}' isn't a channel name: letters, digits, - _ . (up to 100).");
    }

    private sealed class Undo(Action undo) : IDisposable
    {
        private int _done;
        public void Dispose() { if (Interlocked.Exchange(ref _done, 1) == 0) undo(); }
    }
}
