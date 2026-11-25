using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Winche.Realtime.AspNetCore;

/// <summary>The socket's settings.</summary>
public sealed class RealtimeSocketOptions
{
    /// <summary>The largest frame a client may send, in bytes. Default 4 MB (a batch of base64 payloads).</summary>
    public int MaxFrameBytes { get; set; } = 4 * 1024 * 1024;

    /// <summary>The most subscriptions one connection may hold. Default 100.</summary>
    public int MaxSubscriptions { get; set; } = 100;

    /// <summary>
    /// The most a connection's frames waiting to go out may add up to, in bytes. Default 8 MB. When it's full, the
    /// connection's subscriptions wait (they read on from their positions after) and channel messages are dropped.
    /// </summary>
    public long SendQueueBytes { get; set; } = 8 * 1024 * 1024;

    /// <summary>The most item payloads one frame (a subscription's page, a read's answer) carries, in bytes. Default 1 MB; always at least one item.</summary>
    public long FrameBytes { get; set; } = 1024 * 1024;

    /// <summary>How long a connection's queue may stay full before it's closed as too slow (<see cref="RealtimeSocketExtensions.SlowCloseCode"/>). Default 30 s.</summary>
    public TimeSpan SlowClientTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long the server waits for the client to answer its close (an expired sign-in, too slow) before it cuts the connection. Default 5 s.</summary>
    public TimeSpan CloseGrace { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The most items a subscription sends in one frame. Default 500.</summary>
    public int ItemsPerFrame { get; set; } = 500;
}

public static class RealtimeSocketExtensions
{
    /// <summary>A sign-in that has expired: the client signs in again and reconnects (subscribing from its positions).</summary>
    public const int ExpiredCloseCode = 4401;

    /// <summary>The client didn't take what was sent for too long: it reconnects (subscribing from its positions).</summary>
    public const int SlowCloseCode = 4408;

    public static IServiceCollection AddWincheRealtimeWsApi(this IServiceCollection services, Action<RealtimeSocketOptions>? configure = null)
    {
        var options = new RealtimeSocketOptions();
        configure?.Invoke(options);
        services.AddSingleton(options);
        return services;
    }

    /// <summary>Realtime's WebSocket at <c>/{prefix}/ws</c> (default <c>/realtime/v1/ws</c>).</summary>
    public static IEndpointConventionBuilder MapWincheRealtimeWsApi(this IEndpointRouteBuilder app, string prefix = "realtime/v1") =>
        app.Map($"/{prefix.Trim('/')}/ws", HandleAsync);

    private static async Task HandleAsync(HttpContext http)
    {
        if (!http.WebSockets.IsWebSocketRequest)
        {
            http.Response.StatusCode = StatusCodes.Status400BadRequest;
            await http.Response.WriteAsJsonAsync(new WincheError(WincheErrorCode.InvalidArgument, "Realtime's API is a WebSocket: connect with ws(s)://…/ws.").ToBody());
            return;
        }
        var caller = await http.WincheCallerOf();
        using var socket = await http.WebSockets.AcceptWebSocketAsync();
        var connection = new RealtimeConnection(socket, caller, http.RequestServices.GetRequiredService<RealtimeLists>(),
            http.RequestServices.GetRequiredService<RealtimeChannels>(), http.RequestServices.GetRequiredService<RealtimePresence>(),
            http.RequestServices.GetService<RealtimeSocketOptions>() ?? new RealtimeSocketOptions(),
            http.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("Winche.Realtime.Socket"));
        await connection.RunAsync(http.RequestAborted);
    }
}

/// <summary>One client's connection: its requests, its subscriptions, one writer for what goes out.</summary>
internal sealed class RealtimeConnection(WebSocket socket, IReadOnlyDictionary<string, object?>? caller, RealtimeLists lists,
    RealtimeChannels channels, RealtimePresence presence, RealtimeSocketOptions options, ILogger log)
{
    private readonly Channel<byte[]> _out = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
    private readonly object _gate = new();
    private long _queued;
    private TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private WebSocketCloseStatus _closeCode = WebSocketCloseStatus.NormalClosure;
    private string _closeWhy = "bye";
    private bool _closeRequested;
    /// <summary>One subscription reads a page at a time: what a connection holds stays near its queue.</summary>
    internal SemaphoreSlim Producing { get; } = new(1, 1);
    /// <summary>The bytes of frames waiting to go out.</summary>
    internal long QueuedBytes { get { lock (_gate) return _queued; } }
    private readonly Dictionary<string, IDisposable> _subscriptions = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _closing = new();
    private readonly string _id = Guid.NewGuid().ToString("N");
    internal RealtimeLists Lists => lists;
    internal RealtimeSocketOptions Options => options;
    internal RealtimePresence Presence => presence;
    internal ILogger Log => log;

    public async Task RunAsync(CancellationToken aborted)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(aborted, _closing.Token);
        var ct = linked.Token;
        var writing = WriteAsync(ct);
        if (ExpiresAt() is { } at) _ = ExpireAsync(at, ct);
        try
        {
            await SendAsync(new JsonObject { ["type"] = "welcome", ["connectionId"] = _id }, ct);
            await ReceiveAsync(ct);
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        finally
        {
            lock (_subscriptions) { foreach (var s in _subscriptions.Values) s.Dispose(); _subscriptions.Clear(); }
            // Gone without leaving: out of every presence set, its on-disconnect items appended.
            try { await presence.DisconnectedAsync(_id, CancellationToken.None); }
            catch (Exception e) { log.LogWarning(e, "Realtime couldn't end connection {Connection}'s presence: the sweep will.", _id); }
            _out.Writer.TryComplete();
            try { await writing; } catch { /* gone */ }
            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                using var grace = new CancellationTokenSource(options.CloseGrace);
                try { await socket.CloseAsync(_closeCode, _closeWhy, grace.Token); } catch { /* gone */ }
            }
        }
    }

    /// <summary>When the caller's sign-in expires (its token's exp), or null when it doesn't say or says something that isn't a date.</summary>
    private DateTimeOffset? ExpiresAt()
    {
        try
        {
            long? at = caller?.GetValueOrDefault("exp") switch
            {
                long l => l, int i => i, double d when double.IsFinite(d) => (long)Math.Clamp(d, long.MinValue, long.MaxValue),
                JsonElement { ValueKind: JsonValueKind.Number } e when e.TryGetInt64(out var n) => n, _ => null,
            };
            if (at is null) return null;
            return at.Value >= DateTimeOffset.MaxValue.ToUnixTimeSeconds() ? null   // after any date: as good as none
                : DateTimeOffset.FromUnixTimeSeconds(Math.Max(0, at.Value));
        }
        catch (ArgumentOutOfRangeException) { return null; }
    }

    /// <summary>Closes with 4401 when the sign-in expires: waits in steps a .NET timer can take (24 days at most).</summary>
    private async Task ExpireAsync(DateTimeOffset at, CancellationToken ct)
    {
        try
        {
            while (true)
            {
                var left = at - DateTimeOffset.UtcNow;
                if (left <= TimeSpan.Zero) break;
                await Task.Delay(left < TimeSpan.FromDays(24) ? left : TimeSpan.FromDays(24), ct);
            }
            Close(RealtimeSocketExtensions.ExpiredCloseCode, "sign-in expired");
        }
        catch (OperationCanceledException) { }
    }

    private async Task ReceiveAsync(CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        while (!ct.IsCancellationRequested)
        {
            using var message = new MemoryStream();
            WebSocketReceiveResult r;
            do
            {
                r = await socket.ReceiveAsync(buffer, ct);
                if (r.MessageType == WebSocketMessageType.Close) return;
                message.Write(buffer, 0, r.Count);
                if (message.Length > options.MaxFrameBytes)
                {
                    await socket.CloseAsync(WebSocketCloseStatus.MessageTooBig, $"Frames are at most {options.MaxFrameBytes} bytes.", CancellationToken.None);
                    return;
                }
            } while (!r.EndOfMessage);

            if (Volatile.Read(ref _closeRequested)) continue;   // closing: only the client's close is awaited
            JsonObject? frame = null;
            try { frame = JsonNode.Parse(message.ToArray()) as JsonObject; } catch (JsonException) { }
            if (frame is null)
            {
                await SendAsync(Error(null, new RealtimeException(WincheErrorCode.InvalidArgument, "A frame is a JSON object: { type, id, … }.")), ct);
                continue;
            }
            await HandleAsync(frame, ct);
        }
    }

    private async Task HandleAsync(JsonObject frame, CancellationToken ct)
    {
        var id = frame["id"]?.GetValueKind() == JsonValueKind.String ? (string?)frame["id"] : null;
        try
        {
            var result = (string?)frame["type"] switch
            {
                "ping" => new JsonObject(),
                "append" => await AppendAsync(frame, ct),
                "read" => await ReadAsync(frame, ct),
                "subscribe" => await SubscribeAsync(frame, id, ct),
                "unsubscribe" => Unsubscribe(frame),
                "publish" => await PublishAsync(frame, ct),
                "listen" => await ListenAsync(frame, id, ct),
                "join" => await JoinAsync(frame, ct),
                "leave" => await LeaveAsync(frame, ct),
                "watch" => await WatchAsync(frame, id, ct),
                "open" => new JsonObject { ["list"] = List(await lists.OpenAsync(caller, Path(frame), ct)) },
                "close" => new JsonObject { ["list"] = List(await lists.CloseAsync(caller, Path(frame), ct)) },
                var other => throw new RealtimeException(WincheErrorCode.InvalidArgument,
                    $"'{other}' isn't a request: ping, append, read, subscribe, unsubscribe, open, close, publish, listen, join, leave or watch."),
            };
            await SendAsync(new JsonObject { ["type"] = "response", ["id"] = id, ["result"] = result }, ct);
        }
        catch (RealtimeException e) { await SendAsync(Error(id, e), ct); }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            var error = WincheError.Unexpected(e, log, "realtime." + ((string?)frame["type"] ?? "frame"), WincheCaller.UidOf(caller));
            await SendAsync(new JsonObject { ["type"] = "error", ["id"] = id, ["error"] = JsonSerializer.SerializeToNode(error) }, ct);
        }
    }

    private async Task<JsonObject> AppendAsync(JsonObject frame, CancellationToken ct)
    {
        if (frame["items"] is not JsonArray given) throw new RealtimeException(WincheErrorCode.InvalidArgument, "Give items: [{ key, payload }] (payload in base64).");
        var items = new List<RealtimeItemIn>(given.Count);
        foreach (var node in given)
        {
            var key = node?["key"]?.GetValueKind() == JsonValueKind.String ? (string)node["key"]! : "";
            byte[] payload;
            try { payload = Convert.FromBase64String(node?["payload"]?.GetValueKind() == JsonValueKind.String ? (string)node["payload"]! : ""); }
            catch (FormatException) { throw new RealtimeException(WincheErrorCode.InvalidArgument, $"{key}: payload is base64."); }
            items.Add(new RealtimeItemIn(key, payload));
        }
        var positions = await lists.AppendAsync(caller, Path(frame), items, ct);
        return new JsonObject { ["positions"] = new JsonArray(positions.Select(p => (JsonNode)p).ToArray()) };
    }

    private async Task<JsonObject> ReadAsync(JsonObject frame, CancellationToken ct)
    {
        var page = await lists.ReadAsync(caller, Path(frame), After(frame), frame["limit"] is JsonValue l && l.TryGetValue<int>(out var limit) ? limit : 500, options.FrameBytes, ct);
        return new JsonObject { ["items"] = Items(page.Items), ["list"] = page.List is null ? null : List(page.List), ["more"] = page.More };
    }

    private async Task<JsonObject> SubscribeAsync(JsonObject frame, string? id, CancellationToken ct)
    {
        var path = Path(frame);
        var list = await lists.CanReadAsync(caller, path, ct);
        var name = id ?? Guid.NewGuid().ToString("N");
        lock (_subscriptions)
        {
            if (_subscriptions.Count >= options.MaxSubscriptions)
                throw new RealtimeException(WincheErrorCode.ResourceExhausted, $"A connection holds at most {options.MaxSubscriptions} subscriptions.");
            if (_subscriptions.ContainsKey(name)) throw new RealtimeException(WincheErrorCode.AlreadyExists, $"There's a subscription {name} already.");
            _subscriptions[name] = new Subscription(this, name, path, After(frame), list?.State, ct);
        }
        return new JsonObject { ["subscription"] = name, ["list"] = list is null ? null : List(list) };
    }

    private void Hold(string name, IDisposable subscription)
    {
        lock (_subscriptions)
        {
            if (_subscriptions.Count >= options.MaxSubscriptions)
            {
                subscription.Dispose();
                throw new RealtimeException(WincheErrorCode.ResourceExhausted, $"A connection holds at most {options.MaxSubscriptions} subscriptions.");
            }
            if (!_subscriptions.TryAdd(name, subscription))
            {
                subscription.Dispose();
                throw new RealtimeException(WincheErrorCode.AlreadyExists, $"There's a subscription {name} already.");
            }
        }
    }

    // ── channels ────────────────────────────────────────────────────────────────

    private async Task<JsonObject> PublishAsync(JsonObject frame, CancellationToken ct)
    {
        await channels.PublishAsync(caller, Path(frame), ChannelOf(frame), Payload(frame), ct);
        return new JsonObject();
    }

    /// <summary>
    /// Listens on a channel: each message, as it comes, as a <c>message</c> frame. Live only: a connection too slow to
    /// take them misses some (channels keep nothing to catch up from).
    /// </summary>
    private async Task<JsonObject> ListenAsync(JsonObject frame, string? id, CancellationToken ct)
    {
        var path = Path(frame);
        var channel = ChannelOf(frame);
        await channels.CanListenAsync(caller, path, channel, ct);
        var name = id ?? Guid.NewGuid().ToString("N");
        Hold(name, channels.Listen(path, channel, m => TryEnqueue(new JsonObject
        {
            ["type"] = "message", ["subscription"] = name, ["path"] = m.Path, ["channel"] = m.Channel, ["by"] = m.By,
            ["at"] = m.At.ToString("O"), ["payload"] = Convert.ToBase64String(m.Payload),
        })));
        return new JsonObject { ["subscription"] = name };
    }

    // ── presence ────────────────────────────────────────────────────────────────

    private async Task<JsonObject> JoinAsync(JsonObject frame, CancellationToken ct)
    {
        RealtimeItemIn? onDisconnect = null;
        if (frame["onDisconnect"] is JsonObject od)
        {
            var key = od["key"]?.GetValueKind() == JsonValueKind.String ? (string)od["key"]! : "";
            onDisconnect = new RealtimeItemIn(key, Payload(od));
            if (key.Length == 0) throw new RealtimeException(WincheErrorCode.InvalidArgument, "onDisconnect needs a key and a payload (base64).");
        }
        await presence.JoinAsync(caller, _id, Path(frame), frame["data"] as JsonObject, onDisconnect, ct);
        return new JsonObject();
    }

    private async Task<JsonObject> LeaveAsync(JsonObject frame, CancellationToken ct)
    {
        await presence.LeaveAsync(_id, Path(frame), ct);
        return new JsonObject();
    }

    /// <summary>Watches who's at a path: the members now (the answer), then a <c>presence</c> frame with them each time they change.</summary>
    private async Task<JsonObject> WatchAsync(JsonObject frame, string? id, CancellationToken ct)
    {
        var path = Path(frame);
        await presence.CanWatchAsync(caller, path, ct);
        var name = id ?? Guid.NewGuid().ToString("N");
        var members = await presence.MembersAsync(path, ct);
        Hold(name, new Watch(this, name, path, Members(members), ct));
        return new JsonObject { ["subscription"] = name, ["members"] = Members(members) };
    }

    /// <summary>A presence watch: each change re-reads the members (every server's) and sends them if they differ.</summary>
    private sealed class Watch : IDisposable
    {
        private readonly CancellationTokenSource _stop;
        private readonly SemaphoreSlim _changed = new(0, 1);
        private readonly IDisposable _watch;

        public Watch(RealtimeConnection connection, string name, string path, JsonArray sent, CancellationToken ct)
        {
            _stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _watch = connection.Lists.Notifier.Watch(RealtimePresence.Key(path), _ => { if (_changed.CurrentCount == 0) try { _changed.Release(); } catch (SemaphoreFullException) { } });
            _ = RunAsync(connection, name, path, sent.ToJsonString(), _stop.Token);
        }

        private async Task RunAsync(RealtimeConnection connection, string name, string path, string last, CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await _changed.WaitAsync(ct);
                    var members = Members(await connection.Presence.MembersAsync(path, ct));
                    if (members.ToJsonString() == last) continue;
                    last = members.ToJsonString();
                    await connection.SendAsync(new JsonObject { ["type"] = "presence", ["subscription"] = name, ["path"] = path, ["members"] = members }, ct);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e) { connection.Log.LogWarning(e, "Realtime presence watch of {Path} stopped.", path); }
        }

        public void Dispose()
        {
            _watch.Dispose();
            _stop.Cancel();
            _stop.Dispose();
        }
    }

    private static JsonArray Members(IEnumerable<RealtimeMember> members) => new(members.Select(m => (JsonNode)new JsonObject
    {
        ["uid"] = m.Uid, ["data"] = m.Data?.DeepClone(), ["joinedAt"] = m.JoinedAt.ToString("O"),
    }).ToArray());

    private static string ChannelOf(JsonObject frame) =>
        frame["channel"]?.GetValueKind() == JsonValueKind.String ? (string)frame["channel"]! : throw new RealtimeException(WincheErrorCode.InvalidArgument, "Give the channel's name.");

    private static byte[] Payload(JsonObject frame)
    {
        try { return Convert.FromBase64String(frame["payload"]?.GetValueKind() == JsonValueKind.String ? (string)frame["payload"]! : ""); }
        catch (FormatException) { throw new RealtimeException(WincheErrorCode.InvalidArgument, "payload is base64."); }
    }

    private JsonObject Unsubscribe(JsonObject frame)
    {
        var name = frame["subscription"]?.GetValueKind() == JsonValueKind.String ? (string)frame["subscription"]! : "";
        lock (_subscriptions)
            if (_subscriptions.Remove(name, out var s)) s.Dispose();
        return new JsonObject();
    }

    /// <summary>
    /// A subscription: sends what's after its position, then waits to be told the list changed and sends again. Always
    /// read from the store, so a reader that falls behind catches up in order with no gaps.
    /// </summary>
    private sealed class Subscription : IDisposable
    {
        private readonly CancellationTokenSource _stop;
        private readonly SemaphoreSlim _changed = new(0, 1);
        private readonly IDisposable _watch;

        public Subscription(RealtimeConnection connection, string name, string path, long after, string? state, CancellationToken ct)
        {
            _stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _watch = connection.Lists.Notifier.Watch(path, _ => { if (_changed.CurrentCount == 0) try { _changed.Release(); } catch (SemaphoreFullException) { } });
            _ = RunAsync(connection, name, path, after, state, _stop.Token);
        }

        private async Task RunAsync(RealtimeConnection connection, string name, string path, long last, string? state, CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    RealtimePage page;
                    await connection.Producing.WaitAsync(ct);
                    try
                    {
                        // Room for a page first, then read it: a slow client holds its queue and one page, no more.
                        await connection.RoomAsync(connection.Options.FrameBytes, reserve: false, ct);
                        page = await connection.Lists.Store.ReadAsync(path, last, connection.Options.ItemsPerFrame, connection.Options.FrameBytes, ct);
                        if (page.Items.Count > 0)
                        {
                            last = page.Items[^1].Position;
                            await connection.SendAsync(new JsonObject { ["type"] = "items", ["subscription"] = name, ["path"] = path, ["items"] = Items(page.Items) }, ct);
                        }
                    }
                    finally { connection.Producing.Release(); }
                    if (page.More) continue;   // more to catch up
                    if (page.List is { } list && list.State != state)
                    {
                        state = list.State;
                        await connection.SendAsync(new JsonObject { ["type"] = "state", ["subscription"] = name, ["path"] = path, ["state"] = list.State, ["count"] = list.Count }, ct);
                    }
                    await _changed.WaitAsync(ct);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                connection.Log.LogWarning(e, "Realtime subscription to {Path} stopped.", path);
                try { await connection.SendAsync(new JsonObject { ["type"] = "error", ["subscription"] = name, ["error"] = JsonSerializer.SerializeToNode(new WincheError(WincheErrorCode.Unavailable, "The subscription stopped: subscribe again from your last position.")) }, CancellationToken.None); }
                catch { /* gone */ }
            }
        }

        public void Dispose()
        {
            _watch.Dispose();
            _stop.Cancel();
            _stop.Dispose();
        }
    }

    /// <summary>Queues a frame, waiting while the queue is full; closes the connection as too slow if it stays full.</summary>
    private async Task SendAsync(JsonObject frame, CancellationToken ct)
    {
        var bytes = Encoding.UTF8.GetBytes(frame.ToJsonString());
        await RoomAsync(bytes.Length, reserve: true, ct);
        if (!_out.Writer.TryWrite(bytes)) Sent(bytes.Length);   // closing: nothing goes out
    }

    /// <summary>Queues a frame if there's room now, else drops it (a channel message: live only).</summary>
    private void TryEnqueue(JsonObject frame)
    {
        var bytes = Encoding.UTF8.GetBytes(frame.ToJsonString());
        lock (_gate)
        {
            if (_queued > 0 && _queued + bytes.Length > options.SendQueueBytes) return;
            _queued += bytes.Length;
        }
        if (!_out.Writer.TryWrite(bytes)) Sent(bytes.Length);
    }

    /// <summary>
    /// Waits until <paramref name="bytes"/> more fit in the queue (always, when it's empty), and takes them if
    /// <paramref name="reserve"/>. A queue that stays full for <see cref="RealtimeSocketOptions.SlowClientTimeout"/> closes the connection.
    /// </summary>
    internal async Task RoomAsync(long bytes, bool reserve, CancellationToken ct)
    {
        var until = DateTimeOffset.UtcNow + options.SlowClientTimeout;
        while (true)
        {
            Task drained;
            lock (_gate)
            {
                if (_queued == 0 || _queued + bytes <= options.SendQueueBytes)
                {
                    if (reserve) _queued += bytes;
                    return;
                }
                drained = _drained.Task;
            }
            var left = until - DateTimeOffset.UtcNow;
            if (left <= TimeSpan.Zero)
            {
                Close(RealtimeSocketExtensions.SlowCloseCode, "too slow: reconnect and read on from your positions");
                throw new OperationCanceledException();
            }
            await Task.WhenAny(drained, Task.Delay(left, ct));
            ct.ThrowIfCancellationRequested();
        }
    }

    private void Sent(long bytes)
    {
        TaskCompletionSource drained;
        lock (_gate)
        {
            _queued -= bytes;
            drained = _drained;
            _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        drained.TrySetResult();
    }

    /// <summary>
    /// Closes with <paramref name="code"/>: the writer drops what's queued and sends the close; the client's answer
    /// ends the receive loop. Nothing is cancelled at once (cancelling a receive aborts a Kestrel socket, and the close
    /// would never go out); a client that doesn't answer within <see cref="RealtimeSocketOptions.CloseGrace"/> is cut off.
    /// </summary>
    private void Close(int code, string why)
    {
        lock (_gate)
        {
            if (_closeRequested) return;
            _closeRequested = true;
            _closeCode = (WebSocketCloseStatus)code;
            _closeWhy = why;
        }
        _out.Writer.TryComplete();
        try { _closing.CancelAfter(options.CloseGrace); } catch (ObjectDisposedException) { }
    }

    private async Task WriteAsync(CancellationToken ct)
    {
        await foreach (var bytes in _out.Reader.ReadAllAsync(ct))
        {
            if (!Volatile.Read(ref _closeRequested)) await socket.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
            Sent(bytes.Length);
        }
        if (Volatile.Read(ref _closeRequested) && socket.State == WebSocketState.Open)
            await socket.CloseOutputAsync(_closeCode, _closeWhy, ct);
    }

    private static string Path(JsonObject frame) =>
        frame["path"]?.GetValueKind() == JsonValueKind.String ? (string)frame["path"]! : throw new RealtimeException(WincheErrorCode.InvalidArgument, "Give the list's path.");

    private static long After(JsonObject frame) => frame["after"] is JsonValue v && v.TryGetValue<long>(out var after) ? Math.Max(0, after) : 0;

    private static JsonArray Items(IEnumerable<RealtimeItem> items) => new(items.Select(i => (JsonNode)new JsonObject
    {
        ["key"] = i.Key, ["position"] = i.Position, ["at"] = i.At.ToString("O"), ["by"] = i.By, ["payload"] = Convert.ToBase64String(i.Payload),
    }).ToArray());

    private static JsonObject List(RealtimeListInfo l) => new()
    {
        ["path"] = l.Path, ["state"] = l.State, ["count"] = l.Count, ["bytes"] = l.Bytes,
        ["openedAt"] = l.OpenedAt.ToString("O"), ["closedAt"] = l.ClosedAt?.ToString("O"),
    };

    private static JsonObject Error(string? id, RealtimeException e) => new()
    {
        ["type"] = "error", ["id"] = id, ["error"] = JsonSerializer.SerializeToNode(e.ToError()),
    };
}
