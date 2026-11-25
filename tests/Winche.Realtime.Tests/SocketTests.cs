using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Winche.Realtime.AspNetCore;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>The socket: append, subscribe from a position (catching up after a drop), read pages, close.</summary>
[Collection("postgres")]
public class SocketTests(PostgresFixture pg)
{
    private const string Rules = """
        match sessions/{session} {
          allow get: if request.auth != null;
          allow create: if request.auth != null && request.resource.device == request.auth.uid;
          allow update: if request.auth.token.host == true;
        }
        """;

    /// <summary>A host with Realtime at /realtime/v1/ws; ?access_token=uid[,host][,exp=unix] signs in (none: signed out).</summary>
    private async Task<WebApplication> StartAsync(string? database = null, bool kestrel = false)
    {
        var connection = database ?? await pg.NewDatabaseAsync();
        var builder = WebApplication.CreateBuilder();
        if (kestrel) builder.WebHost.UseUrls("http://127.0.0.1:0");   // a real server, on a free port
        else builder.WebHost.UseTestServer();
        builder.Services.AddWincheRealtime(o => { o.ConnectionString = connection; o.UseRulesText(Rules); });
        builder.Services.AddWincheRealtimeWsApi();
        builder.Services.AddWincheCaller(http =>
        {
            if (http.Request.Query["access_token"].ToString() is not { Length: > 0 } token) return null;
            var parts = token.Split(',');
            var claims = new Dictionary<string, object?> { ["uid"] = parts[0], ["sub"] = parts[0], ["host"] = parts.Contains("host") };
            if (parts.FirstOrDefault(p => p.StartsWith("exp=")) is { } exp) claims["exp"] = long.Parse(exp[4..]);
            return claims;
        });
        var app = builder.Build();
        app.UseWebSockets();
        app.UseWincheCaller();
        app.MapWincheRealtimeWsApi();
        await app.StartAsync();
        return app;
    }

    private static async Task<Client> ConnectAsync(WebApplication app, string? token)
    {
        var server = app.GetTestServer();
        var socket = await server.CreateWebSocketClient().ConnectAsync(
            new Uri(server.BaseAddress, "/realtime/v1/ws" + (token is null ? "" : $"?access_token={Uri.EscapeDataString(token)}")), CancellationToken.None);
        var client = new Client(socket);
        await client.WaitForAsync(f => (string?)f["type"] == "welcome");
        return client;
    }

    private static JsonObject Append(string path, params string[] keys) => new()
    {
        ["type"] = "append", ["path"] = path,
        ["items"] = new JsonArray(keys.Select(k => (JsonNode)new JsonObject { ["key"] = k, ["payload"] = Convert.ToBase64String(Encoding.UTF8.GetBytes("p-" + k)) }).ToArray()),
    };

    [Fact]
    public async Task Subscribers_get_every_item_in_order_live_and_after_a_drop()
    {
        await using var app = await StartAsync();
        await using var ann = await ConnectAsync(app, "ann");
        await using var bob = await ConnectAsync(app, "bob");

        var first = await ann.RequestAsync(Append("sessions/s1", "ann:1", "ann:2"));
        Assert.Equal([1L, 2L], first["result"]!["positions"]!.AsArray().Select(p => (long)p!));

        // Bob subscribes from the start: what's there, then what comes.
        var sub = await bob.RequestAsync(new JsonObject { ["type"] = "subscribe", ["path"] = "sessions/s1" });
        var subscription = (string)sub["result"]!["subscription"]!;
        Assert.Equal(2L, (long)sub["result"]!["list"]!["count"]!);
        await ann.RequestAsync(Append("sessions/s1", "ann:3"));
        var seen = await bob.ItemsAsync(subscription, upTo: 3);
        Assert.Equal([1L, 2L, 3L], seen.Select(i => (long)i["position"]!));
        Assert.Equal("p-ann:3", Encoding.UTF8.GetString(Convert.FromBase64String((string)seen[2]["payload"]!)));
        Assert.Equal("ann", (string?)seen[0]["by"]);

        // Bob drops; Ann goes on; Bob comes back from position 3 and gets only what he missed.
        await bob.DisposeAsync();
        await ann.RequestAsync(Append("sessions/s1", "ann:4", "ann:5"));
        await using var back = await ConnectAsync(app, "bob");
        var again = (string)(await back.RequestAsync(new JsonObject { ["type"] = "subscribe", ["path"] = "sessions/s1", ["after"] = 3 }))["result"]!["subscription"]!;
        Assert.Equal([4L, 5L], (await back.ItemsAsync(again, upTo: 5)).Select(i => (long)i["position"]!));
    }

    [Fact]
    public async Task Reads_page_through_a_list_and_closing_tells_subscribers()
    {
        await using var app = await StartAsync();
        await using var ann = await ConnectAsync(app, "ann");
        await using var host = await ConnectAsync(app, "host,host");
        await ann.RequestAsync(Append("sessions/s1", "ann:1", "ann:2", "ann:3"));
        var page = (await ann.RequestAsync(new JsonObject { ["type"] = "read", ["path"] = "sessions/s1", ["after"] = 1, ["limit"] = 1 }))["result"]!;
        Assert.Equal([2L], page["items"]!.AsArray().Select(i => (long)i!["position"]!));

        var subscription = (string)(await ann.RequestAsync(new JsonObject { ["type"] = "subscribe", ["path"] = "sessions/s1", ["after"] = 3 }))["result"]!["subscription"]!;
        Assert.Equal("closed", (string?)(await host.RequestAsync(new JsonObject { ["type"] = "close", ["path"] = "sessions/s1" }))["result"]!["list"]!["state"]);
        var state = await ann.WaitForAsync(f => (string?)f["type"] == "state" && (string?)f["subscription"] == subscription);
        Assert.Equal("closed", (string?)state["state"]);
        var refused = await ann.RequestAsync(Append("sessions/s1", "ann:4"));
        Assert.Equal("failed-precondition", (string?)refused["error"]!["code"]);
    }

    [Fact]
    public async Task Refusals_are_errors_for_the_request_and_the_socket_carries_on()
    {
        await using var app = await StartAsync();
        await using var bob = await ConnectAsync(app, "bob");
        var asAnn = await bob.RequestAsync(Append("sessions/s1", "ann:1"));
        Assert.Equal(("error", "permission-denied", "denied"), ((string?)asAnn["type"], (string?)asAnn["error"]!["code"], (string?)asAnn["error"]!["reason"]));
        var badPayload = await bob.RequestAsync(new JsonObject { ["type"] = "append", ["path"] = "sessions/s1", ["items"] = new JsonArray(new JsonObject { ["key"] = "bob:1", ["payload"] = "not base64!" }) });
        Assert.Equal("invalid-argument", (string?)badPayload["error"]!["code"]);
        Assert.Equal("invalid-argument", (string?)(await bob.RequestAsync(new JsonObject { ["type"] = "nonsense" }))["error"]!["code"]);
        Assert.Equal("response", (string?)(await bob.RequestAsync(new JsonObject { ["type"] = "ping" }))["type"]);

        await using var nobody = await ConnectAsync(app, null);
        Assert.Equal("permission-denied", (string?)(await nobody.RequestAsync(new JsonObject { ["type"] = "subscribe", ["path"] = "sessions/s1" }))["error"]!["code"]);
    }

    [Fact]
    public async Task Followers_on_another_server_get_items_appended_on_this_one_once()
    {
        var database = await pg.NewDatabaseAsync();
        await using var one = await StartAsync(database);
        await using var two = await StartAsync(database);
        await using var ann = await ConnectAsync(one, "ann");
        await using var bob = await ConnectAsync(two, "bob");
        var subscription = (string)(await bob.RequestAsync(new JsonObject { ["type"] = "subscribe", ["path"] = "sessions/s1" }))["result"]!["subscription"]!;
        await Task.Delay(300);   // the second server's relay listening
        await ann.RequestAsync(Append("sessions/s1", "ann:1"));
        await ann.RequestAsync(Append("sessions/s1", "ann:2", "ann:3"));
        var seen = await bob.ItemsAsync(subscription, upTo: 3);
        Assert.Equal([1L, 2L, 3L], seen.Select(i => (long)i["position"]!));

        // Closing on one server reaches followers on the other.
        await using var host = await ConnectAsync(one, "host,host");
        await host.RequestAsync(new JsonObject { ["type"] = "close", ["path"] = "sessions/s1" });
        await bob.WaitForAsync(f => (string?)f["type"] == "state" && (string?)f["subscription"] == subscription && (string?)f["state"] == "closed");
    }

    [Fact]
    public async Task The_socket_closes_when_the_sign_in_expires()
    {
        await using var app = await StartAsync();
        var soon = DateTimeOffset.UtcNow.AddSeconds(2).ToUnixTimeSeconds();
        await using var ann = await ConnectAsync(app, $"ann,exp={soon}");
        await ann.WaitForCloseAsync(TimeSpan.FromSeconds(10));
        Assert.Equal((WebSocketCloseStatus)4401, ann.CloseStatus);
    }

    [Theory]
    [InlineData(60 * 24 * 3600L)]            // 60 days: past what one .NET timer can wait
    [InlineData(long.MaxValue / 2)]          // past any date
    public async Task A_sign_in_that_lasts_long_still_connects_and_works(long seconds)
    {
        await using var app = await StartAsync();
        var exp = seconds > 1L << 40 ? seconds : DateTimeOffset.UtcNow.ToUnixTimeSeconds() + seconds;
        await using var ann = await ConnectAsync(app, $"ann,exp={exp}");
        var appended = await ann.RequestAsync(Append("sessions/s1", "ann:1"));
        Assert.Equal(1L, (long)appended["result"]!["positions"]![0]!);
    }

    [Fact]
    public async Task On_a_real_server_an_expired_sign_in_closes_with_4401()
    {
        await using var app = await StartAsync(kestrel: true);
        var soon = DateTimeOffset.UtcNow.AddSeconds(2).ToUnixTimeSeconds();
        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(new Uri(app.Urls.First().Replace("http", "ws") + $"/realtime/v1/ws?access_token=ann,exp={soon}"), CancellationToken.None);
        var buffer = new byte[1 << 16];
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        WebSocketReceiveResult r;
        do r = await socket.ReceiveAsync(buffer, timeout.Token);   // the welcome, then the close
        while (r.MessageType != WebSocketMessageType.Close);
        Assert.Equal((WebSocketCloseStatus)RealtimeSocketExtensions.ExpiredCloseCode, socket.CloseStatus);
    }

    /// <summary>A frame-level client: send JSON, wait for frames.</summary>
    private sealed class Client : IAsyncDisposable
    {
        private readonly WebSocket _socket;
        private readonly List<JsonObject> _inbox = [];
        private readonly SemaphoreSlim _signal = new(0);
        private readonly Task _receiving;
        private int _next;
        public WebSocketCloseStatus? CloseStatus { get; private set; }

        public Client(WebSocket socket) { _socket = socket; _receiving = Task.Run(ReceiveAsync); }

        public async Task<JsonObject> RequestAsync(JsonObject frame)
        {
            var id = $"r{Interlocked.Increment(ref _next)}";
            frame["id"] = id;
            await _socket.SendAsync(Encoding.UTF8.GetBytes(frame.ToJsonString()), WebSocketMessageType.Text, true, CancellationToken.None);
            return await WaitForAsync(f => (string?)f["id"] == id && (string?)f["type"] is "response" or "error");
        }

        /// <summary>The items of a subscription's item frames, until one at position <paramref name="upTo"/> arrives.</summary>
        public async Task<List<JsonObject>> ItemsAsync(string subscription, long upTo)
        {
            var items = new List<JsonObject>();
            while (items.Count == 0 || (long)items[^1]["position"]! < upTo)
            {
                var frame = await WaitForAsync(f => (string?)f["type"] == "items" && (string?)f["subscription"] == subscription);
                items.AddRange(frame["items"]!.AsArray().Select(i => i!.AsObject()));
            }
            return items;
        }

        public async Task<JsonObject> WaitForAsync(Func<JsonObject, bool> match)
        {
            var until = DateTime.UtcNow.AddSeconds(10);
            while (true)
            {
                lock (_inbox)
                {
                    var hit = _inbox.FirstOrDefault(match);
                    if (hit is not null) { _inbox.Remove(hit); return hit; }
                }
                var left = until - DateTime.UtcNow;
                Assert.True(left > TimeSpan.Zero, "timed out waiting for a frame");
                await _signal.WaitAsync(left);
            }
        }

        public Task WaitForCloseAsync(TimeSpan within) => _receiving.WaitAsync(within);

        private async Task ReceiveAsync()
        {
            var buffer = new byte[1 << 20];
            try
            {
                while (true)
                {
                    using var message = new MemoryStream();
                    WebSocketReceiveResult r;
                    do
                    {
                        r = await _socket.ReceiveAsync(buffer, CancellationToken.None);
                        if (r.MessageType == WebSocketMessageType.Close) { CloseStatus = r.CloseStatus; return; }
                        message.Write(buffer, 0, r.Count);
                    } while (!r.EndOfMessage);
                    lock (_inbox) _inbox.Add(JsonNode.Parse(message.ToArray())!.AsObject());
                    _signal.Release();
                }
            }
            catch (Exception) { /* closed */ }
        }

        public async ValueTask DisposeAsync()
        {
            try { if (_socket.State == WebSocketState.Open) await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None); }
            catch (Exception) { /* already gone */ }
            _socket.Dispose();
        }
    }
}
