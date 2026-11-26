using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Winche.Realtime.AspNetCore;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>The second round's edges: malformed frames, limits at join, order of answers, lists closed twice or made again.</summary>
[Collection("postgres")]
public class HardeningTests(PostgresFixture pg)
{
    private const string Rules = """
        match sessions/{s} {
          allow get, create, update: if request.auth != null;
          match presence { allow get, create: if request.auth != null; }
        }
        """;

    private async Task<WebApplication> StartAsync(Action<RealtimeSocketOptions>? socket = null, Action<WincheRealtimeOptions>? realtime = null)
    {
        var connection = await pg.NewDatabaseAsync();
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddWincheRealtime(o => { o.ConnectionString = connection; o.UseRulesText(Rules); realtime?.Invoke(o); });
        builder.Services.AddWincheRealtimeWsApi(socket);
        builder.Services.AddWincheCaller(http => http.Request.Query["access_token"].ToString() is { Length: > 0 } uid
            ? new Dictionary<string, object?> { ["uid"] = uid, ["sub"] = uid } : null);
        var app = builder.Build();
        app.UseWebSockets();
        app.UseWincheCaller();
        app.MapWincheRealtimeWsApi();
        await app.StartAsync();
        return app;
    }

    private static Task<TestClient> ConnectAsync(WebApplication app, string uid) => TestClient.ConnectAsync(app.GetTestServer(), $"/realtime/v1/ws?access_token={uid}");

    private static JsonObject Append(string path, params string[] keys) => new()
    {
        ["type"] = "append", ["path"] = path,
        ["items"] = new JsonArray(keys.Select(k => (JsonNode)new JsonObject { ["key"] = k, ["payload"] = Convert.ToBase64String(Encoding.UTF8.GetBytes("p-" + k)) }).ToArray()),
    };

    [Fact]
    public async Task Malformed_frames_get_an_error_and_the_connection_carries_on()
    {
        await using var app = await StartAsync();
        await using var ann = await ConnectAsync(app, "ann");
        Assert.Equal("invalid-argument", (string?)(await ann.RequestAsync(new JsonObject { ["type"] = 1 }))["error"]!["code"]);
        Assert.Equal("invalid-argument", (string?)(await ann.RequestAsync(new JsonObject { ["type"] = "append", ["path"] = "sessions/s1", ["items"] = new JsonArray(1) }))["error"]!["code"]);
        Assert.Equal("invalid-argument", (string?)(await ann.RequestAsync(new JsonObject { ["type"] = "append", ["path"] = 5, ["items"] = new JsonArray() }))["error"]!["code"]);
        Assert.Equal("response", (string?)(await ann.RequestAsync(Append("sessions/s1", "ann:1")))["type"]);   // still connected
    }

    [Fact]
    public async Task A_subscription_answers_before_its_first_items()
    {
        await using var app = await StartAsync();
        await using var ann = await ConnectAsync(app, "ann");
        await ann.RequestAsync(Append("sessions/s1", "ann:1", "ann:2"));
        for (var n = 0; n < 20; n++)
        {
            var answer = await ann.RequestAsync(new JsonObject { ["type"] = "subscribe", ["path"] = "sessions/s1" });
            var name = (string)answer["result"]!["subscription"]!;
            await ann.WaitForAsync(f => (string?)f["type"] == "items" && (string?)f["subscription"] == name);
            var frames = ann.Received;
            Assert.True(frames.FindIndex(f => (string?)f["id"] == name) < frames.FindIndex(f => (string?)f["type"] == "items" && (string?)f["subscription"] == name));
        }
    }

    [Fact]
    public async Task Joins_are_limited_per_connection_and_an_on_disconnect_item_is_checked_when_joining()
    {
        await using var app = await StartAsync(s => s.MaxJoins = 2, r => r.MaxItemBytes = 10);
        await using var ann = await ConnectAsync(app, "ann");
        JsonObject Join(string path, string? key = null, int bytes = 1) => new()
        {
            ["type"] = "join", ["path"] = path,
            ["onDisconnect"] = key is null ? null : new JsonObject { ["key"] = key, ["payload"] = Convert.ToBase64String(new byte[bytes]) },
        };
        Assert.Equal("invalid-argument", (string?)(await ann.RequestAsync(Join("sessions/s1", "ann:left", bytes: 11)))["error"]!["code"]);   // past the item limit
        Assert.Equal("invalid-argument", (string?)(await ann.RequestAsync(Join("sessions/s1", new string('k', 201))))["error"]!["code"]);       // past the key limit
        Assert.Equal("response", (string?)(await ann.RequestAsync(Join("sessions/s1", "ann:left")))["type"]);
        Assert.Equal("response", (string?)(await ann.RequestAsync(Join("sessions/s2")))["type"]);
        Assert.Equal("response", (string?)(await ann.RequestAsync(Join("sessions/s1")))["type"]);   // joining again isn't another join
        Assert.Equal("resource-exhausted", (string?)(await ann.RequestAsync(Join("sessions/s3")))["error"]!["code"]);
        await ann.RequestAsync(new JsonObject { ["type"] = "leave", ["path"] = "sessions/s2" });
        Assert.Equal("response", (string?)(await ann.RequestAsync(Join("sessions/s3")))["type"]);
    }

    [Fact]
    public async Task A_subscriber_to_a_path_that_is_deleted_and_made_again_gets_the_new_lists_items()
    {
        await using var app = await StartAsync();
        await using var ann = await ConnectAsync(app, "ann");
        await ann.RequestAsync(Append("sessions/s1", "ann:1", "ann:2"));
        var name = (string)(await ann.RequestAsync(new JsonObject { ["type"] = "subscribe", ["path"] = "sessions/s1" }))["result"]!["subscription"]!;
        await ann.WaitForAsync(f => (string?)f["type"] == "items" && (string?)f["subscription"] == name);
        var store = app.Services.GetRequiredService<RealtimeStore>();
        await app.Services.GetRequiredService<RealtimeLists>().CloseAsync(new Dictionary<string, object?> { ["uid"] = "ann" }, "sessions/s1");
        Assert.True(await store.DeleteClosedAsync("sessions/s1"));
        await ann.WaitForAsync(f => (string?)f["type"] == "state" && (string?)f["subscription"] == name && (string?)f["state"] == "deleted");
        await ann.RequestAsync(Append("sessions/s1", "ann:new"));
        var fresh = await ann.WaitForAsync(f => (string?)f["type"] == "items" && (string?)f["subscription"] == name && f["items"]!.AsArray().Any(i => (string?)i!["key"] == "ann:new"));
        Assert.Equal(1L, (long)fresh["items"]![0]!["position"]!);
    }

    [Fact]
    public async Task Closing_a_list_that_is_already_closed_publishes_nothing_more()
    {
        var sink = new ChangesSink();
        var services = new ServiceCollection().AddLogging().AddSingleton<IWincheChangeSink>(sink);
        var connection = await pg.NewDatabaseAsync();
        services.AddWincheRealtime(o => { o.ConnectionString = connection; o.UseRulesText(Rules); });
        await using var sp = services.BuildServiceProvider();
        var lists = sp.GetRequiredService<RealtimeLists>();
        await lists.ServerAppendAsync("sessions/s1", [new("a:1", Encoding.UTF8.GetBytes("x"))]);
        var first = await lists.ServerCloseAsync("sessions/s1");
        var again = await lists.ServerCloseAsync("sessions/s1");
        Assert.Equal(first.ClosedAt, again.ClosedAt);
        Assert.Single(sink.Changes);
    }

    [Fact]
    public async Task With_archiving_on_and_no_archive_closed_lists_are_kept()
    {
        var options = new WincheRealtimeOptions { ConnectionString = await pg.NewDatabaseAsync(), ArchiveClosedLists = true };
        options.UseRulesText(Rules);
        var lists = RealtimeLists.Create(options);
        await lists.ServerAppendAsync("sessions/s1", [new("a:1", Encoding.UTF8.GetBytes("x"))]);
        await lists.Store.CloseAsync("sessions/s1");
        options.RetainClosedFor = TimeSpan.Zero;
        var sweep = (await new RealtimeSweeper(lists.Store, options, lists.Notifier).RunOnceAsync())!;
        Assert.Equal((0, 0, 1), (sweep.Archived, sweep.Deleted, sweep.Failed));
        Assert.NotNull(await lists.Store.InfoAsync("sessions/s1"));
    }

    [Fact]
    public async Task An_oversized_frame_closes_the_connection_even_if_the_client_never_answers()
    {
        await using var app = await StartAsync();
        var socket = new SilentSocket(Encoding.UTF8.GetBytes(new string('x', 2_000)));
        var connection = new RealtimeConnection(socket, null, app.Services.GetRequiredService<RealtimeLists>(),
            app.Services.GetRequiredService<RealtimeChannels>(), app.Services.GetRequiredService<RealtimePresence>(),
            new RealtimeSocketOptions { MaxFrameBytes = 1_000, CloseGrace = TimeSpan.FromMilliseconds(500) }, NullLogger.Instance);
        await connection.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(WebSocketCloseStatus.MessageTooBig, socket.ClosedWith);
    }


    [Fact]
    public async Task A_join_that_was_saved_but_not_announced_still_counts_towards_the_limit()
    {
        await using var app = await StartAsync(s => s.MaxJoins = 1);
        await using var ann = await ConnectAsync(app, "ann");
        var presence = app.Services.GetRequiredService<RealtimePresence>();
        presence.AnnounceFault = 1;   // the join is saved; telling the watchers fails
        Assert.NotEqual("response", (string?)(await ann.RequestAsync(new JsonObject { ["type"] = "join", ["path"] = "sessions/s1" }))["type"]);
        Assert.Single(await presence.MembersAsync("sessions/s1"));   // saved all the same
        Assert.Equal("resource-exhausted", (string?)(await ann.RequestAsync(new JsonObject { ["type"] = "join", ["path"] = "sessions/s2" }))["error"]!["code"]);
    }

    private sealed class ChangesSink : IWincheChangeSink
    {
        public readonly List<WincheChange> Changes = [];
        public Task OnChangesAsync(IReadOnlyList<WincheChange> changes, CancellationToken ct) { lock (Changes) Changes.AddRange(changes); return Task.CompletedTask; }
    }

    /// <summary>A client that sends one frame in pieces (never its end) and never answers a close.</summary>
    private sealed class SilentSocket(byte[] piece) : WebSocket
    {
        private int _sent;
        public WebSocketCloseStatus? ClosedWith;
        public override WebSocketCloseStatus? CloseStatus => ClosedWith;
        public override string? CloseStatusDescription => null;
        public override WebSocketState State => ClosedWith is null ? WebSocketState.Open : WebSocketState.CloseSent;
        public override string? SubProtocol => null;
        public override void Abort() { }
        public override async Task CloseAsync(WebSocketCloseStatus status, string? description, CancellationToken ct)
        {
            ClosedWith = status;
            await Task.Delay(Timeout.Infinite, ct);   // the client never answers
        }
        public override Task CloseOutputAsync(WebSocketCloseStatus status, string? description, CancellationToken ct) { ClosedWith = status; return Task.CompletedTask; }
        public override void Dispose() { }
        public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken ct)
        {
            if (_sent++ > 0) await Task.Delay(Timeout.Infinite, ct);
            piece.CopyTo(buffer.Array!, buffer.Offset);
            return new WebSocketReceiveResult(piece.Length, WebSocketMessageType.Text, endOfMessage: false);
        }
        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType type, bool end, CancellationToken ct) => Task.CompletedTask;
    }
}
