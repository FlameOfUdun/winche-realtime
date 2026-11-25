using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Winche.Realtime.AspNetCore;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>Channels (live messages, never stored) and presence (who's here, and what happens when they drop).</summary>
[Collection("postgres")]
public class ChannelsPresenceTests(PostgresFixture pg)
{
    private const string Rules = """
        match sessions/{session} {
          allow get: if request.auth != null;
          allow create: if request.auth != null && request.resource.device == request.auth.uid;
          match channels/{channel} {
            allow get: if request.auth != null;
            allow create: if request.auth != null && channel != 'admin';
          }
          match presence {
            allow get, create: if request.auth != null;
          }
        }
        """;

    private async Task<WebApplication> StartAsync(string? database = null, Action<WincheRealtimeOptions>? configure = null)
    {
        var connection = database ?? await pg.NewDatabaseAsync();
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddWincheRealtime(o => { o.ConnectionString = connection; o.UseRulesText(Rules); configure?.Invoke(o); });
        builder.Services.AddWincheRealtimeWsApi();
        builder.Services.AddWincheCaller(http =>
            http.Request.Query["access_token"].ToString() is { Length: > 0 } uid ? new Dictionary<string, object?> { ["uid"] = uid, ["sub"] = uid } : null);
        var app = builder.Build();
        app.UseWebSockets();
        app.UseWincheCaller();
        app.MapWincheRealtimeWsApi();
        await app.StartAsync();
        return app;
    }

    private static Task<TestClient> ConnectAsync(WebApplication app, string uid) => TestClient.ConnectAsync(app.GetTestServer(), $"/realtime/v1/ws?access_token={uid}");

    private static string B64(string text) => Convert.ToBase64String(Encoding.UTF8.GetBytes(text));
    private static string Text(JsonNode? b64) => Encoding.UTF8.GetString(Convert.FromBase64String((string)b64!));

    [Fact]
    public async Task Messages_reach_every_listener_once_and_are_never_kept()
    {
        await using var app = await StartAsync();
        await using var ann = await ConnectAsync(app, "ann");
        await using var bob = await ConnectAsync(app, "bob");
        var listening = (string)(await bob.RequestAsync(new() { ["type"] = "listen", ["path"] = "sessions/s1", ["channel"] = "telemetry" }))["result"]!["subscription"]!;
        var mine = (string)(await ann.RequestAsync(new() { ["type"] = "listen", ["path"] = "sessions/s1", ["channel"] = "telemetry" }))["result"]!["subscription"]!;

        Assert.Equal("response", (string?)(await ann.RequestAsync(new() { ["type"] = "publish", ["path"] = "sessions/s1", ["channel"] = "telemetry", ["payload"] = B64("hr=72") }))["type"]);
        var heard = await bob.WaitForAsync(f => (string?)f["type"] == "message" && (string?)f["subscription"] == listening);
        Assert.Equal(("hr=72", "ann", "telemetry"), (Text(heard["payload"]), (string?)heard["by"], (string?)heard["channel"]));
        await ann.WaitForAsync(f => (string?)f["type"] == "message" && (string?)f["subscription"] == mine);   // the publisher listens too
        await bob.AssertSilenceAsync(f => (string?)f["type"] == "message", TimeSpan.FromMilliseconds(500));   // once

        // Nothing kept: the list at the path is empty; a later listener hears nothing of it.
        Assert.Null((await bob.RequestAsync(new() { ["type"] = "read", ["path"] = "sessions/s1" }))["result"]!["list"]);
        // Rules and limits.
        Assert.Equal("permission-denied", (string?)(await ann.RequestAsync(new() { ["type"] = "publish", ["path"] = "sessions/s1", ["channel"] = "admin", ["payload"] = B64("x") }))["error"]!["code"]);
        Assert.Equal("invalid-argument", (string?)(await ann.RequestAsync(new() { ["type"] = "publish", ["path"] = "sessions/s1", ["channel"] = "telemetry", ["payload"] = B64(new string('x', 5000)) }))["error"]!["code"]);
        Assert.Equal("invalid-argument", (string?)(await ann.RequestAsync(new() { ["type"] = "publish", ["path"] = "sessions/s1", ["channel"] = "no/slash", ["payload"] = B64("x") }))["error"]!["code"]);
    }

    [Fact]
    public async Task Messages_cross_servers_once()
    {
        var database = await pg.NewDatabaseAsync();
        await using var one = await StartAsync(database);
        await using var two = await StartAsync(database);
        await using var ann = await ConnectAsync(one, "ann");
        await using var bob = await ConnectAsync(two, "bob");
        var listening = (string)(await bob.RequestAsync(new() { ["type"] = "listen", ["path"] = "sessions/s1", ["channel"] = "telemetry" }))["result"]!["subscription"]!;
        await Task.Delay(300);
        await ann.RequestAsync(new() { ["type"] = "publish", ["path"] = "sessions/s1", ["channel"] = "telemetry", ["payload"] = B64("1") });
        await ann.RequestAsync(new() { ["type"] = "publish", ["path"] = "sessions/s1", ["channel"] = "telemetry", ["payload"] = B64("2") });
        Assert.Equal("1", Text((await bob.WaitForAsync(f => (string?)f["type"] == "message" && (string?)f["subscription"] == listening))["payload"]));
        Assert.Equal("2", Text((await bob.WaitForAsync(f => (string?)f["type"] == "message" && (string?)f["subscription"] == listening))["payload"]));
        await bob.AssertSilenceAsync(f => (string?)f["type"] == "message", TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task A_full_size_message_of_any_bytes_crosses_servers()
    {
        var database = await pg.NewDatabaseAsync();
        await using var one = await StartAsync(database);
        await using var two = await StartAsync(database);
        await using var ann = await ConnectAsync(one, "ann");
        await using var bob = await ConnectAsync(two, "bob");
        var listening = (string)(await bob.RequestAsync(new() { ["type"] = "listen", ["path"] = "sessions/s1", ["channel"] = "telemetry" }))["result"]!["subscription"]!;
        await Task.Delay(300);
        var payload = Convert.ToBase64String(Enumerable.Repeat(new byte[] { 0xFB, 0xEF, 0xBE }, 1365).SelectMany(b => b).ToArray());   // 4,095 bytes: base64 all '+'
        var sent = await ann.RequestAsync(new() { ["type"] = "publish", ["path"] = "sessions/s1", ["channel"] = "telemetry", ["payload"] = payload });
        Assert.Equal("response", (string?)sent["type"]);
        Assert.Equal(payload, (string?)(await bob.WaitForAsync(f => (string?)f["type"] == "message" && (string?)f["subscription"] == listening))["payload"]);
    }

    [Fact]
    public async Task A_message_too_big_to_cross_servers_is_refused_and_delivered_nowhere()
    {
        await using var one = await StartAsync(configure: o => o.MaxMessageBytes = 64 * 1024);   // set past what fits
        await using var ann = await ConnectAsync(one, "ann");
        await using var cat = await ConnectAsync(one, "cat");
        await cat.RequestAsync(new() { ["type"] = "listen", ["path"] = "sessions/s1", ["channel"] = "telemetry" });
        var sent = await ann.RequestAsync(new() { ["type"] = "publish", ["path"] = "sessions/s1", ["channel"] = "telemetry", ["payload"] = Convert.ToBase64String(new byte[7000]) });
        Assert.Equal("invalid-argument", (string?)sent["error"]!["code"]);
        await cat.AssertSilenceAsync(f => (string?)f["type"] == "message", TimeSpan.FromMilliseconds(500));
    }

    [Theory]
    [InlineData("sessions/s1/channels/telemetry")]
    [InlineData("sessions/s1/presence")]
    [InlineData("channels/x")]
    [InlineData("presence")]
    public async Task No_path_uses_the_words_channels_or_presence(string path)
    {
        await using var app = await StartAsync();
        await using var ann = await ConnectAsync(app, "ann");
        foreach (var frame in new JsonObject[]
        {
            new() { ["type"] = "append", ["path"] = path, ["items"] = new JsonArray(new JsonObject { ["key"] = "ann:1", ["payload"] = B64("x") }) },
            new() { ["type"] = "publish", ["path"] = path, ["channel"] = "telemetry", ["payload"] = B64("x") },
            new() { ["type"] = "join", ["path"] = path },
        })
            Assert.Equal("invalid-argument", (string?)(await ann.RequestAsync(frame))["error"]?["code"]);
    }

    [Fact]
    public async Task Watchers_see_who_joins_and_leaves_and_a_drop_runs_its_on_disconnect()
    {
        await using var app = await StartAsync();
        await using var host = await ConnectAsync(app, "host");
        var watching = (string)(await host.RequestAsync(new() { ["type"] = "watch", ["path"] = "sessions/s1" }))["result"]!["subscription"]!;

        var ann = await ConnectAsync(app, "ann");
        await ann.RequestAsync(new()
        {
            ["type"] = "join", ["path"] = "sessions/s1", ["data"] = new JsonObject { ["role"] = "nurse" },
            ["onDisconnect"] = new JsonObject { ["key"] = "ann:dropped", ["payload"] = B64("{\"event\":\"dropped\"}") },
        });
        var joined = await host.WaitForAsync(f => (string?)f["type"] == "presence" && (string?)f["subscription"] == watching && f["members"]!.AsArray().Count == 1);
        Assert.Equal(("ann", "nurse"), ((string?)joined["members"]![0]!["uid"], (string?)joined["members"]![0]!["data"]!["role"]));

        // Ann's connection drops: she's gone, and her on-disconnect item is in the session's list.
        await ann.AbortAsync();
        await host.WaitForAsync(f => (string?)f["type"] == "presence" && (string?)f["subscription"] == watching && f["members"]!.AsArray().Count == 0);
        var list = (await host.RequestAsync(new() { ["type"] = "read", ["path"] = "sessions/s1" }))["result"]!;
        Assert.Equal("ann:dropped", (string?)list["items"]![0]!["key"]);
        Assert.Equal("ann", (string?)list["items"]![0]!["by"]);

        // Leaving on purpose: gone, nothing appended.
        await using var bob = await ConnectAsync(app, "bob");
        await bob.RequestAsync(new() { ["type"] = "join", ["path"] = "sessions/s1", ["onDisconnect"] = new JsonObject { ["key"] = "bob:dropped", ["payload"] = B64("x") } });
        await host.WaitForAsync(f => (string?)f["type"] == "presence" && f["members"]!.AsArray().Count == 1);
        await bob.RequestAsync(new() { ["type"] = "leave", ["path"] = "sessions/s1" });
        await host.WaitForAsync(f => (string?)f["type"] == "presence" && f["members"]!.AsArray().Count == 0);
        Assert.Single((await host.RequestAsync(new() { ["type"] = "read", ["path"] = "sessions/s1" }))["result"]!["items"]!.AsArray());

        // An on-disconnect item the rules wouldn't let them append is refused when they join, not when they drop.
        Assert.Equal("permission-denied", (string?)(await bob.RequestAsync(new()
        {
            ["type"] = "join", ["path"] = "sessions/s1", ["onDisconnect"] = new JsonObject { ["key"] = "ann:fake", ["payload"] = B64("x") },
        }))["error"]!["code"]);
    }

    [Fact]
    public async Task Presence_is_the_same_on_every_server_and_a_dead_servers_members_are_swept()
    {
        var database = await pg.NewDatabaseAsync();
        await using var one = await StartAsync(database);
        await using var two = await StartAsync(database);
        await using var watcher = await ConnectAsync(two, "host");
        var watching = (string)(await watcher.RequestAsync(new() { ["type"] = "watch", ["path"] = "sessions/s1" }))["result"]!["subscription"]!;
        await Task.Delay(300);
        await using var ann = await ConnectAsync(one, "ann");
        await ann.RequestAsync(new() { ["type"] = "join", ["path"] = "sessions/s1" });
        await watcher.WaitForAsync(f => (string?)f["type"] == "presence" && (string?)f["subscription"] == watching && f["members"]!.AsArray().Count == 1);

        // A server that stopped checking in (crashed): the sweep removes its members and runs their on-disconnect.
        var presence = two.Services.GetRequiredService<RealtimePresence>();
        await presence.AddForTestsAsync("sessions/s1", "ghost", serverId: "dead-server", seenAt: DateTimeOffset.UtcNow.AddMinutes(-5),
            onDisconnect: new RealtimeItemIn("ghost:dropped", Encoding.UTF8.GetBytes("x")));
        await watcher.WaitForAsync(f => (string?)f["type"] == "presence" && f["members"]!.AsArray().Count == 2);
        await two.Services.GetRequiredService<RealtimeSweeper>().RunOnceAsync();
        var after = await watcher.WaitForAsync(f => (string?)f["type"] == "presence" && f["members"]!.AsArray().Count == 1);
        Assert.Equal("ann", (string?)after["members"]![0]!["uid"]);
        Assert.Equal("ghost:dropped", (string?)(await watcher.RequestAsync(new() { ["type"] = "read", ["path"] = "sessions/s1" }))["result"]!["items"]![0]!["key"]);
    }
}
