using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Winche.Realtime.AspNetCore;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>A client that stops reading can't make the server hold more than its queue: it's closed as too slow.</summary>
[Collection("postgres")]
public class BackpressureTests(PostgresFixture pg)
{
    [Fact]
    public async Task Reads_and_subscription_pages_stop_at_a_byte_budget()
    {
        var store = await Store();
        await Fill(store, "sessions/big", 10);
        var page = await store.ReadAsync("sessions/big", 0, 1000, maxBytes: 200_000);
        Assert.Equal(4, page.Items.Count);   // 0, 60, 120 and 180 KB before each: the fourth starts under the budget
        Assert.True(page.More);
        Assert.False((await store.ReadAsync("sessions/big", 0, 1000)).More);
        Assert.Single((await store.ReadAsync("sessions/big", 0, 1000, maxBytes: 1)).Items);   // always at least one
    }

    [Fact]
    public async Task A_client_that_stops_reading_is_held_to_its_queue_then_closed_as_too_slow()
    {
        await using var services = Microsoft.Extensions.DependencyInjection.LoggingServiceCollectionExtensions.AddLogging(new Microsoft.Extensions.DependencyInjection.ServiceCollection())
            .AddWincheRealtime(o => { o.ConnectionString = pg.NewDatabaseAsync().GetAwaiter().GetResult(); o.UseRulesText("match sessions/{s} { allow get: if request.auth != null; }"); })
            .BuildServiceProvider();
        T Get<T>() where T : notnull => Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService<T>(services);
        var store = Get<RealtimeStore>();
        await store.EnsureAsync();
        await Fill(store, "sessions/big", 60);   // 3.6 MB
        var socket = new StuckSocket();
        for (var n = 0; n < 50; n++)
            socket.Incoming.Writer.TryWrite(new JsonObject { ["type"] = "subscribe", ["id"] = $"s{n}", ["path"] = "sessions/big" }.ToJsonString());
        var connection = new RealtimeConnection(socket, new Dictionary<string, object?> { ["uid"] = "ann" }, Get<RealtimeLists>(),
            Get<RealtimeChannels>(), Get<RealtimePresence>(),
            new RealtimeSocketOptions { SendQueueBytes = 1_000_000, FrameBytes = 200_000, SlowClientTimeout = TimeSpan.FromSeconds(1), CloseGrace = TimeSpan.FromSeconds(1) },
            NullLogger.Instance);

        var running = connection.RunAsync(CancellationToken.None);
        long most = 0;
        while (!running.IsCompleted && most < 50_000_000)
        {
            most = Math.Max(most, connection.QueuedBytes);
            await Task.WhenAny(running, Task.Delay(5));
        }
        await running.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.InRange(most, 1, 1_000_000 + 400_000);   // the queue, and at most one page over it
        Assert.Equal((WebSocketCloseStatus)RealtimeSocketExtensions.SlowCloseCode, socket.ClosedWith);
    }

    private async Task<RealtimeStore> Store()
    {
        var store = new RealtimeStore(new WincheRealtimeOptions { ConnectionString = await pg.NewDatabaseAsync() });
        await store.EnsureAsync();
        return store;
    }

    private static async Task Fill(RealtimeStore store, string path, int items)
    {
        for (var n = 0; n < items; n++)
            await store.AppendAsync(path, [new RealtimeItemIn($"k{n}", new byte[60 * 1024])], "ann");
    }

    /// <summary>A socket whose client sends the frames given and never reads: every send waits forever.</summary>
    private sealed class StuckSocket : WebSocket
    {
        public readonly Channel<string> Incoming = Channel.CreateUnbounded<string>();
        public WebSocketCloseStatus? ClosedWith;
        public override WebSocketCloseStatus? CloseStatus => ClosedWith;
        public override string? CloseStatusDescription => null;
        public override WebSocketState State => ClosedWith is null ? WebSocketState.Open : WebSocketState.Closed;
        public override string? SubProtocol => null;
        public override void Abort() { }
        public override Task CloseAsync(WebSocketCloseStatus status, string? description, CancellationToken ct) { ClosedWith = status; return Task.CompletedTask; }
        public override Task CloseOutputAsync(WebSocketCloseStatus status, string? description, CancellationToken ct) => CloseAsync(status, description, ct);
        public override void Dispose() { }

        public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken ct)
        {
            var text = Encoding.UTF8.GetBytes(await Incoming.Reader.ReadAsync(ct));
            text.CopyTo(buffer.Array!, buffer.Offset);
            return new WebSocketReceiveResult(text.Length, WebSocketMessageType.Text, true);
        }

        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType type, bool end, CancellationToken ct) => Task.Delay(Timeout.Infinite, ct);
    }
}
