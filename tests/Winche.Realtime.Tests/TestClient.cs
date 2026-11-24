using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.TestHost;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>A frame-level socket client: send JSON, wait for frames, drop without a goodbye.</summary>
public sealed class TestClient : IAsyncDisposable
{
    private readonly WebSocket _socket;
    private readonly List<JsonObject> _inbox = [];
    private readonly SemaphoreSlim _signal = new(0);
    private readonly Task _receiving;
    private int _next;
    public WebSocketCloseStatus? CloseStatus { get; private set; }

    private TestClient(WebSocket socket) { _socket = socket; _receiving = Task.Run(ReceiveAsync); }

    public static async Task<TestClient> ConnectAsync(TestServer server, string path)
    {
        var socket = await server.CreateWebSocketClient().ConnectAsync(new Uri(server.BaseAddress, path), CancellationToken.None);
        var client = new TestClient(socket);
        await client.WaitForAsync(f => (string?)f["type"] == "welcome");
        return client;
    }

    public async Task<JsonObject> RequestAsync(JsonObject frame)
    {
        var id = $"r{Interlocked.Increment(ref _next)}";
        frame["id"] = id;
        await _socket.SendAsync(Encoding.UTF8.GetBytes(frame.ToJsonString()), WebSocketMessageType.Text, true, CancellationToken.None);
        return await WaitForAsync(f => (string?)f["id"] == id && (string?)f["type"] is "response" or "error");
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

    /// <summary>No frame matching <paramref name="match"/> arrives within <paramref name="window"/>.</summary>
    public async Task AssertSilenceAsync(Func<JsonObject, bool> match, TimeSpan window)
    {
        await Task.Delay(window);
        lock (_inbox) Assert.DoesNotContain(_inbox, f => match(f));
    }

    /// <summary>Drops the connection as a lost network would: no close handshake.</summary>
    public Task AbortAsync()
    {
        _socket.Abort();
        return Task.CompletedTask;
    }

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
