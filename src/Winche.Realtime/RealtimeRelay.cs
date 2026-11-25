using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Winche.Realtime;

/// <summary>
/// Several servers: listens on Postgres for lists' changes (<see cref="RealtimeStore.Channel"/>, sent when an append,
/// open or close commits on any server) and tells this server's followers. A follower reads from the store, so a change
/// heard twice (here, and from this server's own notifier) or late only wakes it; it never repeats or skips an item.
/// </summary>
internal sealed class RealtimeRelay(RealtimeStore store, RealtimeNotifier notifier, RealtimeChannels channels, ILogger<RealtimeRelay> log) : BackgroundService
{
    /// <summary>How long the listening connection may be quiet before it's checked.</summary>
    private static readonly TimeSpan KeepAlive = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var wait = TimeSpan.FromMilliseconds(250);
        var lost = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await store.EnsureAsync(stoppingToken);
                await using var conn = await store.DataSource.OpenConnectionAsync(stoppingToken);
                conn.Notification += (_, e) =>
                {
                    try { if (e.Channel == RealtimeChannels.Channel) channels.Heard(e.Payload); else Heard(e.Payload); }
                    catch (Exception x) { log.LogWarning(x, "Realtime's relay couldn't read a notification."); }
                };
                await using (var listen = new NpgsqlCommand($"LISTEN {RealtimeStore.Channel}; LISTEN {RealtimeChannels.Channel}", conn))
                    await listen.ExecuteNonQueryAsync(stoppingToken);
                wait = TimeSpan.FromMilliseconds(250);
                if (lost)
                {
                    // What other servers said while this one was away is gone: every follower here reads on from where it is.
                    lost = false;
                    notifier.ChangedAll();
                }
                while (!stoppingToken.IsCancellationRequested)
                {
                    if (await conn.WaitAsync(KeepAlive, stoppingToken)) continue;
                    // Quiet for a while: make sure the connection is still there (a half-open one never says it's gone).
                    await using var ping = new NpgsqlCommand("SELECT 1", conn) { CommandTimeout = 10 };
                    await ping.ExecuteScalarAsync(stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception e)
            {
                lost = true;
                log.LogWarning(e, "Realtime's relay lost Postgres: listening again in {Wait}.", wait);
                try { await Task.Delay(wait, stoppingToken); } catch (OperationCanceledException) { return; }
                wait = TimeSpan.FromTicks(Math.Min(wait.Ticks * 2, TimeSpan.FromSeconds(30).Ticks));
            }
        }
    }

    private void Heard(string payload)
    {
        var newline = payload.IndexOf('\n');
        if (newline <= 0 || !long.TryParse(payload.AsSpan(0, newline), out var last)) return;
        notifier.Changed(payload[(newline + 1)..], last);
    }
}
