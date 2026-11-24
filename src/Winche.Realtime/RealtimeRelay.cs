using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Winche.Realtime;

/// <summary>
/// Several servers: listens on Postgres for lists' changes (<see cref="RealtimeStore.Channel"/>, sent when an append,
/// open or close commits on any server) and tells this server's followers. A follower reads from the store, so a change
/// heard twice (here, and from this server's own notifier) or late only wakes it; it never repeats or skips an item.
/// </summary>
internal sealed class RealtimeRelay(RealtimeStore store, RealtimeNotifier notifier, ILogger<RealtimeRelay> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var wait = TimeSpan.FromMilliseconds(250);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await store.EnsureAsync(stoppingToken);
                await using var conn = await store.DataSource.OpenConnectionAsync(stoppingToken);
                conn.Notification += (_, e) => Heard(e.Payload);
                await using (var listen = new NpgsqlCommand($"LISTEN {RealtimeStore.Channel}", conn))
                    await listen.ExecuteNonQueryAsync(stoppingToken);
                wait = TimeSpan.FromMilliseconds(250);
                while (!stoppingToken.IsCancellationRequested) await conn.WaitAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception e)
            {
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
