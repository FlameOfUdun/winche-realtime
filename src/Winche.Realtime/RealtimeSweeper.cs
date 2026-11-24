using System.IO.Compression;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Winche.Core;

namespace Winche.Realtime;

/// <summary>Where closed lists go before they're deleted: one gzipped NDJSON file a list (an item a line, in order).</summary>
public interface IRealtimeArchive
{
    /// <summary>Keeps <paramref name="file"/> for <paramref name="list"/>. Failing keeps the list for the next sweep.</summary>
    Task ArchiveAsync(RealtimeListInfo list, Stream file, CancellationToken ct);
}

public static class RealtimeArchives
{
    /// <summary>The file's name for the list at <paramref name="path"/>, relative: <c>{path}/archive.ndjson.gz</c>.</summary>
    public static string FileName(string path) => $"{path}/archive.ndjson.gz";

    /// <summary>
    /// The list's items as gzipped NDJSON, an item a line: <c>{ key, position, at, by, payload }</c> (payload base64),
    /// read in pages so a big list never sits in memory whole.
    /// </summary>
    internal static async Task<Stream> FileOfAsync(RealtimeStore store, string path, CancellationToken ct)
    {
        var file = new MemoryStream();
        await using (var gzip = new GZipStream(file, CompressionLevel.Optimal, leaveOpen: true))
        await using (var writer = new StreamWriter(gzip))
        {
            long after = 0;
            while (true)
            {
                var page = await store.ReadAsync(path, after, 1000, ct);
                foreach (var item in page.Items)
                {
                    await writer.WriteLineAsync(JsonSerializer.Serialize(new
                    {
                        key = item.Key, position = item.Position, at = item.At.ToString("O"), by = item.By, payload = Convert.ToBase64String(item.Payload),
                    }));
                }
                if (page.Items.Count < 1000) break;
                after = page.Items[^1].Position;
            }
        }
        file.Position = 0;
        return file;
    }
}

/// <summary>What a sweep did.</summary>
public sealed record RealtimeSweep(int Closed, int Archived, int Deleted, int Failed);

/// <summary>
/// The sweep, on one server at a time (a Postgres advisory lock): lists idle for <see cref="WincheRealtimeOptions.IdleCloseAfter"/>
/// are closed (their followers told), and lists closed for <see cref="WincheRealtimeOptions.RetainClosedFor"/> are
/// archived (when there's an archive) and deleted. A list the archive can't take stays for the next sweep.
/// </summary>
public sealed class RealtimeSweeper(RealtimeStore store, WincheRealtimeOptions options, RealtimeNotifier notifier, ILogger? log = null,
    RealtimePresence? presence = null)
{
    private readonly ILogger _log = log ?? NullLogger.Instance;
    private const int Batch = 200;

    /// <summary>Sweeps once: what it did, or null when another server is sweeping.</summary>
    public async Task<RealtimeSweep?> RunOnceAsync(CancellationToken ct = default)
    {
        await store.EnsureAsync(ct);
        await using var lease = await AdvisoryLease.TryAcquireAsync(store.DataSource, "realtime-sweep", TimeSpan.FromSeconds(30), ct);
        if (lease is null) return null;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, lease.Lost);
        var token = linked.Token;
        var now = options.Time.GetUtcNow();

        var closed = 0;
        if (options.IdleCloseAfter is { } idle)
        {
            foreach (var list in await store.CloseIdleAsync(now - idle, Batch, token))
            {
                closed++;
                notifier.Changed(list.Path, list.Count);
                _log.LogInformation("Realtime closed {Path}: no item for {Idle}.", list.Path, idle);
            }
        }

        int archived = 0, deleted = 0, failed = 0;
        foreach (var list in await store.ExpiredAsync(now - options.RetainClosedFor, Batch, token))
        {
            if (options.Archive is { } archive)
            {
                try
                {
                    await using var file = await RealtimeArchives.FileOfAsync(store, list.Path, token);
                    await archive.ArchiveAsync(list, file, token);
                    archived++;
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    failed++;
                    _log.LogWarning(e, "Realtime couldn't archive {Path}: it's kept for the next sweep.", list.Path);
                    continue;
                }
            }
            await store.DeleteAsync(list.Path, token);
            deleted++;
            _log.LogInformation("Realtime deleted {Path} ({Count} items), closed {Days:0} days ago.", list.Path, list.Count, (now - list.ClosedAt!.Value).TotalDays);
        }
        // Servers that stopped checking in (crashed): their members go, their on-disconnect items appended.
        if (presence is not null && await presence.SweepDeadServersAsync(now - options.PresenceTimeout, token) is > 0 and var dropped)
            _log.LogInformation("Realtime removed {Count} members of servers that stopped checking in.", dropped);
        return new RealtimeSweep(closed, archived, deleted, failed);
    }
}

/// <summary>Runs the sweep every <see cref="WincheRealtimeOptions.SweepEvery"/>.</summary>
internal sealed class RealtimeSweepService(RealtimeSweeper sweeper, WincheRealtimeOptions options, ILogger<RealtimeSweepService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await sweeper.RunOnceAsync(stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception e) { log.LogWarning(e, "Realtime's sweep failed: it runs again shortly."); }
            try { await Task.Delay(options.SweepEvery, options.Time, stoppingToken); }
            catch (OperationCanceledException) { return; }
        }
    }
}
