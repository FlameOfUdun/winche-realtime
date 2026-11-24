using System.IO.Compression;
using System.Text;
using System.Text.Json.Nodes;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>The sweep: idle lists closed, closed lists kept for a while, then archived (if asked) and deleted.</summary>
[Collection("postgres")]
public class SweepTests(PostgresFixture pg)
{
    /// <summary>Time that moves only when told.</summary>
    private sealed class Clock : TimeProvider
    {
        public DateTimeOffset Now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => Now;
    }

    /// <summary>An archive that keeps what it's given (and can be made to fail, or to take its time).</summary>
    private sealed class Archive : IRealtimeArchive
    {
        public readonly List<(RealtimeListInfo List, byte[] File)> Kept = [];
        public bool Fail;
        public TimeSpan Delay;

        public async Task ArchiveAsync(RealtimeListInfo list, Stream file, CancellationToken ct)
        {
            if (Delay > TimeSpan.Zero) await Task.Delay(Delay, ct);
            if (Fail) throw new IOException("The archive is down.");
            using var copy = new MemoryStream();
            await file.CopyToAsync(copy, ct);
            lock (Kept) Kept.Add((list, copy.ToArray()));
        }
    }

    private async Task<(RealtimeLists Lists, RealtimeSweeper Sweeper, Clock Clock, Archive Archive, WincheRealtimeOptions Options)> Start(bool archive = true)
    {
        var clock = new Clock();
        var store = new Archive();
        var options = new WincheRealtimeOptions { ConnectionString = await pg.NewDatabaseAsync(), Time = clock };
        options.UseRulesText("match sessions/{s} { allow get, create, update: if true; }");
        if (archive) options.UseArchive(store);
        var lists = RealtimeLists.Create(options);
        return (lists, new RealtimeSweeper(lists.Store, options, lists.Notifier), clock, store, options);
    }

    private static RealtimeItemIn Item(string key) => new(key, Encoding.UTF8.GetBytes("p-" + key));

    [Fact]
    public async Task Idle_lists_are_closed_and_their_followers_told()
    {
        var (lists, sweeper, clock, _, _) = await Start();
        await lists.AppendAsync(null, "sessions/quiet", [Item("a:1")]);
        await lists.AppendAsync(null, "sessions/busy", [Item("a:1")]);
        var told = new TaskCompletionSource<long>();
        using var _ = lists.Notifier.Watch("sessions/quiet", last => told.TrySetResult(last));

        clock.Now += TimeSpan.FromHours(23);
        Assert.Equal(0, (await sweeper.RunOnceAsync())!.Closed);   // not idle 24 hours yet
        await lists.AppendAsync(null, "sessions/busy", [Item("a:2")]);   // busy: an item an hour before the next sweep
        clock.Now += TimeSpan.FromHours(2);
        Assert.Equal(1, (await sweeper.RunOnceAsync())!.Closed);
        Assert.Equal("closed", (await lists.Store.InfoAsync("sessions/quiet"))!.State);
        Assert.Equal("open", (await lists.Store.InfoAsync("sessions/busy"))!.State);
        Assert.Equal(1L, await told.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Closed_lists_are_kept_their_time_then_archived_in_order_and_deleted()
    {
        var (lists, sweeper, clock, archive, _) = await Start();
        await lists.AppendAsync(null, "sessions/s1", [Item("a:1"), Item("b:1")]);
        await lists.AppendAsync(null, "sessions/s1", [Item("a:2")]);
        await lists.Store.CloseAsync("sessions/s1");

        clock.Now += TimeSpan.FromDays(29);
        Assert.Equal(0, (await sweeper.RunOnceAsync())!.Deleted);
        Assert.NotNull(await lists.Store.InfoAsync("sessions/s1"));

        clock.Now += TimeSpan.FromDays(2);
        var swept = (await sweeper.RunOnceAsync())!;
        Assert.Equal((1, 1), (swept.Archived, swept.Deleted));
        Assert.Null(await lists.Store.InfoAsync("sessions/s1"));
        Assert.Empty((await lists.Store.ReadAsync("sessions/s1", 0, 10)).Items);

        var (list, file) = Assert.Single(archive.Kept);
        Assert.Equal(("sessions/s1", 3L), (list.Path, list.Count));
        using var lines = new StreamReader(new GZipStream(new MemoryStream(file), CompressionMode.Decompress));
        var items = (await lines.ReadToEndAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonNode.Parse(l)!).ToList();
        Assert.Equal(["a:1", "b:1", "a:2"], items.Select(i => (string)i["key"]!));
        Assert.Equal([1L, 2L, 3L], items.Select(i => (long)i["position"]!));
        Assert.Equal("p-b:1", Encoding.UTF8.GetString(Convert.FromBase64String((string)items[1]["payload"]!)));
        Assert.Equal("sessions/s1/archive.ndjson.gz", RealtimeArchives.FileName("sessions/s1"));
    }

    [Fact]
    public async Task A_list_the_archive_can_not_take_is_kept_for_the_next_sweep()
    {
        var (lists, sweeper, clock, archive, _) = await Start();
        await lists.AppendAsync(null, "sessions/s1", [Item("a:1")]);
        await lists.Store.CloseAsync("sessions/s1");
        clock.Now += TimeSpan.FromDays(31);
        archive.Fail = true;
        var failed = (await sweeper.RunOnceAsync())!;
        Assert.Equal((0, 0, 1), (failed.Archived, failed.Deleted, failed.Failed));
        Assert.NotNull(await lists.Store.InfoAsync("sessions/s1"));   // nothing lost
        archive.Fail = false;
        Assert.Equal(1, (await sweeper.RunOnceAsync())!.Deleted);
    }

    [Fact]
    public async Task Without_an_archive_closed_lists_are_just_deleted_when_their_time_is_up()
    {
        var (lists, sweeper, clock, archive, _) = await Start(archive: false);
        await lists.AppendAsync(null, "sessions/s1", [Item("a:1")]);
        await lists.Store.CloseAsync("sessions/s1");
        clock.Now += TimeSpan.FromDays(31);
        Assert.Equal((0, 1), (await sweeper.RunOnceAsync()) is { } s ? (s.Archived, s.Deleted) : default);
        Assert.Empty(archive.Kept);
    }

    [Fact]
    public async Task With_archiving_turned_off_closed_lists_are_deleted_without_archiving()
    {
        var (lists, sweeper, clock, archive, options) = await Start();
        options.ArchiveClosedLists = false;
        await lists.AppendAsync(null, "sessions/s1", [Item("a:1")]);
        await lists.Store.CloseAsync("sessions/s1");
        clock.Now += TimeSpan.FromDays(31);
        Assert.Equal((0, 1), (await sweeper.RunOnceAsync()) is { } s ? (s.Archived, s.Deleted) : default);
        Assert.Empty(archive.Kept);
    }

    [Fact]
    public async Task Two_servers_sweeping_at_once_archive_each_list_once()
    {
        var (lists, sweeper, clock, archive, options) = await Start();
        var other = new RealtimeSweeper(new RealtimeStore(options), options, new RealtimeNotifier());
        await lists.AppendAsync(null, "sessions/s1", [Item("a:1")]);
        await lists.Store.CloseAsync("sessions/s1");
        clock.Now += TimeSpan.FromDays(31);
        archive.Delay = TimeSpan.FromMilliseconds(300);
        var both = await Task.WhenAll(sweeper.RunOnceAsync(), other.RunOnceAsync());
        Assert.Single(archive.Kept);
        Assert.Contains(both, s => s is null);   // one found the other sweeping, and left it
    }
}
