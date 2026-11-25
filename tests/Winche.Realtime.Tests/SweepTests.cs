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
        public Func<RealtimeListInfo, bool>? FailFor;
        public TimeSpan Delay;

        public async Task ArchiveAsync(RealtimeListInfo list, Stream file, CancellationToken ct)
        {
            if (Delay > TimeSpan.Zero) await Task.Delay(Delay, ct);
            if (Fail || FailFor?.Invoke(list) == true) throw new IOException("The archive is down.");
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
    }

    [Fact]
    public void Each_archive_has_a_name_of_its_own_that_any_storage_takes()
    {
        var closed = new DateTimeOffset(2026, 11, 25, 9, 30, 15, 123, TimeSpan.Zero);
        RealtimeListInfo List(string path, DateTimeOffset at) => new(path, "closed", 1, 1) { ClosedAt = at };
        Assert.Equal("sessions/s1/20261125T093015123Z.ndjson.gz", RealtimeArchives.FileName(List("sessions/s1", closed)));
        // The same path used again: another file, the first one kept.
        Assert.NotEqual(RealtimeArchives.FileName(List("sessions/s1", closed)), RealtimeArchives.FileName(List("sessions/s1", closed.AddDays(3))));
        // ':' isn't a file name's: written as '~' (paths never have one).
        Assert.Equal("sessions/user~1/20261125T093015123Z.ndjson.gz", RealtimeArchives.FileName(List("sessions/user:1", closed)));
    }

    [Theory]
    [InlineData("sessions/.")]
    [InlineData("sessions/..")]
    [InlineData(".winche-x/s1")]
    public async Task Paths_have_no_segment_starting_with_a_dot(string path)
    {
        var (lists, _, _, _, _) = await Start();
        Assert.Equal(WincheErrorCode.InvalidArgument, (await Assert.ThrowsAsync<RealtimeException>(() => lists.AppendAsync(null, path, [Item("a:1")]))).Code);
    }

    [Fact]
    public async Task Lists_that_keep_failing_to_archive_wait_an_hour_and_never_hold_up_the_rest()
    {
        var (lists, sweeper, clock, archive, _) = await Start();
        archive.FailFor = l => l.Path.StartsWith("sessions/bad");
        for (var n = 0; n < 201; n++)
        {
            await lists.AppendAsync(null, $"sessions/bad{n}", [Item("a:1")]);
            await lists.Store.CloseAsync($"sessions/bad{n}");
        }
        clock.Now += TimeSpan.FromMinutes(1);
        await lists.AppendAsync(null, "sessions/good", [Item("a:1")]);
        await lists.Store.CloseAsync("sessions/good");   // closed after all of them: the last in line
        clock.Now += TimeSpan.FromDays(31);
        var first = (await sweeper.RunOnceAsync())!;
        Assert.Equal((0, 200), (first.Archived, first.Failed));
        var second = (await sweeper.RunOnceAsync())!;   // a minute later: the failed ones wait, the rest go
        Assert.Equal((1, 1, 1), (second.Archived, second.Deleted, second.Failed));
        Assert.Null(await lists.Store.InfoAsync("sessions/good"));
        Assert.Equal(0, (await sweeper.RunOnceAsync())!.Failed);   // nothing retried within the hour
        clock.Now += TimeSpan.FromHours(1);
        archive.FailFor = null;
        Assert.Equal(200, (await sweeper.RunOnceAsync())!.Archived);   // an hour on: tried again
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
        clock.Now += TimeSpan.FromHours(1);   // tried again an hour later
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
