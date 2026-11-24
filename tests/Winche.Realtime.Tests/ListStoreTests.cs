using System.Text;
using Winche;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>Lists: ordered, append-only, deduplicated by the client's keys, read from a position, within limits.</summary>
[Collection("postgres")]
public class ListStoreTests(PostgresFixture pg)
{
    private async Task<RealtimeStore> Store(Action<WincheRealtimeOptions>? configure = null)
    {
        var options = new WincheRealtimeOptions { ConnectionString = await pg.NewDatabaseAsync() };
        configure?.Invoke(options);
        var store = new RealtimeStore(options);
        await store.EnsureAsync();
        return store;
    }

    private static RealtimeItemIn Item(string key, string text) => new(key, Encoding.UTF8.GetBytes(text));

    [Fact]
    public async Task Items_get_positions_in_order_and_read_back_from_a_position()
    {
        var store = await Store();
        Assert.Equal([1L, 2L], await store.AppendAsync("sessions/s1", [Item("d1:1", "a"), Item("d1:2", "b")], "alice"));
        Assert.Equal([3L], await store.AppendAsync("sessions/s1", [Item("d2:1", "c")], "bob"));

        var all = await store.ReadAsync("sessions/s1", after: 0, limit: 100);
        Assert.Equal(["a", "b", "c"], all.Items.Select(i => Encoding.UTF8.GetString(i.Payload)));
        Assert.Equal(["alice", "alice", "bob"], all.Items.Select(i => i.By));
        Assert.Equal(new RealtimeListInfo("sessions/s1", "open", 3, 3), all.List! with { OpenedAt = default, ClosedAt = null });

        var rest = await store.ReadAsync("sessions/s1", after: 1, limit: 1);
        Assert.Equal([2L], rest.Items.Select(i => i.Position));   // a page: after position 1, one item
        Assert.Empty((await store.ReadAsync("sessions/none", 0, 10)).Items);
        Assert.Null((await store.ReadAsync("sessions/none", 0, 10)).List);
    }

    [Fact]
    public async Task A_retried_append_never_duplicates_and_a_changed_payload_is_refused()
    {
        var store = await Store();
        await store.AppendAsync("sessions/s1", [Item("d1:1", "a")], "alice");
        // The same key and payload again (a retry): answered with the stored item's position, nothing added.
        Assert.Equal([1L, 2L], await store.AppendAsync("sessions/s1", [Item("d1:1", "a"), Item("d1:2", "b")], "alice"));
        Assert.Equal(2, (await store.ReadAsync("sessions/s1", 0, 10)).Items.Count);

        // A different payload under a stored key: the whole batch is refused, nothing of it kept.
        var e = await Assert.ThrowsAsync<RealtimeException>(() => store.AppendAsync("sessions/s1", [Item("d1:3", "c"), Item("d1:1", "changed")], "alice"));
        Assert.Equal(WincheErrorCode.AlreadyExists, e.Code);
        Assert.Equal(2, (await store.ReadAsync("sessions/s1", 0, 10)).Items.Count);
        // The same key twice in one batch with different payloads: refused.
        Assert.Equal(WincheErrorCode.InvalidArgument, (await Assert.ThrowsAsync<RealtimeException>(() =>
            store.AppendAsync("sessions/s1", [Item("d1:9", "x"), Item("d1:9", "y")], "alice"))).Code);
    }

    [Fact]
    public async Task Concurrent_appends_get_one_order_with_no_gaps()
    {
        var store = await Store();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(device => Task.Run(async () =>
        {
            for (var n = 0; n < 25; n++) await store.AppendAsync("sessions/s1", [Item($"d{device}:{n}", $"{device}.{n}")], $"u{device}");
        })));
        var items = (await store.ReadAsync("sessions/s1", 0, 1000)).Items;
        Assert.Equal(Enumerable.Range(1, 200).Select(i => (long)i), items.Select(i => i.Position));
        // Each device's own items keep the order it appended them in.
        foreach (var device in Enumerable.Range(0, 8))
            Assert.Equal(Enumerable.Range(0, 25).Select(n => $"{device}.{n}"),
                items.Where(i => i.By == $"u{device}").Select(i => Encoding.UTF8.GetString(i.Payload)));
    }

    [Fact]
    public async Task Limits_on_items_and_lists_refuse_the_whole_batch()
    {
        var store = await Store(o => { o.MaxItemBytes = 10; o.MaxListItems = 3; o.MaxBatchItems = 5; });
        Assert.Equal(WincheErrorCode.InvalidArgument, (await Assert.ThrowsAsync<RealtimeException>(() =>
            store.AppendAsync("sessions/s1", [Item("k1", new string('x', 11))], "a"))).Code);
        await store.AppendAsync("sessions/s1", [Item("k1", "a"), Item("k2", "b")], "a");
        var full = await Assert.ThrowsAsync<RealtimeException>(() => store.AppendAsync("sessions/s1", [Item("k3", "c"), Item("k4", "d")], "a"));
        Assert.Equal(WincheErrorCode.ResourceExhausted, full.Code);
        Assert.Equal(2, (await store.ReadAsync("sessions/s1", 0, 10)).Items.Count);   // none of that batch
        Assert.Equal(WincheErrorCode.InvalidArgument, (await Assert.ThrowsAsync<RealtimeException>(() =>
            store.AppendAsync("sessions/s2", Enumerable.Range(0, 6).Select(n => Item($"k{n}", "x")).ToList(), "a"))).Code);
        Assert.Equal(WincheErrorCode.InvalidArgument, (await Assert.ThrowsAsync<RealtimeException>(() =>
            store.AppendAsync("sessions/s2", [Item("", "x")], "a"))).Code);   // a key is required
    }

    [Fact]
    public async Task A_closed_list_takes_no_appends_and_still_reads()
    {
        var store = await Store();
        await store.AppendAsync("sessions/s1", [Item("k1", "a")], "a");
        var closed = await store.CloseAsync("sessions/s1");
        Assert.Equal("closed", closed.State);
        Assert.NotNull(closed.ClosedAt);
        Assert.Equal(WincheErrorCode.FailedPrecondition, (await Assert.ThrowsAsync<RealtimeException>(() =>
            store.AppendAsync("sessions/s1", [Item("k2", "b")], "a"))).Code);
        Assert.Single((await store.ReadAsync("sessions/s1", 0, 10)).Items);
        // A retry of an item it already has is still answered (nothing new is added).
        Assert.Equal([1L], await store.AppendAsync("sessions/s1", [Item("k1", "a")], "a"));
        // Opened explicitly: a new list, empty.
        Assert.Equal(("open", 0L), ((await store.OpenAsync("sessions/s2")).State, (await store.OpenAsync("sessions/s2")).Count));
        Assert.Equal(WincheErrorCode.NotFound, (await Assert.ThrowsAsync<RealtimeException>(() => store.CloseAsync("sessions/none"))).Code);
    }

    [Theory]
    [InlineData("")]
    [InlineData("/sessions/s1")]
    [InlineData("sessions//s1")]
    [InlineData("sessions/s1/")]
    [InlineData("sessions/s#1")]
    public async Task Paths_are_slash_separated_segments(string path)
    {
        var store = await Store();
        Assert.Equal(WincheErrorCode.InvalidArgument, (await Assert.ThrowsAsync<RealtimeException>(() => store.AppendAsync(path, [Item("k", "a")], "a"))).Code);
    }

    [Fact]
    public async Task Lists_page_by_path_with_a_prefix_and_state_and_a_closed_one_deletes()
    {
        var store = await Store();
        foreach (var p in new[] { "sessions/a", "sessions/b", "sessions/c", "other/x" }) await store.AppendAsync(p, [Item("k", "v")], null);
        await store.CloseAsync("sessions/b");
        Assert.Equal(["sessions/a", "sessions/b"], (await store.ListsAsync("sessions/", null, null, 2)).Select(l => l.Path));
        Assert.Equal(["sessions/c"], (await store.ListsAsync("sessions/", null, "sessions/b", 2)).Select(l => l.Path));
        Assert.Equal(["sessions/b"], (await store.ListsAsync(null, "closed", null, 10)).Select(l => l.Path));
        Assert.False(await store.DeleteClosedAsync("sessions/a"));   // open: not deleted
        Assert.True(await store.DeleteClosedAsync("sessions/b"));
        Assert.Null(await store.InfoAsync("sessions/b"));
    }
}
