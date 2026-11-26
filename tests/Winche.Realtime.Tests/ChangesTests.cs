using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Winche.Core;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>Closed lists are published as changes (source realtime), for webhooks and functions' triggers.</summary>
[Collection("postgres")]
public class ChangesTests(PostgresFixture pg)
{
    private sealed class Sink : IWincheChangeSink
    {
        public readonly List<WincheChange> Changes = [];
        public Task OnChangesAsync(IReadOnlyList<WincheChange> changes, CancellationToken ct) { lock (Changes) Changes.AddRange(changes); return Task.CompletedTask; }
    }

    [Fact]
    public async Task Closing_a_list_by_hand_or_for_idleness_publishes_it()
    {
        var sink = new Sink();
        var services = new ServiceCollection().AddLogging().AddSingleton<IWincheChangeSink>(sink);
        var connection = await pg.NewDatabaseAsync();
        services.AddWincheRealtime(o => { o.ConnectionString = connection; o.UseRulesText("match sessions/{s} { allow get, create, update: if true; }"); });
        await using var sp = services.BuildServiceProvider();
        Assert.Contains(sp.GetServices<WincheChangeSource>(), s => s.Name == "realtime" && s.Events.Contains("closed"));

        var lists = sp.GetRequiredService<RealtimeLists>();
        await lists.AppendAsync(null, "sessions/s1", [new("a:1", Encoding.UTF8.GetBytes("x")), new("a:2", Encoding.UTF8.GetBytes("y"))]);
        await lists.CloseAsync(null, "sessions/s1");
        var closed = Assert.Single(sink.Changes);
        Assert.Equal(("realtime", "closed", "sessions/s1"), (closed.Source, closed.Type, closed.Path));
        Assert.Equal(2L, (long)closed.Data!["count"]!);
        Assert.Equal("sessions/s1", closed.Variables["path"]);

        var options = sp.GetRequiredService<WincheRealtimeOptions>();
        await lists.AppendAsync(null, "sessions/s2", [new("a:1", Encoding.UTF8.GetBytes("x"))]);
        options.IdleCloseAfter = TimeSpan.Zero;
        await sp.GetRequiredService<RealtimeSweeper>().RunOnceAsync();
        Assert.Contains(sink.Changes, c => c.Path == "sessions/s2" && c.Type == "closed");
    }

    /// <summary>A sink that fails its first <see cref="FailFirst"/> calls (the hooks' queue unreachable for a moment).</summary>
    private sealed class FlakySink : IWincheChangeSink
    {
        public int FailFirst = 1;
        public readonly List<WincheChange> Changes = [];
        public Task OnChangesAsync(IReadOnlyList<WincheChange> changes, CancellationToken ct)
        {
            if (Interlocked.Decrement(ref FailFirst) >= 0) throw new InvalidOperationException("The hooks' queue is down.");
            lock (Changes) Changes.AddRange(changes);
            return Task.CompletedTask;
        }
    }

    private sealed class Clock : TimeProvider
    {
        public DateTimeOffset Now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => Now;
    }

    [Fact]
    public async Task A_closed_event_that_could_not_be_published_goes_out_later_once()
    {
        var sink = new FlakySink();
        var clock = new Clock();
        var services = new ServiceCollection().AddLogging().AddSingleton<IWincheChangeSink>(sink);
        var connection = await pg.NewDatabaseAsync();
        services.AddWincheRealtime(o => { o.ConnectionString = connection; o.Time = clock; o.UseRulesText("match sessions/{s} { allow get, create, update: if true; }"); });
        await using var sp = services.BuildServiceProvider();
        var lists = sp.GetRequiredService<RealtimeLists>();
        await lists.AppendAsync(null, "sessions/s1", [new("a:1", Encoding.UTF8.GetBytes("x"))]);

        await Assert.ThrowsAsync<InvalidOperationException>(() => lists.CloseAsync(null, "sessions/s1"));
        Assert.Equal("closed", (await lists.Store.InfoAsync("sessions/s1"))!.State);   // closed, its event not out
        Assert.Empty(sink.Changes);

        var sweeper = sp.GetRequiredService<RealtimeSweeper>();
        await sweeper.RunOnceAsync();
        Assert.Empty(sink.Changes);   // a close may still be publishing: not within its first minute
        clock.Now += TimeSpan.FromMinutes(2);
        await sweeper.RunOnceAsync();
        Assert.Equal(("closed", "sessions/s1"), (Assert.Single(sink.Changes).Type, sink.Changes[0].Path));
        var info = (await lists.Store.InfoAsync("sessions/s1"))!;
        Assert.Equal(RealtimeChanges.IdOf(info), sink.Changes[0].Id);   // the id a first try had too: a repeat is recognisable
        await sweeper.RunOnceAsync();
        Assert.Single(sink.Changes);   // once

        // An idle close that couldn't be published either: the next sweeps catch it up.
        sink.FailFirst = 1;
        await lists.AppendAsync(null, "sessions/s2", [new("a:1", Encoding.UTF8.GetBytes("x"))]);
        var options = sp.GetRequiredService<WincheRealtimeOptions>();
        options.IdleCloseAfter = TimeSpan.FromMinutes(1);
        clock.Now += TimeSpan.FromMinutes(5);
        await sweeper.RunOnceAsync();   // closes it; its event fails
        clock.Now += TimeSpan.FromMinutes(2);
        await sweeper.RunOnceAsync();
        Assert.Single(sink.Changes, c => c.Path == "sessions/s2");
    }
}
