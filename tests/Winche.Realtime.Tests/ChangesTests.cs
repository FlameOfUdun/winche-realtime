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
}
