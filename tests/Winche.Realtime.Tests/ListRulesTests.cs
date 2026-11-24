using System.Text;
using Winche;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>Who may do what to a list: the project's rules, default deny, checked once per append or subscription.</summary>
[Collection("postgres")]
public class ListRulesTests(PostgresFixture pg)
{
    private const string Rules = """
        match sessions/{session} {
          // Anyone signed in reads; appends only under their own device, and at most 100 items a list.
          allow get: if request.auth != null;
          allow create: if request.auth != null && request.resource.device == request.auth.uid
                         && (resource == null || resource.count + request.resource.count <= 100);
          // Only the session's host closes it.
          allow update: if request.auth.token.host == true;
        }
        """;

    private async Task<RealtimeLists> Lists(string rules = Rules)
    {
        var options = new WincheRealtimeOptions { ConnectionString = await pg.NewDatabaseAsync() }.UseRulesText(rules);
        return RealtimeLists.Create(options);
    }

    private static Dictionary<string, object?> As(string uid, bool host = false) =>
        new() { ["uid"] = uid, ["sub"] = uid, ["host"] = host };

    private static RealtimeItemIn Item(string key) => new(key, Encoding.UTF8.GetBytes(key));

    [Fact]
    public async Task Rules_decide_and_everything_else_is_refused()
    {
        var lists = await Lists();
        Assert.Equal([1L], await lists.AppendAsync(As("ann"), "sessions/s1", [Item("ann:1")]));
        Assert.Single((await lists.ReadAsync(As("bob"), "sessions/s1", 0, 10)).Items);

        // Bob appending as Ann's device: refused, and nothing kept.
        var asAnn = await Assert.ThrowsAsync<RealtimeException>(() => lists.AppendAsync(As("bob"), "sessions/s1", [Item("bob:1"), Item("ann:2")]));
        Assert.Equal((WincheErrorCode.PermissionDenied, "denied"), (asAnn.Code, asAnn.Reason));
        Assert.Single((await lists.ReadAsync(As("bob"), "sessions/s1", 0, 10)).Items);

        // Signed out: nothing. Another path: no rule, nothing.
        await Assert.ThrowsAsync<RealtimeException>(() => lists.ReadAsync(null, "sessions/s1", 0, 10));
        await Assert.ThrowsAsync<RealtimeException>(() => lists.AppendAsync(As("ann"), "other/s1", [Item("ann:1")]));

        // Closing: the host only.
        await Assert.ThrowsAsync<RealtimeException>(() => lists.CloseAsync(As("bob"), "sessions/s1"));
        Assert.Equal("closed", (await lists.CloseAsync(As("host", host: true), "sessions/s1")).State);
    }

    [Fact]
    public async Task Rules_see_the_lists_size_so_they_can_cap_it()
    {
        var lists = await Lists();
        await lists.AppendAsync(As("ann"), "sessions/s1", Enumerable.Range(1, 99).Select(n => Item($"ann:{n}")).ToList());
        await Assert.ThrowsAsync<RealtimeException>(() => lists.AppendAsync(As("ann"), "sessions/s1", [Item("ann:100"), Item("ann:101")]));
        Assert.Equal([100L], await lists.AppendAsync(As("ann"), "sessions/s1", [Item("ann:100")]));
    }

    [Fact]
    public async Task Without_rules_everything_is_refused()
    {
        var lists = RealtimeLists.Create(new WincheRealtimeOptions { ConnectionString = await pg.NewDatabaseAsync() });
        await Assert.ThrowsAsync<RealtimeException>(() => lists.AppendAsync(As("ann"), "sessions/s1", [Item("ann:1")]));
        await Assert.ThrowsAsync<RealtimeException>(() => lists.ReadAsync(As("ann"), "sessions/s1", 0, 10));
    }

    [Fact]
    public async Task Appends_are_announced_to_whoever_waits_on_the_path()
    {
        var lists = await Lists();
        var heard = new TaskCompletionSource<long>();
        using var _ = lists.Notifier.Watch("sessions/s1", last => heard.TrySetResult(last));
        await lists.AppendAsync(As("ann"), "sessions/s1", [Item("ann:1"), Item("ann:2")]);
        Assert.Equal(2L, await heard.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
