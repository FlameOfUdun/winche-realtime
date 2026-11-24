using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>The console's settings for Realtime (area <c>realtime</c>): lifecycle, archiving and limits.</summary>
public class SettingsTests
{
    [Fact]
    public void Settings_apply_to_the_running_options_and_bad_ones_are_refused()
    {
        var options = new WincheRealtimeOptions { ConnectionString = "Host=x" };
        var defaults = RealtimeSettings.From(options);
        Assert.Equal((24.0, 30.0, false, 64.0), (defaults.IdleCloseHours, defaults.RetainClosedDays, defaults.ArchiveClosedLists, defaults.MaxItemKb));

        new RealtimeSettings { IdleCloseHours = null, RetainClosedDays = 7, ArchiveClosedLists = true, MaxItemKb = 16, MaxListItems = 1000, MaxListMb = 5 }.ApplyTo(options);
        Assert.Null(options.IdleCloseAfter);
        Assert.Equal(TimeSpan.FromDays(7), options.RetainClosedFor);
        Assert.True(options.ArchiveClosedLists);
        Assert.Equal((16 * 1024, 1000L, 5L * 1024 * 1024), (options.MaxItemBytes, options.MaxListItems, options.MaxListBytes));

        Assert.NotEmpty(new RealtimeSettings { RetainClosedDays = 0 }.Problems());
        Assert.NotEmpty(new RealtimeSettings { IdleCloseHours = 0 }.Problems());
        Assert.NotEmpty(new RealtimeSettings { MaxItemKb = 10_000 }.Problems());
        Assert.Empty(new RealtimeSettings().Problems());
    }
}
