namespace Winche.Realtime;

/// <summary>
/// The settings area <c>realtime</c> (<c>o.UseSettings()</c>): when lists close and go, whether they're archived first,
/// and how big they may get. Changes apply to the running server at once.
/// </summary>
public sealed record RealtimeSettings
{
    public const string Area = "realtime";

    /// <summary>An open list with no new item for this many hours is closed; null: never.</summary>
    public double? IdleCloseHours { get; init; } = 24;

    /// <summary>A closed list is kept this many days (it still reads), then deleted.</summary>
    public double RetainClosedDays { get; init; } = 30;

    /// <summary>Whether a closed list is archived (one file in the project's storage) before it's deleted.</summary>
    public bool ArchiveClosedLists { get; init; }

    /// <summary>The largest item, in KB.</summary>
    public double MaxItemKb { get; init; } = 64;

    /// <summary>The most items a list holds.</summary>
    public long MaxListItems { get; init; } = 200_000;

    /// <summary>The most a list holds, in MB.</summary>
    public double MaxListMb { get; init; } = 50;

    public static RealtimeSettings From(WincheRealtimeOptions o) => new()
    {
        IdleCloseHours = o.IdleCloseAfter?.TotalHours,
        RetainClosedDays = o.RetainClosedFor.TotalDays,
        ArchiveClosedLists = o.ArchiveClosedLists,
        MaxItemKb = Math.Round(o.MaxItemBytes / 1024.0, 3),
        MaxListItems = o.MaxListItems,
        MaxListMb = Math.Round(o.MaxListBytes / 1048576.0, 3),
    };

    public void ApplyTo(WincheRealtimeOptions o)
    {
        o.IdleCloseAfter = IdleCloseHours is { } hours ? TimeSpan.FromHours(hours) : null;
        o.RetainClosedFor = TimeSpan.FromDays(RetainClosedDays);
        o.ArchiveClosedLists = ArchiveClosedLists;
        o.MaxItemBytes = (int)(MaxItemKb * 1024);
        o.MaxListItems = MaxListItems;
        o.MaxListBytes = (long)(MaxListMb * 1048576);
    }

    public IReadOnlyList<string> Problems()
    {
        var problems = new List<string>();
        if (IdleCloseHours is < 0.1 or > 24 * 365) problems.Add("Close idle lists after 6 minutes to a year (or never).");
        if (RetainClosedDays is < 0.01 or > 3650) problems.Add("Keep closed lists for 15 minutes to 10 years.");
        if (MaxItemKb is < 0.1 or > 1024) problems.Add("An item is at most 0.1 KB to 1 MB.");
        if (MaxListItems is < 1 or > 100_000_000) problems.Add("A list holds 1 to 100 million items.");
        if (MaxListMb is < 0.01 or > 100 * 1024) problems.Add("A list holds 0.01 MB to 100 GB.");
        return problems;
    }
}
