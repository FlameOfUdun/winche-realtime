using Winche.Rules;
using Winche.Rules.Text;

namespace Winche.Realtime;

/// <summary>Winche Realtime's settings: where it keeps lists, how big they may get, and its rules.</summary>
public sealed class WincheRealtimeOptions
{
    /// <summary>The Postgres database lists are kept in.</summary>
    public string ConnectionString { get; set; } = "";

    /// <summary>The largest item's payload, in bytes. Default 64 KB.</summary>
    public int MaxItemBytes { get; set; } = 64 * 1024;

    /// <summary>The most items one append may carry. Default 500.</summary>
    public int MaxBatchItems { get; set; } = 500;

    /// <summary>The most items a list may hold. Default 200,000.</summary>
    public long MaxListItems { get; set; } = 200_000;

    /// <summary>The most payload bytes a list may hold. Default 50 MB.</summary>
    public long MaxListBytes { get; set; } = 50L * 1024 * 1024;

    /// <summary>The longest key, in characters. Default 200.</summary>
    public int MaxKeyLength { get; set; } = 200;

    /// <summary>A list with no new item for this long is closed by the sweep. Null: never. Default 24 hours.</summary>
    public TimeSpan? IdleCloseAfter { get; set; } = TimeSpan.FromHours(24);

    /// <summary>How long a closed list is kept (it still reads) before it's archived, if asked, and deleted. Default 30 days.</summary>
    public TimeSpan RetainClosedFor { get; set; } = TimeSpan.FromDays(30);

    /// <summary>How often the sweep runs (one server at a time). Default a minute.</summary>
    public TimeSpan SweepEvery { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>The clock (tests move it).</summary>
    public TimeProvider Time { get; set; } = TimeProvider.System;

    /// <summary>Where closed lists go before they're deleted, if anywhere.</summary>
    public IRealtimeArchive? Archive { get; private set; }

    /// <summary>Archives each closed list (one gzipped NDJSON file) before it's deleted.</summary>
    public WincheRealtimeOptions UseArchive(IRealtimeArchive archive)
    {
        Archive = archive ?? throw new ArgumentNullException(nameof(archive));
        return this;
    }

    internal List<RuleSet> Rulesets { get; } = [];

    /// <summary>Adds rules (OR-combined with others). Without any, everything is refused.</summary>
    public WincheRealtimeOptions UseRules(RuleSet rules)
    {
        ArgumentNullException.ThrowIfNull(rules);
        Rulesets.Add(rules);
        return this;
    }

    /// <summary>Adds rules in the rules language, as in the console's editor. A syntax error throws here.</summary>
    public WincheRealtimeOptions UseRulesText(string rules)
    {
        ArgumentNullException.ThrowIfNull(rules);
        Rulesets.Add(RuleText.ParseRuleSet(rules));
        return this;
    }
}
