using Microsoft.Extensions.DependencyInjection;
using Winche.Rules;
using Winche.Rules.DependencyInjection;

namespace Winche.Realtime;

public static class ServiceCollectionExtensions
{
    /// <summary>The service key of Realtime's rules (its own engine; the console swaps its rules through it).</summary>
    public const string RulesKey = "WincheRealtime.Rules";

    /// <summary>
    /// Winche Realtime: its store (tables made on first use), rules (default deny) and notifier, and
    /// <see cref="RealtimeLists"/>, which the socket and functions use.
    /// </summary>
    public static IServiceCollection AddWincheRealtime(this IServiceCollection services, Action<WincheRealtimeOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new WincheRealtimeOptions();
        configure(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString)) throw new ArgumentException("Winche Realtime needs a ConnectionString.");
        services.AddSingleton(options);
        services.AddSingleton(new RealtimeStore(options));
        services.AddSingleton<RealtimeNotifier>();
        services.AddWincheRules(RulesKey, o => o.WithMutableRuleSetRepository(RuleSet.Merge(options.Rulesets)));
        services.AddSingleton(sp => new RealtimeLists(sp.GetRequiredService<RealtimeStore>(), sp.GetRequiredKeyedService<RuleEngine>(RulesKey),
            sp.GetRequiredService<RealtimeNotifier>()));
        return services;
    }
}
