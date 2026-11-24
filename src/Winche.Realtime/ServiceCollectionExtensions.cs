using Microsoft.Extensions.DependencyInjection;
using Winche.Core;
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
        if (options.SettingsEnabled)
        {
            var area = Winche.Core.ServiceCollectionExtensions.AddWincheSettingsArea(services, RealtimeSettings.Area, "Realtime", RealtimeSettings.From(options), s => s.Problems());
            area.Changed += s => s.ApplyTo(options);
        }
        services.AddSingleton(new RealtimeStore(options));
        services.AddSingleton<RealtimeNotifier>();
        services.AddWincheRules(RulesKey, o => o.WithMutableRuleSetRepository(RuleSet.Merge(options.Rulesets)));
        services.AddSingleton(sp => new RealtimeLists(sp.GetRequiredService<RealtimeStore>(), sp.GetRequiredKeyedService<RuleEngine>(RulesKey),
            sp.GetRequiredService<RealtimeNotifier>()));
        services.AddSingleton<RealtimeNode>();
        services.AddSingleton(sp => new RealtimeChannels(sp.GetRequiredService<RealtimeLists>(), sp.GetRequiredService<RealtimeNode>(), options));
        services.AddSingleton(sp => new RealtimePresence(sp.GetRequiredService<RealtimeLists>(), sp.GetRequiredService<RealtimeNode>(), options,
            sp.GetService<Microsoft.Extensions.Logging.ILoggerFactory>()?.CreateLogger("Winche.Realtime.Presence")));
        services.AddSingleton(sp => new RealtimeSweeper(sp.GetRequiredService<RealtimeStore>(), options, sp.GetRequiredService<RealtimeNotifier>(),
            sp.GetService<Microsoft.Extensions.Logging.ILoggerFactory>()?.CreateLogger("Winche.Realtime.Sweep"), sp.GetRequiredService<RealtimePresence>()));
        // Several servers share lists, messages and presence through Postgres; each checks in; one sweeps at a time.
        // On a host whose products are started (Winche Server), once Realtime is; otherwise at once.
        services.AddSingleton<IWincheProductSetup, RealtimeProductSetup>();
        services.AddWincheProductService<RealtimeRelay>(RealtimeProductSetup.Name);
        services.AddWincheProductService<RealtimeHeartbeat>(RealtimeProductSetup.Name);
        services.AddWincheProductService<RealtimeSweepService>(RealtimeProductSetup.Name);
        return services;
    }
}
