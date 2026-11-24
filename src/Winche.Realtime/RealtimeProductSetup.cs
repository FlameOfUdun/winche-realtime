using Winche.Core;

namespace Winche.Realtime;

/// <summary>Realtime as a product a host starts (Winche Server's projects): its name, and its setup (the tables).</summary>
public sealed class RealtimeProductSetup(RealtimeStore store) : IWincheProductSetup
{
    /// <summary>The product's name.</summary>
    public const string Name = "realtime";

    public string Product => Name;

    public IReadOnlyList<string> Steps { get; } = ["Making the lists' and presence tables"];

    public Task RunStepAsync(int step, CancellationToken ct) => store.EnsureAsync(ct);
}
