using System.Collections.Concurrent;

namespace Winche.Realtime;

/// <summary>
/// Tells whoever waits on a path that it changed: its last position now (an append), or its state (open, closed).
/// In this process; across servers, the relay (Postgres LISTEN/NOTIFY) calls <see cref="Changed"/> too.
/// </summary>
public sealed class RealtimeNotifier
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Action<long>>> _watchers = new(StringComparer.Ordinal);

    /// <summary>Calls <paramref name="changed"/> with the path's last position each time it changes, until disposed.</summary>
    public IDisposable Watch(string path, Action<long> changed)
    {
        var id = Guid.NewGuid();
        _watchers.GetOrAdd(path, _ => new())[id] = changed;
        return new Unwatch(() =>
        {
            if (_watchers.TryGetValue(path, out var those))
            {
                those.TryRemove(id, out _);
                if (those.IsEmpty) _watchers.TryRemove(new KeyValuePair<string, ConcurrentDictionary<Guid, Action<long>>>(path, those));
            }
        });
    }

    /// <summary>The path changed: its last position is <paramref name="last"/>.</summary>
    public void Changed(string path, long last)
    {
        if (!_watchers.TryGetValue(path, out var those)) return;
        foreach (var watcher in those.Values)
        {
            try { watcher(last); }
            catch { /* a watcher's own failure is its own */ }
        }
    }

    private sealed class Unwatch(Action undo) : IDisposable
    {
        private int _done;
        public void Dispose() { if (Interlocked.Exchange(ref _done, 1) == 0) undo(); }
    }
}
