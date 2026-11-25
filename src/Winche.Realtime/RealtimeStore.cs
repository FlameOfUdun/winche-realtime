using System.Security.Cryptography;
using Npgsql;
using Winche.Core;
using NpgsqlTypes;

namespace Winche.Realtime;

/// <summary>
/// Lists in Postgres: <c>winche_realtime_lists</c> (one row a list: state, count, bytes) and
/// <c>winche_realtime_items</c> (the items, keyed by <c>(path, key)</c>, ordered by <c>(path, position)</c>). Positions
/// are given out under the list row's lock, so appends to one list are in one order with no gaps; appends to
/// different lists don't wait for each other. No checks of who may do what here: that's <see cref="RealtimeLists"/>'.
/// </summary>
public sealed class RealtimeStore : IAsyncDisposable
{
    public const string Lists = "winche_realtime_lists", Items = "winche_realtime_items";

    /// <summary>The Postgres channel lists' changes are announced on: <c>{last position}\n{path}</c>, after commit.</summary>
    public const string Channel = "winche_realtime";

    private static readonly string[] Schema =
    [
        $"""
        CREATE TABLE IF NOT EXISTS {Lists} (
            path text PRIMARY KEY,
            state text NOT NULL,
            count bigint NOT NULL DEFAULT 0,
            bytes bigint NOT NULL DEFAULT 0,
            opened_at timestamptz NOT NULL,
            closed_at timestamptz,
            last_at timestamptz
        );
        CREATE TABLE IF NOT EXISTS {Items} (
            path text NOT NULL,
            key text NOT NULL,
            position bigint NOT NULL,
            at timestamptz NOT NULL,
            by_uid text,
            payload bytea NOT NULL,
            PRIMARY KEY (path, key)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{Items}_path_position ON {Items} (path, position);
        """,
        // Presence: who's connected where, and the servers they're on (each checks in).
        $"""
        CREATE TABLE IF NOT EXISTS {RealtimePresence.Servers} (
            id text PRIMARY KEY,
            seen_at timestamptz NOT NULL
        );
        CREATE TABLE IF NOT EXISTS {RealtimePresence.Members} (
            connection_id text NOT NULL,
            path text NOT NULL,
            uid text,
            data jsonb,
            on_disconnect_key text,
            on_disconnect_payload bytea,
            server_id text NOT NULL,
            joined_at timestamptz NOT NULL,
            PRIMARY KEY (connection_id, path)
        );
        CREATE INDEX IF NOT EXISTS idx_{RealtimePresence.Members}_path ON {RealtimePresence.Members} (path);
        CREATE INDEX IF NOT EXISTS idx_{RealtimePresence.Members}_server ON {RealtimePresence.Members} (server_id);
        """,
        // A closed list the archive couldn't take: when it last failed (it waits before it's tried again).
        $"ALTER TABLE {Lists} ADD COLUMN IF NOT EXISTS archive_failed_at timestamptz;",
    ];

    private readonly WincheRealtimeOptions _options;
    private readonly NpgsqlDataSource _db;
    internal NpgsqlDataSource DataSource => _db;
    private volatile bool _ready;

    public RealtimeStore(WincheRealtimeOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString)) throw new ArgumentException("Winche Realtime needs a ConnectionString.", nameof(options));
        _options = options;
        _db = NpgsqlDataSource.Create(options.ConnectionString);
    }

    /// <summary>Makes (or upgrades) the tables; also done before first use.</summary>
    public async Task EnsureAsync(CancellationToken ct = default)
    {
        if (_ready) return;
        await WincheSchema.EnsureAsync(_db, "realtime", Schema, ct);
        _ready = true;
    }

    /// <summary>
    /// Appends <paramref name="items"/> to the list at <paramref name="path"/> (opening it if there's none), all or
    /// none: the positions they have, in order. A key already there with the same payload is a retry, answered with
    /// its position; with another payload, the batch is refused.
    /// </summary>
    public async Task<IReadOnlyList<long>> AppendAsync(string path, IReadOnlyList<RealtimeItemIn> items, string? by, CancellationToken ct = default)
    {
        CheckPath(path);
        CheckBatch(items);
        await EnsureAsync(ct);
        await using var conn = await _db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var list = await LockAsync(conn, tx, path, create: true, ct);
        var now = _options.Time.GetUtcNow();

        // What's there already under these keys.
        var keys = items.Select(i => i.Key).Distinct().ToArray();
        var stored = new Dictionary<string, (long Position, byte[] Payload)>(StringComparer.Ordinal);
        await using (var read = new NpgsqlCommand($"SELECT key, position, payload FROM {Items} WHERE path = $1 AND key = ANY($2)", conn, tx))
        {
            read.Parameters.Add(new() { Value = path });
            read.Parameters.Add(new() { Value = keys, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
            await using var r = await read.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct)) stored[r.GetString(0)] = (r.GetInt64(1), (byte[])r[2]);
        }

        var positions = new long[items.Count];
        var fresh = new List<(int Index, RealtimeItemIn Item)>();
        var inBatch = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            if (stored.TryGetValue(item.Key, out var had))
            {
                if (!Same(had.Payload, item.Payload))
                    throw new RealtimeException(WincheErrorCode.AlreadyExists, $"{path} already has an item {item.Key} with another payload: keys are never reused.", "key-reused");
                positions[i] = had.Position;
            }
            else if (inBatch.TryGetValue(item.Key, out var first))
            {
                if (!Same(items[first].Payload, item.Payload))
                    throw new RealtimeException(WincheErrorCode.InvalidArgument, $"The batch has {item.Key} twice with different payloads.");
                positions[i] = -first - 1;   // the same as the first one: filled in below
            }
            else
            {
                inBatch[item.Key] = i;
                fresh.Add((i, item));
            }
        }

        if (fresh.Count > 0)
        {
            if (list.State != "open")
                throw new RealtimeException(WincheErrorCode.FailedPrecondition, $"{path} is closed: it takes no more items.", "list-closed");
            var bytes = fresh.Sum(f => (long)f.Item.Payload.Length);
            if (list.Count + fresh.Count > _options.MaxListItems)
                throw new RealtimeException(WincheErrorCode.ResourceExhausted, $"{path} would hold more than {_options.MaxListItems} items.", "list-full");
            if (list.Bytes + bytes > _options.MaxListBytes)
                throw new RealtimeException(WincheErrorCode.ResourceExhausted, $"{path} would hold more than {_options.MaxListBytes} bytes.", "list-full");

            var next = list.Count;
            await using (var insert = new NpgsqlBatch(conn, tx))
            {
                foreach (var (index, item) in fresh)
                {
                    positions[index] = ++next;
                    var cmd = new NpgsqlBatchCommand($"INSERT INTO {Items} (path, key, position, at, by_uid, payload) VALUES ($1, $2, $3, $4, $5, $6)");
                    cmd.Parameters.Add(new() { Value = path });
                    cmd.Parameters.Add(new() { Value = item.Key });
                    cmd.Parameters.Add(new() { Value = next });
                    cmd.Parameters.Add(new() { Value = now });
                    cmd.Parameters.Add(new() { Value = (object?)by ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
                    cmd.Parameters.Add(new() { Value = item.Payload });
                    insert.BatchCommands.Add(cmd);
                }
                await insert.ExecuteNonQueryAsync(ct);
            }
            await using var update = new NpgsqlCommand($"UPDATE {Lists} SET count = $2, bytes = bytes + $3, last_at = $4 WHERE path = $1", conn, tx);
            update.Parameters.Add(new() { Value = path });
            update.Parameters.Add(new() { Value = next });
            update.Parameters.Add(new() { Value = bytes });
            update.Parameters.Add(new() { Value = now });
            await update.ExecuteNonQueryAsync(ct);
            await AnnounceAsync(conn, tx, path, next, ct);
        }
        for (var i = 0; i < positions.Length; i++)
            if (positions[i] < 0) positions[i] = positions[-(int)positions[i] - 1];
        await tx.CommitAsync(ct);
        return positions;
    }

    /// <summary>
    /// Up to <paramref name="limit"/> items after position <paramref name="after"/>, in order, and the list; no more
    /// than <paramref name="maxBytes"/> of payloads (always at least one item, if there is one).
    /// </summary>
    public async Task<RealtimePage> ReadAsync(string path, long after, int limit, CancellationToken ct = default) =>
        await ReadAsync(path, after, limit, long.MaxValue, ct);

    /// <inheritdoc cref="ReadAsync(string, long, int, CancellationToken)"/>
    public async Task<RealtimePage> ReadAsync(string path, long after, int limit, long maxBytes, CancellationToken ct = default)
    {
        CheckPath(path);
        await EnsureAsync(ct);
        limit = Math.Clamp(limit, 1, 1000);
        var list = await InfoAsync(path, ct);
        if (list is null) return new RealtimePage([], null);
        var items = new List<RealtimeItem>();
        // The page's items, then only those that start under the byte budget: the payloads past it never leave Postgres.
        await using var cmd = _db.CreateCommand($"""
            SELECT key, position, at, by_uid, payload FROM (
              SELECT *, sum(octet_length(payload)) OVER (ORDER BY position) - octet_length(payload) AS before
              FROM (SELECT key, position, at, by_uid, payload FROM {Items} WHERE path = $1 AND position > $2 ORDER BY position LIMIT $3) page
            ) sized WHERE before < $4
            ORDER BY position
            """);
        cmd.Parameters.Add(new() { Value = path });
        cmd.Parameters.Add(new() { Value = Math.Max(0, after) });
        cmd.Parameters.Add(new() { Value = limit });
        cmd.Parameters.Add(new() { Value = Math.Max(1, maxBytes) });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            items.Add(new RealtimeItem(r.GetString(0), r.GetInt64(1), r.GetFieldValue<DateTimeOffset>(2), r.IsDBNull(3) ? null : r.GetString(3), (byte[])r[4]));
        }
        // Positions run 1..count with no gaps: more after these if the last isn't the list's last.
        return new RealtimePage(items, list) { More = items.Count > 0 && items[^1].Position < list.Count };
    }

    /// <summary>The list at <paramref name="path"/>, or null.</summary>
    public async Task<RealtimeListInfo?> InfoAsync(string path, CancellationToken ct = default)
    {
        CheckPath(path);
        await EnsureAsync(ct);
        await using var cmd = _db.CreateCommand($"SELECT path, state, count, bytes, opened_at, closed_at FROM {Lists} WHERE path = $1");
        cmd.Parameters.Add(new() { Value = path });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct) ? Info(r) : null;
    }

    /// <summary>Opens a list at <paramref name="path"/> (one that's there already is answered as it is).</summary>
    public async Task<RealtimeListInfo> OpenAsync(string path, CancellationToken ct = default)
    {
        CheckPath(path);
        await EnsureAsync(ct);
        await using var conn = await _db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var list = await LockAsync(conn, tx, path, create: true, ct);
        await AnnounceAsync(conn, tx, path, list.Count, ct);
        await tx.CommitAsync(ct);
        return list;
    }

    /// <summary>Closes the list at <paramref name="path"/>: it takes no more items, and still reads.</summary>
    public async Task<RealtimeListInfo> CloseAsync(string path, CancellationToken ct = default) => (await CloseOnceAsync(path, ct)).List;

    /// <summary>As <see cref="CloseAsync"/>, and whether this call closed it (false: it was closed already).</summary>
    internal async Task<(RealtimeListInfo List, bool Closed)> CloseOnceAsync(string path, CancellationToken ct = default)
    {
        CheckPath(path);
        await EnsureAsync(ct);
        await using var conn = await _db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        RealtimeListInfo? list;
        await using (var cmd = new NpgsqlCommand($"""
            UPDATE {Lists} SET state = 'closed', closed_at = $2 WHERE path = $1 AND state <> 'closed'
            RETURNING path, state, count, bytes, opened_at, closed_at
            """, conn, tx))
        {
            cmd.Parameters.Add(new() { Value = path });
            cmd.Parameters.Add(new() { Value = _options.Time.GetUtcNow() });
            await using var r = await cmd.ExecuteReaderAsync(ct);
            list = await r.ReadAsync(ct) ? Info(r) : null;
        }
        if (list is null)
        {
            await tx.RollbackAsync(ct);
            // Closed already (as it was), or no list at all.
            return (await InfoAsync(path, ct) ?? throw new RealtimeException(WincheErrorCode.NotFound, $"There's no list at {path}."), false);
        }
        await AnnounceAsync(conn, tx, path, list.Count, ct);
        await tx.CommitAsync(ct);
        return (list, true);
    }

    /// <summary>Lists in path order, after <paramref name="afterPath"/>: those under <paramref name="prefix"/>, in <paramref name="state"/> (open, closed).</summary>
    public async Task<IReadOnlyList<RealtimeListInfo>> ListsAsync(string? prefix, string? state, string? afterPath, int limit, CancellationToken ct = default)
    {
        await EnsureAsync(ct);
        var lists = new List<RealtimeListInfo>();
        await using var cmd = _db.CreateCommand($"""
            SELECT path, state, count, bytes, opened_at, closed_at FROM {Lists}
            WHERE ($1::text IS NULL OR left(path, length($1)) = $1) AND ($2::text IS NULL OR state = $2) AND ($3::text IS NULL OR path > $3)
            ORDER BY path LIMIT $4
            """);
        cmd.Parameters.Add(new() { Value = (object?)(string.IsNullOrEmpty(prefix) ? null : prefix) ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        cmd.Parameters.Add(new() { Value = (object?)state ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        cmd.Parameters.Add(new() { Value = (object?)afterPath ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        cmd.Parameters.Add(new() { Value = Math.Clamp(limit, 1, 500) });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) lists.Add(Info(r));
        return lists;
    }

    /// <summary>Deletes a closed list and its items now (the console's "delete"): false when it's open or not there.</summary>
    public async Task<bool> DeleteClosedAsync(string path, CancellationToken ct = default)
    {
        CheckPath(path);
        return await DeleteAsync(path, ct);
    }

    // ── the sweep's ─────────────────────────────────────────────────────────────

    /// <summary>Closes open lists with no item (nor opening) since <paramref name="before"/>: the lists closed.</summary>
    internal async Task<IReadOnlyList<RealtimeListInfo>> CloseIdleAsync(DateTimeOffset before, int limit, CancellationToken ct)
    {
        await EnsureAsync(ct);
        var closed = new List<RealtimeListInfo>();
        await using var cmd = _db.CreateCommand($"""
            UPDATE {Lists} SET state = 'closed', closed_at = $2
            WHERE path IN (SELECT path FROM {Lists} WHERE state = 'open' AND COALESCE(last_at, opened_at) < $1 LIMIT $3 FOR UPDATE SKIP LOCKED)
            RETURNING path, state, count, bytes, opened_at, closed_at
            """);
        cmd.Parameters.Add(new() { Value = before });
        cmd.Parameters.Add(new() { Value = _options.Time.GetUtcNow() });
        cmd.Parameters.Add(new() { Value = limit });
        await using (var r = await cmd.ExecuteReaderAsync(ct))
            while (await r.ReadAsync(ct)) closed.Add(Info(r));
        foreach (var list in closed)
        {
            await using var announce = _db.CreateCommand("SELECT pg_notify($1, $2)");
            announce.Parameters.Add(new() { Value = Channel });
            announce.Parameters.Add(new() { Value = $"{list.Count}\n{list.Path}" });
            await announce.ExecuteNonQueryAsync(ct);
        }
        return closed;
    }

    /// <summary>Closed lists due to go: closed before <paramref name="before"/>, and not failing to archive since <paramref name="retryBefore"/>.</summary>
    internal async Task<IReadOnlyList<RealtimeListInfo>> ExpiredAsync(DateTimeOffset before, DateTimeOffset retryBefore, int limit, CancellationToken ct)
    {
        await EnsureAsync(ct);
        var due = new List<RealtimeListInfo>();
        await using var cmd = _db.CreateCommand($"""
            SELECT path, state, count, bytes, opened_at, closed_at FROM {Lists}
            WHERE state = 'closed' AND closed_at < $1 AND (archive_failed_at IS NULL OR archive_failed_at <= $3)
            ORDER BY closed_at LIMIT $2
            """);
        cmd.Parameters.Add(new() { Value = before });
        cmd.Parameters.Add(new() { Value = limit });
        cmd.Parameters.Add(new() { Value = retryBefore });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) due.Add(Info(r));
        return due;
    }

    /// <summary>The archive couldn't take the list at <paramref name="path"/>: it waits before it's tried again.</summary>
    internal async Task ArchiveFailedAsync(string path, DateTimeOffset at, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand($"UPDATE {Lists} SET archive_failed_at = $2 WHERE path = $1");
        cmd.Parameters.Add(new() { Value = path });
        cmd.Parameters.Add(new() { Value = at });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Deletes the list at <paramref name="path"/> and its items, if it's closed (one opened again meanwhile stays, items
    /// and all); its followers are told. Whether it was deleted.
    /// </summary>
    internal async Task<bool> DeleteAsync(string path, CancellationToken ct)
    {
        await EnsureAsync(ct);
        await using var conn = await _db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var list = new NpgsqlCommand($"DELETE FROM {Lists} WHERE path = $1 AND state = 'closed'", conn, tx))
        {
            list.Parameters.Add(new() { Value = path });
            if (await list.ExecuteNonQueryAsync(ct) == 0) return false;   // rolled back: nothing to delete
        }
        await using (var items = new NpgsqlCommand($"DELETE FROM {Items} WHERE path = $1", conn, tx))
        {
            items.Parameters.Add(new() { Value = path });
            await items.ExecuteNonQueryAsync(ct);
        }
        await AnnounceAsync(conn, tx, path, 0, ct);
        await tx.CommitAsync(ct);
        return true;
    }

    /// <summary>Announces the path's change to every server (delivered when the transaction commits).</summary>
    private static async Task AnnounceAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string path, long last, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT pg_notify($1, $2)", conn, tx);
        cmd.Parameters.Add(new() { Value = Channel });
        cmd.Parameters.Add(new() { Value = $"{last}\n{path}" });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The list row, locked for this transaction (made first when <paramref name="create"/>).</summary>
    private async Task<RealtimeListInfo> LockAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string path, bool create, CancellationToken ct)
    {
        if (create)
        {
            await using var make = new NpgsqlCommand($"INSERT INTO {Lists} (path, state, opened_at) VALUES ($1, 'open', $2) ON CONFLICT (path) DO NOTHING", conn, tx);
            make.Parameters.Add(new() { Value = path });
            make.Parameters.Add(new() { Value = _options.Time.GetUtcNow() });
            await make.ExecuteNonQueryAsync(ct);
        }
        await using var cmd = new NpgsqlCommand($"SELECT path, state, count, bytes, opened_at, closed_at FROM {Lists} WHERE path = $1 FOR UPDATE", conn, tx);
        cmd.Parameters.Add(new() { Value = path });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct) ? Info(r) : throw new RealtimeException(WincheErrorCode.NotFound, $"There's no list at {path}.");
    }

    private static RealtimeListInfo Info(NpgsqlDataReader r) =>
        new(r.GetString(0), r.GetString(1), r.GetInt64(2), r.GetInt64(3))
        {
            OpenedAt = r.GetFieldValue<DateTimeOffset>(4),
            ClosedAt = r.IsDBNull(5) ? null : r.GetFieldValue<DateTimeOffset>(5),
        };

    private static bool Same(byte[] a, byte[] b) => CryptographicOperations.FixedTimeEquals(a, b);

    /// <summary>
    /// A path: segments of letters, digits, - _ . : @, none starting with '.'. Never the words <c>channels</c> or <c>presence</c>: rules see a
    /// path's channels at <c>{path}/channels/{channel}</c> and its presence at <c>{path}/presence</c>, and no list may land there.
    /// </summary>
    internal static void CheckPath(string path)
    {
        if (string.IsNullOrEmpty(path) || path.Length > 512 || path.Split('/').Any(s => s.Length == 0 || !s.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' or ':' or '@')))
            throw new RealtimeException(WincheErrorCode.InvalidArgument, $"'{path}' isn't a path: segments of letters, digits, - _ . : @, separated by single slashes (e.g. sessions/s1).");
        if (path.Split('/').Any(s => s[0] == '.'))
            throw new RealtimeException(WincheErrorCode.InvalidArgument, $"'{path}': a segment doesn't start with '.'.");
        if (path.Split('/').Any(s => s is "channels" or "presence"))
            throw new RealtimeException(WincheErrorCode.InvalidArgument, $"'{path}': 'channels' and 'presence' aren't used in paths (rules see a path's channels at {{path}}/channels/… and its presence at {{path}}/presence).");
    }

    /// <summary>Refuses items an append would refuse (keys, sizes, how many): checked before they're kept for later.</summary>
    internal void CheckItems(IReadOnlyList<RealtimeItemIn> items) => CheckBatch(items);

    private void CheckBatch(IReadOnlyList<RealtimeItemIn> items)
    {
        if (items.Count == 0) throw new RealtimeException(WincheErrorCode.InvalidArgument, "Give at least one item.");
        if (items.Count > _options.MaxBatchItems)
            throw new RealtimeException(WincheErrorCode.InvalidArgument, $"An append carries at most {_options.MaxBatchItems} items.");
        foreach (var item in items)
        {
            if (string.IsNullOrEmpty(item.Key) || item.Key.Length > _options.MaxKeyLength)
                throw new RealtimeException(WincheErrorCode.InvalidArgument, $"Every item needs a key of 1 to {_options.MaxKeyLength} characters (e.g. device:sequence).");
            if (item.Payload is null || item.Payload.Length > _options.MaxItemBytes)
                throw new RealtimeException(WincheErrorCode.InvalidArgument, $"{item.Key}: an item's payload is at most {_options.MaxItemBytes} bytes.");
        }
    }

    public ValueTask DisposeAsync() => _db.DisposeAsync();
}
