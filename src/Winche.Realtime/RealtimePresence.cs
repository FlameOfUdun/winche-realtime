using System.Text.Json.Nodes;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using Winche.Rules;

namespace Winche.Realtime;

/// <summary>Someone connected at a path: who, what they said of themselves, since when.</summary>
public sealed record RealtimeMember(string? Uid, JsonObject? Data, DateTimeOffset JoinedAt);

/// <summary>
/// Presence: who is connected at a path, the same on every server (a table, with each server checking in), and what
/// happens when a connection ends without leaving: the item it gave (<c>onDisconnect</c>) is appended to the path's
/// list, as it. Rules see it at <c>presence/{path}</c>: <c>create</c> to join, <c>get</c> to watch; an on-disconnect
/// item is checked against the list's rules when joining.
/// </summary>
public sealed class RealtimePresence(RealtimeLists lists, RealtimeNode node, WincheRealtimeOptions options, ILogger? log = null)
{
    public const string Members = "winche_realtime_presence", Servers = "winche_realtime_servers";
    private readonly ILogger _log = log ?? NullLogger.Instance;
    private RealtimeStore Store => lists.Store;

    /// <summary>The notifier's key for presence at <paramref name="path"/> (never a list's: paths have no control characters).</summary>
    internal static string Key(string path) => "\u0001presence/" + path;

    /// <summary>Joins <paramref name="path"/> as the caller on <paramref name="connection"/>, if the rules allow.</summary>
    public async Task JoinAsync(IReadOnlyDictionary<string, object?>? caller, string connection, string path, JsonObject? data, RealtimeItemIn? onDisconnect,
        CancellationToken ct = default)
    {
        RealtimeStore.CheckPath(path);
        var json = data?.ToJsonString();
        if (json is not null && json.Length > options.MaxPresenceDataBytes)
            throw new RealtimeException(WincheErrorCode.InvalidArgument, $"Presence data is at most {options.MaxPresenceDataBytes} bytes of JSON.");
        await lists.RequireAsync(RuleOperation.Create, "join", $"presence/{path}", caller, null, null, ct);
        if (onDisconnect is not null) await lists.CanAppendAsync(caller, path, [onDisconnect], ct);
        await BeatAsync(ct);
        await using var cmd = Store.DataSource.CreateCommand($"""
            INSERT INTO {Members} (connection_id, path, uid, data, on_disconnect_key, on_disconnect_payload, server_id, joined_at)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)
            ON CONFLICT (connection_id, path) DO UPDATE SET data = EXCLUDED.data, on_disconnect_key = EXCLUDED.on_disconnect_key,
                on_disconnect_payload = EXCLUDED.on_disconnect_payload
            """);
        cmd.Parameters.Add(new() { Value = connection });
        cmd.Parameters.Add(new() { Value = path });
        cmd.Parameters.Add(new() { Value = (object?)WincheCaller.UidOf(caller) ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        cmd.Parameters.Add(new() { Value = (object?)json ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        cmd.Parameters.Add(new() { Value = (object?)onDisconnect?.Key ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        cmd.Parameters.Add(new() { Value = (object?)onDisconnect?.Payload ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bytea });
        cmd.Parameters.Add(new() { Value = node.Id });
        cmd.Parameters.Add(new() { Value = options.Time.GetUtcNow() });
        await cmd.ExecuteNonQueryAsync(ct);
        await AnnounceAsync(path, ct);
    }

    /// <summary>Leaves on purpose: gone, and nothing appended.</summary>
    public async Task LeaveAsync(string connection, string path, CancellationToken ct = default)
    {
        await using var cmd = Store.DataSource.CreateCommand($"DELETE FROM {Members} WHERE connection_id = $1 AND path = $2");
        cmd.Parameters.Add(new() { Value = connection });
        cmd.Parameters.Add(new() { Value = path });
        if (await cmd.ExecuteNonQueryAsync(ct) > 0) await AnnounceAsync(path, ct);
    }

    /// <summary>The connection ended: it leaves everywhere, and its on-disconnect items are appended.</summary>
    public async Task DisconnectedAsync(string connection, CancellationToken ct = default)
    {
        await using var cmd = Store.DataSource.CreateCommand($"""
            DELETE FROM {Members} WHERE connection_id = $1 RETURNING path, uid, on_disconnect_key, on_disconnect_payload
            """);
        cmd.Parameters.Add(new() { Value = connection });
        await GoneAsync(cmd, ct);
    }

    /// <summary>Who is at <paramref name="path"/>, earliest first.</summary>
    public async Task<IReadOnlyList<RealtimeMember>> MembersAsync(string path, CancellationToken ct = default)
    {
        await Store.EnsureAsync(ct);
        var members = new List<RealtimeMember>();
        await using var cmd = Store.DataSource.CreateCommand($"SELECT uid, data::text, joined_at FROM {Members} WHERE path = $1 ORDER BY joined_at, connection_id");
        cmd.Parameters.Add(new() { Value = path });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            members.Add(new RealtimeMember(r.IsDBNull(0) ? null : r.GetString(0), r.IsDBNull(1) ? null : JsonNode.Parse(r.GetString(1)) as JsonObject, r.GetFieldValue<DateTimeOffset>(2)));
        return members;
    }

    /// <summary>Whether the caller may watch who's at <paramref name="path"/>: refused if not.</summary>
    public async Task CanWatchAsync(IReadOnlyDictionary<string, object?>? caller, string path, CancellationToken ct = default)
    {
        RealtimeStore.CheckPath(path);
        await lists.RequireAsync(RuleOperation.Get, "watch", $"presence/{path}", caller, null, null, ct);
    }

    /// <summary>This server checks in: its members stay while it does.</summary>
    internal async Task BeatAsync(CancellationToken ct)
    {
        await Store.EnsureAsync(ct);
        await using var cmd = Store.DataSource.CreateCommand($"INSERT INTO {Servers} (id, seen_at) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET seen_at = EXCLUDED.seen_at");
        cmd.Parameters.Add(new() { Value = node.Id });
        cmd.Parameters.Add(new() { Value = options.Time.GetUtcNow() });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Servers that stopped checking in before <paramref name="before"/>: their members are gone (on-disconnect run). How many.</summary>
    internal async Task<int> SweepDeadServersAsync(DateTimeOffset before, CancellationToken ct)
    {
        await Store.EnsureAsync(ct);
        await using var cmd = Store.DataSource.CreateCommand($"""
            WITH dead AS (DELETE FROM {Servers} WHERE seen_at < $1 RETURNING id)
            DELETE FROM {Members} WHERE server_id IN (SELECT id FROM dead) RETURNING path, uid, on_disconnect_key, on_disconnect_payload
            """);
        cmd.Parameters.Add(new() { Value = before });
        return await GoneAsync(cmd, ct);
    }

    /// <summary>For tests: a member on another server, as it last checked in.</summary>
    internal async Task AddForTestsAsync(string path, string uid, string serverId, DateTimeOffset seenAt, RealtimeItemIn? onDisconnect)
    {
        await Store.EnsureAsync();
        await using (var server = Store.DataSource.CreateCommand($"INSERT INTO {Servers} (id, seen_at) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET seen_at = EXCLUDED.seen_at"))
        {
            server.Parameters.Add(new() { Value = serverId });
            server.Parameters.Add(new() { Value = seenAt });
            await server.ExecuteNonQueryAsync();
        }
        await using (var member = Store.DataSource.CreateCommand($"""
            INSERT INTO {Members} (connection_id, path, uid, on_disconnect_key, on_disconnect_payload, server_id, joined_at) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """))
        {
            member.Parameters.Add(new() { Value = Guid.NewGuid().ToString("N") });
            member.Parameters.Add(new() { Value = path });
            member.Parameters.Add(new() { Value = uid });
            member.Parameters.Add(new() { Value = (object?)onDisconnect?.Key ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
            member.Parameters.Add(new() { Value = (object?)onDisconnect?.Payload ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bytea });
            member.Parameters.Add(new() { Value = serverId });
            member.Parameters.Add(new() { Value = options.Time.GetUtcNow() });
            await member.ExecuteNonQueryAsync();
        }
        await AnnounceAsync(path, default);
    }

    /// <summary>Members a statement removed: their on-disconnect items appended (as them), every path told.</summary>
    private async Task<int> GoneAsync(NpgsqlCommand delete, CancellationToken ct)
    {
        await Store.EnsureAsync(ct);
        var gone = new List<(string Path, string? Uid, string? Key, byte[]? Payload)>();
        await using (var r = await delete.ExecuteReaderAsync(ct))
            while (await r.ReadAsync(ct))
                gone.Add((r.GetString(0), r.IsDBNull(1) ? null : r.GetString(1), r.IsDBNull(2) ? null : r.GetString(2), r.IsDBNull(3) ? null : (byte[])r[3]));
        foreach (var (path, uid, key, payload) in gone)
        {
            if (key is not null && payload is not null)
            {
                try
                {
                    var positions = await Store.AppendAsync(path, [new RealtimeItemIn(key, payload)], uid, ct);   // checked by the rules when they joined
                    lists.Notifier.Changed(path, positions[0]);
                }
                catch (RealtimeException e) { _log.LogInformation("Realtime couldn't append {Key} to {Path} as {Uid} left: {Why}", key, path, uid, e.Message); }
            }
        }
        foreach (var path in gone.Select(g => g.Path).Distinct()) await AnnounceAsync(path, ct);
        return gone.Count;
    }

    /// <summary>Presence at <paramref name="path"/> changed: watchers here at once, other servers through Postgres.</summary>
    private async Task AnnounceAsync(string path, CancellationToken ct)
    {
        lists.Notifier.Changed(Key(path), 0);
        await using var cmd = Store.DataSource.CreateCommand("SELECT pg_notify($1, $2)");
        cmd.Parameters.Add(new() { Value = RealtimeStore.Channel });
        cmd.Parameters.Add(new() { Value = "0\n" + Key(path) });
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>This server checks in every few seconds, so other servers keep its members.</summary>
internal sealed class RealtimeHeartbeat(RealtimePresence presence, WincheRealtimeOptions options, ILogger<RealtimeHeartbeat> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await presence.BeatAsync(stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception e) { log.LogWarning(e, "Realtime couldn't check in."); }
            try { await Task.Delay(options.PresenceBeatEvery, stoppingToken); } catch (OperationCanceledException) { return; }
        }
    }
}
