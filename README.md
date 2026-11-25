# Winche.Realtime

Realtime lists for Winche: ordered, append-only lists that devices write to and follow live, for data that flows
during a session (thousands of small items, needed live and after a reconnect) rather than data that's kept.

- **Ordered:** every item gets a position from the server; every reader sees the same order, with no gaps.
- **Never duplicated:** items carry the client's own key (e.g. `device:sequence`); appending a key again with the same
  payload is a retry, answered with its position. Another payload under a used key is refused.
- **Catch up from where you were:** subscribe from a position and get everything after it, then new items as they come.
- **Payloads are yours:** bytes the server never reads (JSON, a binary encoding, compressed batches).
- **Rules and limits:** who may do what, per path (default deny); limits per item, batch and list.
- **Several servers:** they share lists through Postgres; an append or close on one reaches followers on all.
- **Lifecycle:** idle lists close by themselves; closed lists are kept a while, then archived (if you give it an
  archive) and deleted.

- **Channels:** live messages at a path (telemetry a screen draws), to everyone listening on every server, never kept.
- **Presence:** who's connected at a path, the same on every server, and an item appended to the path's list when a
  connection drops without leaving.

Functions, the console and Winche Server come in later releases.

## Packages

| Package | What |
| --- | --- |
| `Winche.Realtime` | The lists (Postgres), the rules guard, the notifier. |
| `Winche.Realtime.AspNetCore` | The WebSocket. |

## Quick start

```csharp
builder.Services.AddWincheRealtime(o =>
{
    o.ConnectionString = builder.Configuration.GetConnectionString("Realtime")!;
    o.UseRulesText("""
        match sessions/{session} {
          allow get: if request.auth != null;
          allow create: if request.auth != null && request.resource.device == request.auth.uid;
          allow update: if request.auth.token.host == true;
        }
        """);
});
builder.Services.AddWincheRealtimeWsApi();
builder.Services.AddWincheCaller(…);   // how to read the caller's claims (Winche.Core.AspNetCore)

var app = builder.Build();
app.UseWebSockets();
app.UseWincheCaller();
app.MapWincheRealtimeWsApi();   // /realtime/v1/ws
```

The tables (`winche_realtime_lists`, `winche_realtime_items`) are made on first use.

## The socket

JSON frames; each request has an `id`, answered by `response { id, result }` or `error { id, error }`. Payloads are
base64. The server says `welcome { connectionId }` first.

| Request | Result |
| --- | --- |
| `append { path, items: [{ key, payload }] }` | `{ positions }`: all or none. |
| `subscribe { path, after? }` | `{ subscription, list }`, then `items { subscription, path, items }` frames from position `after`, and `state { subscription, state, count }` when the list opens or closes. |
| `unsubscribe { subscription }` | `{}` |
| `read { path, after?, limit? }` | `{ items, list }`: a page (up to 1000). |
| `open { path }` / `close { path }` | `{ list }`. A closed list takes no more items and still reads. |
| `publish { path, channel, payload }` | `{}`: to everyone listening now, on every server. Never kept. |
| `listen { path, channel }` | `{ subscription }`, then `message { subscription, path, channel, by, at, payload }` frames. Live only: a connection too slow to take them misses some. |
| `join { path, data?, onDisconnect?: { key, payload } }` | `{}`: in the path's presence, saying `data` (JSON, up to 1 KB) of yourself. |
| `leave { path }` | `{}`: out, and nothing appended. |
| `watch { path }` | `{ subscription, members }`, then `presence { subscription, path, members }` each time they change. A member is `{ uid, data, joinedAt }`. |
| `ping` | `{}` |

Items are `{ key, position, at, by, payload }`. A list is `{ path, state, count, bytes, openedAt, closedAt }`.

The socket closes with code **4401** when the caller's sign-in expires: sign in again, reconnect, and subscribe from
the last position you got. Nothing is lost.

## Rules

Per path, default deny, with the shared operations. Lists are ruled at their path; channels at
`channels/{path}/{channel}` (`create` to publish, with `request.resource.bytes`; `get` to listen); presence at
`presence/{path}` (`create` to join, `get` to watch). An on-disconnect item is checked against the list's `create` rule
when joining, so a drop never appends what the member couldn't have.

| Operation | Covers |
| --- | --- |
| `get` | `read` and `subscribe` (checked once, when the subscription starts) |
| `create` | `append` |
| `update` | `open` and `close` |

`resource` is the list as it is (`state`, `count`, `bytes`), or null when there's none. For an append,
`request.resource` is the batch: `count`, `bytes`, `keys`, `devices` (each key's part before its first `:`) and
`device` (the one device all its keys belong to, or null), so `request.resource.device == request.auth.uid` keeps one
device from appending as another. Rules never see payloads.

## Several servers

Every append, open and close announces itself on Postgres (`NOTIFY winche_realtime`, sent when it commits); each
server listens and wakes its followers, who read what's new from the database. A follower woken twice, or late,
never repeats or skips an item. Nothing to set up beyond the shared database.

## Presence across servers

Members are kept in `winche_realtime_presence`, with each server checking in every 10 seconds
(`PresenceBeatEvery`). A connection that ends leaves everywhere at once; a server that stops checking in for 30 seconds
(`PresenceTimeout`, a crash) has its members removed by the sweep, their on-disconnect items appended as them.

## Lifecycle

| Setting | Default | What |
| --- | --- | --- |
| `IdleCloseAfter` | 24 hours | An open list with no new item for this long is closed (null: never). |
| `RetainClosedFor` | 30 days | A closed list is kept (it still reads), then archived and deleted. |
| `SweepEvery` | 1 minute | How often the sweep runs: one server at a time (a Postgres advisory lock). |

`UseArchive(IRealtimeArchive)` gives the sweep somewhere to put each list before it's deleted: one gzipped NDJSON
file (an item a line, in order: `key`, `position`, `at`, `by`, `payload` in base64), named
`RealtimeArchives.FileName(path)` (`{path}/archive.ndjson.gz`) if you want a name. A list the archive can't take stays
for the next sweep; nothing is deleted unarchived. Without an archive, lists are simply deleted.

## Limits

`WincheRealtimeOptions`: `MaxItemBytes` (64 KB), `MaxBatchItems` (500), `MaxListItems` (200,000), `MaxListBytes`
(50 MB), `MaxKeyLength` (200). `RealtimeSocketOptions`: `MaxFrameBytes` (4 MB), `MaxSubscriptions` (100),
`SendQueueBytes` (8 MB), `FrameBytes` (1 MB of payloads per frame, always at least one item), `ItemsPerFrame` (500),
`SlowClientTimeout` (30 s). A slow reader slows its own subscriptions down and never loses items; one whose queue
stays full for the timeout is closed with code **4408** and reconnects, subscribing from its positions. A `read`
answers with `more: true` when the page stopped at its byte budget.

## License

Elastic License 2.0.
