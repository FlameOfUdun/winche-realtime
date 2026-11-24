# Winche.Realtime

Realtime lists for Winche: ordered, append-only lists that devices write to and follow live, for data that flows
during a session (thousands of small items, needed live and after a reconnect) rather than data that's kept.

- **Ordered:** every item gets a position from the server; every reader sees the same order, with no gaps.
- **Never duplicated:** items carry the client's own key (e.g. `device:sequence`); appending a key again with the same
  payload is a retry, answered with its position. Another payload under a used key is refused.
- **Catch up from where you were:** subscribe from a position and get everything after it, then new items as they come.
- **Payloads are yours:** bytes the server never reads (JSON, a binary encoding, compressed batches).
- **Rules and limits:** who may do what, per path (default deny); limits per item, batch and list.

Channels (live messages, never stored), presence, several servers and the console come in later releases.

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
| `ping` | `{}` |

Items are `{ key, position, at, by, payload }`. A list is `{ path, state, count, bytes, openedAt, closedAt }`.

The socket closes with code **4401** when the caller's sign-in expires: sign in again, reconnect, and subscribe from
the last position you got. Nothing is lost.

## Rules

Per path, default deny, with the shared operations:

| Operation | Covers |
| --- | --- |
| `get` | `read` and `subscribe` (checked once, when the subscription starts) |
| `create` | `append` |
| `update` | `open` and `close` |

`resource` is the list as it is (`state`, `count`, `bytes`), or null when there's none. For an append,
`request.resource` is the batch: `count`, `bytes`, `keys`, `devices` (each key's part before its first `:`) and
`device` (the one device all its keys belong to, or null), so `request.resource.device == request.auth.uid` keeps one
device from appending as another. Rules never see payloads.

## Limits

`WincheRealtimeOptions`: `MaxItemBytes` (64 KB), `MaxBatchItems` (500), `MaxListItems` (200,000), `MaxListBytes`
(50 MB), `MaxKeyLength` (200). `RealtimeSocketOptions`: `MaxFrameBytes` (4 MB), `MaxSubscriptions` (100),
`SendQueueLimit` (256), `ItemsPerFrame` (500). A slow reader slows its own subscription down; it never loses items.

## License

Elastic License 2.0.
