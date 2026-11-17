# Winche Realtime: plan

## What it's for

Some app data is a flow, not a set of documents: what two or more devices do together during a session, as it
happens. Thousands of small items per session, read in order, needed live and after a reconnect, and worth little once
the session's outcome is saved. Winche Database is the wrong home for it: every item would pay for a document (a JSON
row with its indexes, the change feed, history, rules per write, listener matching, the device's document cache).

Winche Realtime is a separate product for that flow, with its own socket and its own storage:

- **Lists:** ordered, append-only, durable while their session lasts. Devices append items and subscribe from a
  cursor; a device that drops catches up from where it was. The building block for replicating events between devices
  (each device folds the list into its own state; the server only stores and relays).
- **Channels:** broadcast, never stored. Live telemetry a screen draws and nobody needs later.
- **Presence:** who is connected at a path, and what happens when a connection ends (an item appended, a value set).

What it isn't: a store for application state (that's Winche Database), a queue for server work (that's Winche
Functions' triggers), or a general tree of values that grows without bound.

## Model

Everything lives under a **path** (`/`-separated segments, e.g. `sessions/{id}`). A path holds at most one list, any
number of channels (named under it: `sessions/{id}#telemetry`) and a presence set.

| Thing | Where | What |
| --- | --- | --- |
| List item | table `winche_realtime_items` | `(path, key)` primary key, `position` (server-assigned, a per-path counter), `at` (server time), `by` (uid), `payload` (bytes, opaque to the server), `size` |
| List | table `winche_realtime_lists` | `path`, `state` (open / closed), `opened`, `closed`, `count`, `bytes`, `expires` |
| Presence | in memory on the server holding the socket; a heartbeat row per server (as functions do) so several servers agree | `path` → connections: `uid`, `device`, `since`, `data` (small), the on-disconnect action |
| Channel | nowhere: relayed to the subscribers connected now (across servers via Postgres `LISTEN/NOTIFY`, payload capped) | — |
| Rules | the console's rules store (subsystem `realtime`), the Winche.Rules language | versioned like the other products' rules |

- **Keys** are the client's (e.g. `{device}:{sequence}`): appending a key that exists is a no-op that answers with the
  stored item's position, so a retried append never duplicates. A different payload under an existing key is refused
  (`already-exists`), never overwritten.
- **Order** is the server's `position`, the same for every reader. Clients that order by their own clocks still can
  (the payload is theirs); the position is what a cursor counts.
- **Payloads** are bytes to the server: JSON, a binary encoding, compressed batches. Limits per item (default 64 KB),
  per list (default 50 MB / 200,000 items) and per project (settings), so nothing grows unseen.
- **Lifecycle:** a list is opened (explicitly, or by its first append), closed (explicitly, by a rule-allowed client or
  a function, or after it's been idle for a setting), then kept for a retention period (setting, e.g. 30 days) and
  deleted, or first archived as one file to Winche Storage (`realtime/{path}.ndjson.gz`) if the project asks. A closed
  list takes no appends and still reads.
- Only an index on `(path, position)` besides the key: no change feed, no history, no per-item rules.

## Socket (`/realtime/v1/ws`, per project)

Its own WebSocket, signed in like the other products (the project's trusted issuers or Winche Auth; a token refresh
doesn't drop it). JSON frames with an id per request; items' payloads base64 (a binary frame mode can come later).

| Frame | What |
| --- | --- |
| `append { path, items: [{ key, payload }] }` | Batched; all or none; answers each item's `position`. |
| `subscribe { path, after? }` | Every item after the position (all, without `after`), then new ones as they come; answers the list's state. |
| `read { path, after?, limit }` | A page, no subscription (catching up a big list in steps). |
| `close { path }` / `open { path }` | Lifecycle, if the rules allow. |
| `publish { path, channel, payload }` / `listen { path, channel }` | Channels. |
| `join { path, data?, onDisconnect? }` / `leave` / `watch { path }` | Presence. |
| `unsubscribe { id }`, `ping` | — |

Server frames: `items { path, items: [{ key, position, at, by, payload }] }`, `state { path, state, count }`,
`message { path, channel, payload, by }`, `presence { path, joined, left }`, errors per request.

Delivery: the server pushes items in position order with no gaps per subscription; after a reconnect the client
subscribes again with its last position. Slow consumers are buffered up to a limit, then told to resubscribe from
their position.

## Rules

Per path, default deny. Operations: `append`, `read` (subscribe and read), `close`, `open`, `publish`, `listen`,
`join`, `watch`. They see `request.auth`, the path captures, the list (`state`, `count`, `bytes`) and, for an append,
the batch's size (not the payloads: opaque). Checked once when a subscription starts or a batch arrives, not per item
delivered. A rule may read a database document (e.g. "a member of this session"), as storage rules can.

## Server side

- **Functions** (`Winche.Functions.Realtime`): `ctx.realtime.list(path)` to read, append and close with the server's
  rights; triggers `realtime.onClosed(path)` (build the outcome documents from the list) and `realtime.onAppended(path)`
  (batched, at least once, for live processing).
- **Console:** a Realtime section per project: open lists (path, items, size, age), a list's items (decoded when JSON),
  close / delete, presence now, the rules editor, settings (limits, retention, archive).
- **Logs:** opens, closes, refusals by rules or limits; never payloads.
- **Several servers:** appends go through Postgres (the position is assigned there), and each server relays new items
  to its own subscribers via `LISTEN/NOTIFY` (a notification per batch carries the path and the new last position;
  servers read what they need).

## Packages

- `Winche.Realtime`: the store, positions, lifecycle and retention sweep, limits, the rules guard.
- `Winche.Realtime.AspNetCore`: the socket endpoint and routing.
- `Winche.Functions.Realtime`: `ctx.realtime` and the triggers.
- Winche Server: mounts it per project (`/realtime/v1`), the console section, settings; the CLI emulator runs it.
- Dart SDK `winche_realtime`: `WincheRealtime.instance.list('sessions/s1')` with `append(items)`,
  `subscribe(after:)` (a stream of batches with positions), `read`, `close`; `channel(name)` with `publish` / `listen`;
  `presence` with `join` / `watch`. Reconnects and resubscribes from the last position it delivered. No device cache:
  an app keeps what it needs itself (a library that replicates events keeps its own log).

## Steps

1. **Lists, server:** the store (tables, keys, positions, limits, lifecycle), the socket with `append` / `subscribe`
   / `read`, rules, one server. Tests: duplicates, ordering under concurrent appends, catch-up after a drop, limits.
2. **Dart SDK, lists:** connect, append with retries, subscribe and resubscribe from the last position. Tests against
   the emulator; a chaos test (drops, duplicates, reconnects) with three clients agreeing on the order.
3. **Several servers and lifecycle:** `LISTEN/NOTIFY` relay, close / idle close, retention and archive, the sweep.
4. **Channels and presence**, in the server and the SDK.
5. **Functions** (`ctx.realtime`, triggers), **console** section, **CLI** emulator and rules deploy.
6. **README, release:** Winche.Realtime 1.0, the server, the CLI, `winche_realtime` 0.1.

## Open questions

- Binary frames (and a compact encoding for payloads) from the start, or JSON + base64 first?
- Should a list's `position` be visible to rules (e.g. "only the first 10,000 items")?
- Archive format and whether a function can read an archived list back.
- Per-device identity: let rules require that a key's device part matches the signed-in device, so one device can't
  append as another.
