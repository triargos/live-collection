# The read path

> **What this is.** The read path is the forever-running fiber that keeps every mounted collection in
> sync with the server: it **catches up** from a durable cursor, then **tails** a single SSE
> connection, applying every event to the local store. You rarely call into it directly —
> [`useLiveSync`](#wiring-it-up) forks it once near your app root. Read this when you wire a custom
> transport, debug a missed event, or reason about reconnect/resync behaviour. Everything here is
> **frontend-only**; the server owns `GET /sync` and `GET /catchup` (see [./backend.md](./backend.md))
> and the wire shapes live in the protocol kit (see [./protocol.md](./protocol.md)).

The loop is one fiber draining a **merged inbox** — live SSE events and registry mount signals on the
same stream, so they never interleave. It lives in
[`packages/live-collection/src/client/sync-loop.ts`](../packages/live-collection/src/client/sync-loop.ts).
Its only typed errors are caught internally; the channel is `never` once forked. For the architecture
overview (registry, mount handle, dispatch) see [./architecture.md](./architecture.md).

---

## The loop at a glance

`syncLoop(map, onResync, options?)` returns an `Effect<void, never, CollectionRegistry | SyncTransport |
CatchupClient | LastSyncIdStore | EventLogStore>` and **runs forever** — fork it, don't await it
([`sync-loop.ts:54`](../packages/live-collection/src/client/sync-loop.ts#L54)).

One cycle:

```
each cycle (retry on SyncConnectionLost, spaced 3s):
  from = cursor ?? "0"
  resp = catchup.fetch({ from })            CatchupFailed ⇒ log + tail anyway
  applyCatchup(resp)                         Resync arm ⇒ snapshot every model
                                             else ⇒ ingest each entity event
  cursor.set(resp.lastSyncId)
  runForEach(inbox):                         merged: live SSE + registry mounts
    Live entity event ⇒ ingest               append to log, apply, advance cursor
    Live Resync       ⇒ clear cursor *> onResync *> stop the tail
    Mount signal      ⇒ onMount (skip / replay / bootstrap)
```

The cycle is wrapped in `Effect.retry({ while: SyncConnectionLost, schedule: spaced 3s })`, so a
transport drop re-enters the cycle, **re-runs catchup to heal the disconnect gap**, and reconnects.
A live `Resync` raises an internal `ResyncStop` that is caught to end the tail after
`onResync` has run ([`sync-loop.ts:278-287`](../packages/live-collection/src/client/sync-loop.ts#L278)).

This document covers catchup → tail and the transport/cursor/dispatch seams. The mount arm
(`onMount`, `decideOnMount`, the EventLog replay path) is summarized below and detailed in
[./replay-on-mount.md](./replay-on-mount.md).

---

## SyncTransport — the one SSE connection

The seam: `yield* SyncTransport`
([`sync-transport.ts:67`](../packages/live-collection/src/client/sync-transport.ts#L67)).

```ts
interface SyncTransportShape {
  readonly connect: Stream.Stream<HydratedSyncEventEnvelope, SyncConnectionLost>
}
```

`connect` is the single app-wide SSE stream to `GET /sync`, fully decoded. It hides SSE line-framing,
keep-alive timeout, text/JSON decoding, and `HydratedSyncEventEnvelope` decoding. Key behaviours, all
in the prod adapter ([`sync-transport.ts:28-64`](../packages/live-collection/src/client/sync-transport.ts#L28)):

- **Decode at the boundary, never cast.** Each `data:` line is decoded with
  `Schema.decode(Schema.parseJson(HydratedSyncEventEnvelope))`
  ([`sync-transport.ts:26`](../packages/live-collection/src/client/sync-transport.ts#L26)). The envelope
  leaves `data` as opaque `Schema.Unknown` — it is decoded per-model later at the dispatch seam, against
  the model's own schema. A **malformed line is logged and skipped, never fatal**: a newer server may
  emit shapes this client can't parse
  ([`sync-transport.ts:46-56`](../packages/live-collection/src/client/sync-transport.ts#L46)).
- **Keep-alive timeout.** Every line resets a timer via `Stream.timeoutFail(..., keepAlive)`. SSE
  servers emit `:` comment pings, so a gap longer than `keepAlive` means the connection is dead even
  though no error surfaced — it fails with `SyncConnectionLost({ reason: "keep-alive timeout" })`
  ([`sync-transport.ts:40`](../packages/live-collection/src/client/sync-transport.ts#L40)).
- **Fails on drop — does not retry internally.** A server-closed stream is still a drop, so
  the stream is concatenated with `Stream.fail(new SyncConnectionLost({ reason: "stream ended" }))`
  ([`sync-transport.ts:61`](../packages/live-collection/src/client/sync-transport.ts#L61)). The
  orchestrator's outer retry catches it, re-runs catchup, and reconnects. This is **expected, not
  exceptional** — `SyncConnectionLost` carries a `reason: string` for logs and is a `Schema.TaggedError`
  ([`sync-transport.ts:10-12`](../packages/live-collection/src/client/sync-transport.ts#L10)).

### Layers

```ts
SyncTransport.layer({ url, keepAlive })   // prod: GET {url} as SSE over the platform HttpClient
SyncTransport.layerMemory(queue)          // test: events drained from a Queue.Dequeue; closing it
                                          //       surfaces SyncConnectionLost
```

`layer` requires `HttpClient.HttpClient` in context
([`sync-transport.ts:69-72`](../packages/live-collection/src/client/sync-transport.ts#L69)). The
playground uses `layerMemory(queue)` and feeds the queue from a `BroadcastChannel`, simulating
cross-tab SSE ([shared-backend.ts:250](../examples/playground/src/live/shared-backend.ts#L250)).

---

## CatchupClient — `GET /catchup?from=`

The seam: `yield* CatchupClient`
([`catchup-client.ts:41`](../packages/live-collection/src/client/catchup-client.ts#L41)).

```ts
interface CatchupClientShape {
  readonly fetch: (request: CatchupRequest) => Effect.Effect<CatchupResponse, CatchupFailed>
}
```

One-shot. The server resolves the caller's sync groups from their permissions —
**there is no group parameter**, no client-side narrowing — squashes the missed events with the
[squasher](./protocol.md), hydrates them, and returns `{ events, lastSyncId }`
([catchup.ts:28-32](../packages/protocol/src/catchup.ts#L28)). Each event's `data` stays opaque here.

- **Decode at the boundary.** The prod adapter pins `filterStatusOk`, reads `response.json`, then
  `Schema.decodeUnknown(CatchupResponse)` — it never casts the wire shape
  ([`catchup-client.ts:27-37`](../packages/live-collection/src/client/catchup-client.ts#L27)).
- **`CatchupFailed` is modeled and recoverable.** Any non-2xx, broken connection, or decode failure
  becomes `CatchupFailed({ from, reason })`
  ([`catchup-client.ts:11-14`](../packages/live-collection/src/client/catchup-client.ts#L11)). The loop
  **catches it, logs a warning, and tails the live stream anyway** — a transient catchup miss is healed
  on the next reconnect, so the read path degrades gracefully instead of crashing
  ([`sync-loop.ts:266-273`](../packages/live-collection/src/client/sync-loop.ts#L266)).

### Layers

```ts
CatchupClient.layer({ url })       // prod: GET {url}?from= over the platform HttpClient
CatchupClient.layerMemory(canned)  // test: always returns the canned CatchupResponse
```

([`catchup-client.ts:43-47`](../packages/live-collection/src/client/catchup-client.ts#L43))

---

## LastSyncIdStore — the durable cursor

The seam: `yield* LastSyncIdStore`
([`last-sync-id-store.ts:55`](../packages/live-collection/src/client/last-sync-id-store.ts#L55)).

```ts
interface LastSyncIdStoreShape {
  readonly get: Effect.Effect<Option.Option<SyncId>>
  readonly set: (id: SyncId) => Effect.Effect<void>
  readonly clear: Effect.Effect<void>
}
```

The single, durable, **global** high-water mark of events this client has applied. It gates catchup
(`from = cursor ?? "0"`) and advances as catchup responses and live events land. It is deliberately
**ours, not the framework's `staleTime`**: `staleTime` resets on reload; a sync
cursor must not ([`last-sync-id-store.ts:4-13`](../packages/live-collection/src/client/last-sync-id-store.ts#L4)).

- **`get` is `Option`** — `None` only on a truly cold start; the loop reads it as
  `Option.getOrElse(() => SyncId.make("0"))` ([`sync-loop.ts:265`](../packages/live-collection/src/client/sync-loop.ts#L265)).
- **`set` is monotonic by `compareSyncId`.** It keeps the larger of current and incoming via
  `Order.max(compareSyncId)`, so a late, out-of-order event can never pull the cursor backwards
  ([`last-sync-id-store.ts:22-26`](../packages/live-collection/src/client/last-sync-id-store.ts#L22)).
  `compareSyncId` parses to `bigint`, so it stays exact beyond `Number.MAX_SAFE_INTEGER`
  ([ids.ts:21](../packages/protocol/src/ids.ts#L21)) — never compare `SyncId`s as strings or numbers.
- **The cursor's single source of truth is the sync stream** — `CatchupResponse.lastSyncId`
  and each live `event.syncId`. A `listFn` returns rows only; it does not move the cursor.
- **`clear`** is used only by the live-resync reload path; the next start then catches up cold from
  `"0"` and re-snapshots ([`sync-loop.ts:251-255`](../packages/live-collection/src/client/sync-loop.ts#L251)).

### Layers

```ts
LastSyncIdStore.layer        // prod (browser): a single localStorage entry, decoded against SyncId on read
LastSyncIdStore.layerMemory  // test/SSR: a Ref
```

The stored string is external input, so it is decoded against `SyncId` on read (a corrupt value reads
as `None`); storage faults are defects (`orDie`)
([`last-sync-id-store.ts:30-42`](../packages/live-collection/src/client/last-sync-id-store.ts#L30)).

---

## Dispatch — source-agnostic application

The loop has **one** application path, keyed by scope, shared by live events, catchup events, and
replayed log rows. Only the *recording* (cursor + log) is ingest-specific.

- **`applyWrite(meta, data)`** computes the collection key from `meta.scopeOf` — `globalKey(entity)` if
  the model has no scope, else `scopedKey({ entity, scope: scopeOf(data) })` — looks the instance up in
  the registry, and writes through `collection.utils.writeSynced(data)`. **If no instance is mounted for
  that scope, the event is ignored** for application — but it is still in the log for later replay
  ([`sync-loop.ts:77-88`](../packages/live-collection/src/client/sync-loop.ts#L77)).
- **`applyDelete(meta, modelId)`** fans the tombstone across *every* mounted instance of the entity via
  `registry.getByEntity` (a delete carries no data, so its scope is unknown), calling
  `collection.utils.deleteSynced(modelId)` on each
  ([`sync-loop.ts:90-97`](../packages/live-collection/src/client/sync-loop.ts#L90)).

`ingest` is the new-event path: it appends a `LoggedEvent` to the `EventLogStore`, applies it through
`applyWrite`/`applyDelete`, then `store.set(event.syncId)` to advance the cursor, periodically pruning
the log ([`sync-loop.ts:99-135`](../packages/live-collection/src/client/sync-loop.ts#L99)). The entity
`data` is decoded against the per-model schema with `Schema.decodeUnknown(meta.schema)(event.data)` at
this seam — never cast ([`sync-loop.ts:117`](../packages/live-collection/src/client/sync-loop.ts#L117)).
An **unknown model name is skipped entirely** (a newer server may emit more)
([`sync-loop.ts:102-103`](../packages/live-collection/src/client/sync-loop.ts#L102)).

> **Snapshot reconcile.** `snapshotInstance` replaces an instance's contents with a server
> `listFn`: upsert every fetched row (`writeSynced`) and delete the absent ones
> (`deleteSynced` for `currentKeys − fetchedKeys`), so a snapshot is a true replacement, not a merge
> ([`sync-loop.ts:147-158`](../packages/live-collection/src/client/sync-loop.ts#L147)).

---

## Resync — blunt and context-split

`Resync` is the server's "your view is stale, rebuild it" signal. The read path handles it **bluntly**:
the resync `target` is ignored entirely — no per-target dispose, no `groupScope` hook. The
behaviour splits on *where* the resync arrives:

- **In a catchup response** ⇒ **snapshot every mounted model in place** (`snapshotAll`) and record the
  resync syncId as `lastResyncAt`. No reload — so no loop
  ([`sync-loop.ts:192-208`](../packages/live-collection/src/client/sync-loop.ts#L192)).
- **Live in the SSE tail** ⇒ record `lastResyncAt`, `cursor.clear`, run the injected `onResync`, then
  raise `ResyncStop` to end the tail (Model A — a full reload)
  ([`sync-loop.ts:250-255`](../packages/live-collection/src/client/sync-loop.ts#L250)). `onResync` is an
  injected `Effect<void>` (prod: `reloadWindow`), keeping core framework-neutral and the reload
  assertable in tests ([live-runtime.ts:62-63](../packages/live-collection/src/runtime/live-runtime.ts#L62)).

A resync that passed **while a scope was unmounted** is caught at mount time: a single **global**
`lastResyncAt` (newest resync syncId, monotonic) forces a `Bootstrap` decision rather than a stale
replay — see [./replay-on-mount.md](./replay-on-mount.md).

---

## The merged inbox & mount arm

The inbox is `Stream.merge(registry.mounts, transport.connect)` mapped to a tagged `Inbox` union, so
mount signals and live events are drained on **one fiber** and never interleave
([`sync-loop.ts:259-262`](../packages/live-collection/src/client/sync-loop.ts#L259)). A `Mount` signal
runs `onMount(key)`, which reads freshness metadata and lets the pure `decideOnMount` pick
**skip / replay / bootstrap** from syncId positions alone
([`sync-loop.ts:211-245`](../packages/live-collection/src/client/sync-loop.ts#L211)). That replay path —
the durable `EventLogStore`, watermarks, and `decideOnMount` — is documented in
[./replay-on-mount.md](./replay-on-mount.md).

---

## Wiring it up

In React, fork the loop once near the app root with `useLiveSync` — it forks `runtime.forkLoop(map)`
on mount and `Fiber.interrupt`s it on unmount (interrupting stops the SSE loop but does **not** dispose
collections; the registry's lifetime is the app's, so a remount reuses the warm local store)
([react/src/index.ts:21-32](../packages/react/src/index.ts#L21)):

```tsx
// examples/playground/src/routes/App.tsx
import { useLiveSync } from "@triargos/live-collection-react"

function App() {
  // forks the catchup/cursor/tail fiber for the app's lifetime
  useLiveSync(pg.runtime, pg.models)
  // ...
}
```

The runtime is built with `makeLiveRuntime({ persistence, loop, onResync })`, where `loop` is the
merged transport/catchup/cursor/eventlog layer
([live-runtime.ts:40-60](../packages/live-collection/src/runtime/live-runtime.ts#L40)). The playground
composes it from the four loop seams plus the durable event log:

```ts
// examples/playground/src/live/shared-backend.ts:250
const loop = Layer.mergeAll(
  LastSyncIdStore.layerMemory,
  catchup,                          // CatchupClient.layerMemory(...)
  SyncTransport.layerMemory(queue), // queue fed from a BroadcastChannel (cross-tab SSE)
)

// examples/playground/src/live/playground.ts:49
const loopWithLog = Layer.merge(loop, EventLogStore.layer({ databaseName: `${dbName}-eventlog` }))
const runtime = makeLiveRuntime({ persistence, loop: loopWithLog, onResync: reloadWindow })
```

In production, swap the memory layers for `SyncTransport.layer({ url, keepAlive })` and
`CatchupClient.layer({ url })` (over a platform `HttpClient`) and `LastSyncIdStore.layer`
(localStorage). The registry is created synchronously in a long-lived scope and shared into the loop's
`ManagedRuntime` via `Layer.succeed(CollectionRegistry, registry)`, so dispatch writes to exactly the
instances the UI mounted ([live-runtime.ts:45-49](../packages/live-collection/src/runtime/live-runtime.ts#L45)).

---

## Tuning

`syncLoop`'s third arg is `SyncLoopOptions`, controlling EventLog retention
([`sync-loop.ts:34-39`](../packages/live-collection/src/client/sync-loop.ts#L34)):

```ts
const defaultOptions: SyncLoopOptions = {
  prune: { perModel: 1000, total: 5000, everyEvents: 100 },
}
```

Keep the newest `perModel` rows per model and `total` overall, pruning every `everyEvents` ingests.
Tune against the backend's catchup retention and your working-set size — see
[./replay-on-mount.md](./replay-on-mount.md).

---

## Not built / deferred

- **Throttled watermark flush** while mounted (an optimization to shorten reload replay).
- **Per-target resync** — kept blunt and global via `lastResyncAt`; the resync `target` is
  carried on the wire but not acted on.
- **Incremental workspace-switch bootstrap** as a first-class path (handled today via the mount arm's
  bootstrap decision; an unmounted-workspace eviction policy is deferred).
- **Offline-durable writes** — the read path persists what it sees, but the write path is online-only
  today (see the write-path docs).

> **Pin versions.** `@tanstack/db` is pinned exactly at **0.6.7** (alpha) and the browser persistence
> adapter at **0.1.11**; the persistence surface shifts between alphas. Verify any persistence call
> against the installed version before relying on it.
