# Replay-on-mount

**What this is.** The client-side machinery that heals a collection when it mounts late. In a
live-sync app, a scope (an org's webhooks, say) can be mounted *after* its events have already
streamed past on the SSE tail — the `syncLoop` logged and dropped them because nothing was listening,
while advancing the global cursor past them. Without healing, that scope renders **empty**. The
replay-on-mount path fixes this: every received event is appended to a durable local log, and on
mount a collection converges by **replaying the log** when it safely can — falling back to a network
**bootstrap** (`listFn`) only when the log can't cover the gap. The decision is made from `syncId`
positions alone (no timestamps, no arithmetic). It is entirely frontend; there is exactly one backend
obligation (see [the backend contract](#the-backend-contract) and [`./backend.md`](./backend.md)).

**How you use it.** You almost never call into this directly — `syncLoop` drives it. It surfaces in
your code when you wire the runtime (choosing `EventLogStore.layer` vs. `layerMemory`), tune prune
caps, or reason about why a remount bootstrapped instead of replayed. The pieces are in
`packages/live-collection/src/client/`.

---

## The three outcomes

`decideOnMount` returns one of three things, an enum at
[`mount-decision.ts:5`](../packages/live-collection/src/client/mount-decision.ts):

```ts
export enum MountDecision {
  Skip = "skip",        // base already complete to the cursor — do nothing
  Replay = "replay",    // base is behind, but the local log covers the gap — re-apply logged events
  Bootstrap = "bootstrap", // log can't be trusted to cover the gap — refetch via listFn
}
```

The function takes only `syncId` positions, all `Option<SyncId>` — no entity data, no clock
([`mount-decision.ts:23`](../packages/live-collection/src/client/mount-decision.ts)):

```ts
export const decideOnMount = (i: {
  readonly baseWatermark: Option.Option<SyncId>   // B_X: through which this collection's base is complete
  readonly cursor: Option.Option<SyncId>          // the global lastSyncId watermark
  readonly modelFloor: Option.Option<SyncId>      // the model's prune boundary
  readonly lastResyncAt: Option.Option<SyncId>    // newest resync the client ingested (global, monotonic)
}): MountDecision
```

The decision ladder, top to bottom
([`mount-decision.ts:29-42`](../packages/live-collection/src/client/mount-decision.ts)):

| Condition | Outcome | Why |
| --- | --- | --- |
| `baseWatermark` is `None` | `Bootstrap` | never had a base — fetch one |
| `base >= cursor` | `Skip` | base already complete to the cursor |
| a resync passed since `base` (`lastResyncAt > base`) | `Bootstrap` | the resync invalidated the local log |
| `floor > base` | `Bootstrap` | pruned past the base — the gap is no longer in the log |
| otherwise | `Replay` | the log fully covers `(base, cursor]` |

All comparisons go through the protocol's `compareSyncId` (magnitude, never subtraction — `syncId`s
are opaque *positions*, and the system is gap-tolerant). A missing `cursor` is treated as `"0"`.

**`modelFloor` is the prune boundary, not the oldest event.** `None` means *nothing has been pruned*,
so the log is complete from the start and replay is safe; `Some(f)` means events below `f` were
deleted, so replay is safe only when `f <= base`. (Treating `None` as `Bootstrap` would make the
common "caught up → a few events → remount" path always bootstrap — replay is the correct outcome.)

---

## `EventLogStore` — the durable log and its metadata

One seam owns both "what events have we received" and "how complete is each collection's base." It is
a hand-rolled `Context.Tag` + `EventLogStoreShape` + separate `make` + layers — never `Effect.Service`
([`event-log-store.ts:270`](../packages/live-collection/src/client/event-log-store.ts)):

```ts
export class EventLogStore extends Context.Tag("EventLogStore")<EventLogStore, EventLogStoreShape>() {
  static readonly layerMemory: Layer.Layer<EventLogStore>          // Ref-backed, non-durable (test/dev)
  static readonly layer: (options?: { readonly databaseName?: string }) => Layer.Layer<EventLogStore>
}                                                                  // durable IndexedDB (prod)
```

`layer` is `Layer.scoped`: it opens `databaseName ?? "live-collection-eventlog"` on acquire and calls
`db.close()` on scope-out ([`event-log-store.ts:179`, `:274`](../packages/live-collection/src/client/event-log-store.ts)).
`layerMemory` is `Ref`-backed and exists only for the loop's behavior tests — it cannot prove
durability across a reload.

### The interface

[`EventLogStoreShape`](../packages/live-collection/src/client/event-log-store.ts) at
[`event-log-store.ts:28`](../packages/live-collection/src/client/event-log-store.ts):

```ts
export interface EventLogStoreShape {
  // Append received events; upsert by syncId so catchup/tail overlap dedupes for free.
  readonly append: (rows: ReadonlyArray<LoggedEvent>) => Effect.Effect<void>

  // The replay slice for one collection — its model's events after `since` (exclusive), syncId-ordered.
  // scope None ⇒ the whole model (a global instance); Some(s) ⇒ that scope's rows PLUS the model's
  // scope-less Deletes (which fan across scopes, exactly as live dispatch does).
  readonly read: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly since: SyncId
  }) => Effect.Effect<ReadonlyArray<LoggedEvent>>

  // Trim the log: per-model keep newest `perModel`, then globally keep newest `total`.
  readonly prune: (caps: { readonly perModel: number; readonly total: number }) => Effect.Effect<void>

  // The model's prune boundary: None ⇒ nothing pruned; Some(f) ⇒ deleted below f.
  readonly floor: (modelName: ModelName) => Effect.Effect<Option.Option<SyncId>>

  // B_X — the syncId through which this collection's base is complete.
  readonly getBaseWatermark: (key: CollectionKey<unknown>) => Effect.Effect<Option.Option<SyncId>>
  readonly setBaseWatermark: (a: { readonly key: CollectionKey<unknown>; readonly at: SyncId }) => Effect.Effect<void>

  // The newest resync the client has ingested (monotonic) — invalidates replay across it.
  readonly getLastResync: Effect.Effect<Option.Option<SyncId>>
  readonly setLastResync: (at: SyncId) => Effect.Effect<void>
}
```

Every method's error channel is empty: driver faults are **defects** (the IDB adapter wraps requests
in `Effect.promise`, which dies on rejection), not modeled domain errors. That matches the repo rule —
infrastructure failures are `orDie`'d, the error channel stays for domain failures only.

`setBaseWatermark` and `setLastResync` are **monotonic** — they keep whichever `syncId` is numerically
larger ([`event-log-store.ts:55`](../packages/live-collection/src/client/event-log-store.ts)). An
under-estimated watermark is safe (it only causes extra idempotent replay); an over-estimate would be
a correctness bug, so the setters never regress.

### The log row

`LoggedEvent` is the schema-agnostic, at-rest wire form, re-decoded on replay
([`event-log-store.ts:13`](../packages/live-collection/src/client/event-log-store.ts)):

```ts
export interface LoggedEvent {
  readonly syncId: SyncId                  // PK, order, dedupe
  readonly modelName: ModelName
  readonly scope: Option.Option<string>    // the read filter; None for a global instance OR a Delete
  readonly tag: "Insert" | "Update" | "Delete"
  readonly modelId: ModelId
  readonly data: Option.Option<unknown>    // opaque wire data; the model schema re-decodes it on replay
}
```

`scope` is `None` for two distinct cases that never collide (an entity is uniformly global or scoped):
a global instance's event, and any `Delete` (a delete carries no data to derive a scope from). The
`read` filter (`scopeMatches`, [`event-log-store.ts:58`](../packages/live-collection/src/client/event-log-store.ts))
encodes the contract: a query with `scope: Some(s)` returns that scope's rows **and** every scope-less
`Delete`, because deletes fan across all scopes — exactly as live dispatch does. `data` is opaque
`unknown` on disk; replay decodes it against the model schema at the boundary
([`sync-loop.ts:138`](../packages/live-collection/src/client/sync-loop.ts)), never casting the wire
shape.

### Why the IDB index is on `modelName` alone

The IndexedDB
adapter ([`event-log-store.ts:179`](../packages/live-collection/src/client/event-log-store.ts)) keys
events by `syncId` (PK) and indexes only `modelName`. `read` and `prune` narrow with that index (or
`getAll` the cap-bounded store), then **filter, sort, and retain in memory** with `compareSyncId`. Two
facts force this:

1. **IDB orders string keys lexicographically; `syncId`s order by magnitude.** Lexicographically
   `"10" < "2"`, so a `syncId` range scan or range delete on the key would be wrong. Never range-scan
   the key — the in-memory sort is the only correct ordering. (The browser test asserts exactly this:
   `["1","2","10"]`, not the string-sorted `["1","10","2"]` —
   [`event-log-store.browser.test.ts:67`](../examples/playground/test/event-log-store.browser.test.ts).)
2. **A `[modelName, scope]` compound index drops scope-less `Delete`s.** A `null` value in an indexed
   key path makes the record un-indexed, so a scoped `read` against the compound index would silently
   miss the deletes it must include. The single-column index keeps them.

Rows round-trip through a `StoredEvent` schema that flattens `Option ⇄ string|null` / `unknown|null`
at the seam ([`event-log-store.ts:127`](../packages/live-collection/src/client/event-log-store.ts)).
Watermarks, `lastResyncAt`, and per-model floors live in a sibling keyval `META` store in the **same**
IDB database, separate from the OPFS collection DB (cross-store eventual consistency is already
accepted).

---

## `prunePlan` — the retention policy

`prunePlan` is a pure function so any adapter (memory, IndexedDB) shares one retention policy. It is
internal to the package (not re-exported from the barrel); both adapters call it
([`prune-plan.ts:18`](../packages/live-collection/src/client/prune-plan.ts)):

```ts
export const prunePlan = (args: {
  readonly rows: ReadonlyArray<LoggedEvent>
  readonly perModel: number   // keep the newest `perModel` events of every model
  readonly total: number      // then globally keep at most `total`
}): PrunePlan

export interface PrunePlan {
  readonly keep: ReadonlyArray<LoggedEvent>
  readonly deletedHighWater: ReadonlyMap<string, SyncId>  // highest syncId deleted per model
}
```

Two caps, denominated in **events (size), not wall-clock** — that's why there is no
`createdAt` on a row:

- **`perModel`** — per-model isolation. A chatty model can't evict a quiet model's gap.
- **`total`** — a global backstop. If the per-model survivors still exceed `total`, the oldest across
  all models are trimmed ([`prune-plan.ts:39`](../packages/live-collection/src/client/prune-plan.ts)).

### The floor = the prune boundary

`deletedHighWater` is the highest `syncId` deleted per model. The adapter **merges it monotonically**
into that model's `floor` ([`event-log-store.ts:94`](../packages/live-collection/src/client/event-log-store.ts)
for memory, [`:244`](../packages/live-collection/src/client/event-log-store.ts) for IDB). The floor is
precisely the boundary `decideOnMount` consults: events above it remain complete, so replay is safe
iff `floor <= base`. This is the single line that keeps the size cap from silently corrupting a base.

The loop calls `prune` every `everyEvents` ingests; the defaults are
`{ perModel: 1000, total: 5000, everyEvents: 100 }`
([`sync-loop.ts:39`](../packages/live-collection/src/client/sync-loop.ts)).

---

## The three invariants

Correctness rests on these (named in code):

1. **Idempotency.** Application is key-addressed upsert/delete (`writeSynced` / `deleteSynced` by
   `ModelId`) in `syncId` order. Replaying a dominated event is a no-op, so an under-estimated
   `baseWatermark` only re-applies events the collection already has. Replay flows through the **same**
   `applyWrite` / `applyDelete` path as live ingest and catchup — it never touches the
   optimistic-mutation handlers ([`sync-loop.ts:138`](../packages/live-collection/src/client/sync-loop.ts)).
2. **Floor-guard.** Never `Replay` when `floor > base` — `Bootstrap` instead
   ([`mount-decision.ts:37`](../packages/live-collection/src/client/mount-decision.ts)). The one line
   that keeps the prune cap from corrupting the base.
3. **Cursor-completeness.** The cursor never advances past terminal state the client hasn't received:
   catchup delivers the visible terminal set up to `lastSyncId` (else a `Resync`), the tail is
   in-order, and `ingest` appends to the log *before* routing/applying
   ([`sync-loop.ts:114`, `:126`](../packages/live-collection/src/client/sync-loop.ts)). Floor-guard +
   completeness together make replay hole-proof.

---

## How the loop uses it

You don't call `decideOnMount` or `EventLogStore.read` yourself — `syncLoop`'s `onMount` does, on the
single merged inbox (transport tail ⊕ `registry.mounts`), so replay and live ingest never interleave.
The three arms ([`sync-loop.ts:225-244`](../packages/live-collection/src/client/sync-loop.ts)):

- **Skip** — return; the base is already complete.
- **Replay** — `read({ modelName, scope: key.scope, since: base })`, apply each row through
  `replayRow`, then `setBaseWatermark({ key, at: cursor })`.
- **Bootstrap** — look the instance up in the registry, `snapshotInstance` it from `listFn`, then
  `setBaseWatermark({ key, at: cursor })`.

The base watermark is written **once per mount, at `onMount` completion** — no dispose-writes, no
per-event writes, no flush. A mounted instance that rode a catchup is marked complete to
`lastSyncId` so its later mount skips instead of re-bootstrapping (which an empty `listFn` would turn
into a wipe — [`sync-loop.ts:178`](../packages/live-collection/src/client/sync-loop.ts)).

See [`./architecture.md`](./architecture.md) for the loop, registry, and factory as a whole.

---

## Worked example — wiring the runtime and proving the heal

From the playground's browser test
([`replay-on-mount.browser.test.ts`](../examples/playground/test/replay-on-mount.browser.test.ts)).
The durable IDB `EventLogStore.layer` is merged into the loop's layers exactly like any other seam:

```ts
const layers = Layer.mergeAll(
  LastSyncIdStore.layerMemory,
  CatchupClient.layerMemory({ events: [], lastSyncId: SyncId.make("0") }),
  SyncTransport.layerMemory(queue),
  EventLogStore.layer({ databaseName }),   // durable IndexedDB — survives reload / workspace-switch
  Layer.succeed(CollectionRegistry, registry),
)
```

The test scripts the discriminating scenario — mount cold, unmount, seed three "remote" inserts onto
the tail **while unmounted**, then mount again — and asserts the heal comes from the durable log with
**no further `listFn`** (the `listFn` returns `[]`, so a regression to "bootstrap on every mount" would
surface 0 rows and bump the call count):

```ts
// 3. Push three remote inserts WHILE UNMOUNTED; wait until they're durably in the local log.
yield* seed(); yield* seed(); yield* seed()
yield* waitUntilE(
  log.read({ modelName: MODEL, scope: Option.some(LAB), since: SyncId.make("0") })
     .pipe(Effect.map((rows) => rows.length >= 3)),
  "seeds logged while unmounted",
)

// 4. Mount again → it must fill from the durable log (Replay), NOT refetch (Bootstrap).
const second = webhooks(LAB)
yield* Effect.promise(() => second.preload())
yield* waitUntil(() => rowCount(second) >= 3, "remount replays the 3 logged events")

assert.strictEqual(rowCount(second), 3)               // healed from the durable IndexedDB log
assert.strictEqual(listCalls(), networkBefore)        // and with NO further listFn ⇒ replay, not bootstrap
```

The `EventLogStore.layer` browser test
([`event-log-store.browser.test.ts`](../examples/playground/test/event-log-store.browser.test.ts))
separately proves, against real IndexedDB, that append dedupes by `syncId`, that a scoped `read`
includes scope-less `Delete`s, that ordering is by magnitude not lexicographic, and that the floor,
watermarks, and `lastResync` each survive a fresh layer scope over the same database (the "reload").

> Versions shift under you. `@tanstack/db` is pinned at `0.6.7` (alpha) and the OPFS persistence
> adapter at `0.1.11`; the persistence surface is unstable — **verify signatures against the installed
> version** before relying on them. `EventLogStore` itself is plain IndexedDB and has no such
> dependency.

---

## The backend contract

Replay is frontend-only, but it rests on **one** server obligation (full wire contract in
[`./protocol.md`](./protocol.md) and [`./backend.md`](./backend.md)):

> A `/catchup` request whose `from` is **below the server's retention floor MUST return a `Resync`**,
> never silently-incomplete deltas.

Otherwise cursor-completeness breaks and replay would apply over a hole. Retention is two independent
axes: the **client** caps (`perModel` / `total`, in events, bounding local replay) and **server**
retention (wall-clock, bounding the offline gap) — and the latter surfaces to the client *only* as a
`Resync`. The client never computes a cursor's age. A resync ingested while a scope was unmounted
bumps the global `lastResyncAt`, which forces that scope to `Bootstrap` on its next mount;
resync events are not themselves appended to the log.

---

## Not built (deferred)

These are intentionally absent today:

- **Throttled flush of `baseWatermark` while mounted** — would shorten replay after a reload. Today
  the watermark is written only at `onMount` completion.
- **Registry eviction backstop** — collections stay resident until an explicit `dispose*`; there is no
  idle/LRU eviction.
- **Per-target resync** — resync stays blunt and global via `lastResyncAt` rather than per `SyncGroup`.
- **Offline-durable writes** and the unmounted-workspace policy — separate workstreams, not part
  of this path.
