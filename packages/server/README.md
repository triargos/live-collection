# `@triargos/live-collection-server`

The optional backend kernel for [`@triargos/live-collection`](https://www.npmjs.com/package/@triargos/live-collection) — an Effect + TanStack DB live-sync engine for the frontend.

**This whole package is optional; the contract is two endpoints.** A live-collection backend needs exactly one catchup handler and one SSE stream, satisfying a set of documented invariants — any stack, any language, no package required. What this package offers is those invariants **enforced by code** instead of prose, for backends built on [Effect](https://effect.website) (v4).

```bash
npm install @triargos/live-collection-server @triargos/live-collection-protocol effect
```

---

## The sync model in three paragraphs

A live-collection backend keeps an append-only **event log**. Every authoritative write (insert / update / delete of a synced entity) appends one reference-only event: model name, entity id, the **sync groups** that may see it, and a server-assigned, monotonically increasing **`syncId`**. Events never carry entity data at rest — data is attached ("hydrated") at delivery time, so subscribers always receive an entity's *current* state.

Clients consume the log through two read surfaces. **Catchup** (`GET /catchup?from=<syncId>`) returns everything visible since the client's stored cursor — compacted, hydrated, plus the log's current head. The **SSE stream** tails new events live. Live delivery is best-effort; catchup is the source of truth, and clients re-run it on every reconnect.

Visibility is group-based: each event is stamped with sync groups (e.g. `user:42`, `organization:abc`), the server resolves the caller's groups from their own auth on every request, and an event is delivered when the two sets intersect — by exact match, never hierarchically. Hydration is the second, authoritative check: if the entity is gone or the caller lost access since the event was logged, the event is delivered as a **`Delete`** instead.

This package implements everything in those three paragraphs except the parts that are inherently yours: **auth, routes, storage, repos, and group resolution**.

## Why a kernel package

The correctness of the syncing client depends on backend behaviors that fail *silently on the client* when hand-rolled wrong — stale rows that never clear, cursors that freeze, optimistic writes that never confirm. The kernel makes them unbypassable:

| Invariant | Where enforced |
|---|---|
| Catchup events are compacted (one event per entity, not its history) with the exact fold the client relies on | `SyncFeed.catchup` via the protocol's `squash` |
| Entity gone / access lost at hydration time ⇒ delivered as `Delete` | `SyncFeed` (both surfaces) |
| A cursor older than retention ⇒ a single synthetic `Resync(All)` ("wipe and re-fetch"), never an error, never written to the log | `SyncFeed.catchup` |
| Log timeline resets (memory store reboot, truncation, restore) are detectable ⇒ `epoch` returned on catchup | `SyncEventStore` port + `SyncFeed.catchup` |
| One bad event is logged and skipped — never a killed stream or failed page | `SyncFeed` (both surfaces) |
| SSE silence never exceeds the client's window ⇒ keepalive comments merged in | `SyncFeed.streamEvents` |
| Writes persist first; a failed live publish logs a warning and never fails the write | `SyncDispatcher.dispatch` |
| **No echo suppression** — originating clients receive their own writes back (the client's optimistic-mutation reconciliation requires it; do not add a `clientId` filter) | `SyncDispatcher` by construction |
| Batched hydration — one lookup pass per model, not one per event | `SyncFeed` via the registry's `hydrateMany` |

## What you supply: two ports

### 1. `SyncEventStore` — the event log

```ts
interface SyncEventStoreShape {
  /** Persist a pending event, assigning its syncId and createdAt. */
  appendEvent(pending: PendingSyncEvent): Effect<SyncEvent>
  /** Events with syncId strictly greater than cursor, in order.
      Fails CursorOutOfRetentionError when the cursor predates retention. */
  listEvents(args: { cursor: SyncId }): Effect<ReadonlyArray<SyncEvent>, CursorOutOfRetentionError>
  /** The log's current head. */
  getLatestSyncId: Effect<SyncId>
  /** The log timeline's identity. Same value for the server's lifetime;
      changes only when history is destroyed/replaced. None ⇒ durable log, no epoch checking. */
  getCurrentEpoch: Effect<Option<Epoch>>
}
```

The shipped **`SyncEventStore.layerMemory`** is correct for dev, tests, and single-node demos: it never prunes, and it mints a fresh epoch per boot (a memory log *is* a new timeline, and the epoch is what lets clients holding old cursors detect that and self-heal). A durable adapter — a table with a `BIGSERIAL` cursor column, say — is yours, and is where retention, pruning, and the stored-once epoch live.

### 2. A model registry — how entities are validated and hydrated

Built with the make-pattern: `ModelRegistry.layer` takes an *effect* that yields your repos once and returns the descriptor record (checked with the protocol's `defineModelRegistry`). The registry variable **is** a layer:

```ts
import { Effect } from "effect"
import { defineModelRegistry } from "@triargos/live-collection-protocol"
import { ModelRegistry } from "@triargos/live-collection-server"

export const RegistryLayer = ModelRegistry.layer(Effect.gen(function* () {
  const todos = yield* TodoRepo               // resolve deps once, here
  return defineModelRegistry({
    Todo: {
      modelName: "Todo",                      // must equal the key — compile-checked
      schema: Todo,                           // the entity's wire schema
      hydrate: (id, syncGroups) =>            // Option.none ⇒ gone / access lost ⇒ delivered as Delete
        todos.find(TodoId.make(id)),
      hydrateMany: (ids, syncGroups) => todos.findMany(ids), // optional batch — avoids catchup N+1
    },
  })
}))
// : Layer<ModelRegistry, never, TodoRepo>    — requirements inferred from the build effect
```

`syncGroups` is the caller's current visibility set — use it when a row's visibility can change after logging. Descriptors are plain closures over the resolved repos; a `hydrate` that tries to look services up per call is a compile error.

## What the kernel provides

```ts
import {
  SyncEventStore,  // the port above + layerMemory
  SyncEventBus,    // in-process fan-out + layerMemory (swap for Redis/pg NOTIFY on multi-node)
  ModelRegistry,   // ModelRegistry.layer(buildEffect) — your registry, as a layer
  SyncDispatcher,  // dispatch(pending) — persist, then best-effort publish
  SyncFeed,        // catchup(...) + streamEvents(...) — the two client-facing surfaces
} from "@triargos/live-collection-server"
```

### Wiring

```ts
import { Layer } from "effect"

const Storage = Layer.mergeAll(
  TodoRepo.layerMemory,            // your repos
  SyncEventStore.layerMemory,      // or your durable adapter
  SyncEventBus.layerMemory,
)

export const SyncServices = Layer.merge(
  Storage,
  Layer.merge(SyncDispatcher.layer, SyncFeed.layer).pipe(
    Layer.provide(RegistryLayer),
    Layer.provide(Storage),
  ),
)
```

### The three call sites your app writes

```ts
// 1. Catchup route — you own auth; resolve the caller's groups server-side,
//    never from a client-supplied parameter.
const feed = yield* SyncFeed
return yield* feed.catchup({
  fromSyncId: query.from,
  syncGroups: groupsFor(session),
})
// → { events, lastSyncId, epoch } — encode with the protocol's CatchupResponse schema.

// 2. SSE route — streamEvents emits ready-to-send frame strings
//    ("data: <json>\n\n" plus ":ka\n\n" keepalives, default every 15s).
return HttpServerResponse.stream(
  feed.streamEvents({ syncGroups: groupsFor(session) }).pipe(Stream.encodeText),
  { contentType: "text/event-stream", headers: { "cache-control": "no-cache" } },
)

// 3. After every authoritative write:
const dispatcher = yield* SyncDispatcher
yield* dispatcher.dispatch(PendingSyncEvent.cases.Insert.make({
  modelName, modelId, syncGroups,
}))
```

That's the entire integration. Deliberately **not** in this package: HTTP (no route, method, or status-code opinions — the kernel emits plain strings and response-shaped values), storage drivers, and any auth surface (no principal type, no group-resolver signature — a `user:<id>` group is how per-user visibility is expressed, and mapping principals to groups is your design).

## Operational notes

- **Resyncs are first-class events.** To force clients to re-fetch (permission change, bulk correction), dispatch a `Resync` with a structural target — `All`, `Group({ group })`, or `Model({ model })`. On membership removal, deliver a `Group` resync to `user:<removedUserId>` so exactly that client clears its local data.
- **Multi-node:** replace `SyncEventBus.layerMemory` with your own adapter over Redis pub/sub or Postgres NOTIFY. A lost publish is safe — catchup heals it on the next reconnect.
- **Keepalive:** `streamEvents({ keepAlive })` defaults to 15 seconds; it must undercut the silence window configured in the client's transport.

## Further reading (repository docs)

- [The backend contract](https://github.com/triargos/live-collection/blob/main/docs/backend.md) — the full invariant list this package enforces, and what satisfying it by hand looks like.
- [The wire protocol](https://github.com/triargos/live-collection/blob/main/docs/protocol.md) — event schemas, the sync-group grammar, the squasher, resync targets, `SyncId`/`Epoch` semantics.
- [The pi-demo reference backend](https://github.com/triargos/live-collection/tree/main/examples/pi-demo/server) — a complete, tested consumer of this package: auth, routes, repos, registry, and layer graph.
