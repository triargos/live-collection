# Protocol reference

`@triargos/live-collection-protocol` is the wire contract shared by the client and your backend: the sync-event schemas, the sync-group grammar, resync targets, branded ids, the squasher, the model-registry types, and the catchup schemas. It is pure — depends only on `effect`, no I/O, no HTTP. It defines the *shapes* that cross the wire; the transport (routes, methods, status codes, auth) is your backend's.

Decode everything that crosses the wire with these schemas; never cast.

## Branded ids

| Type | Underlying | Meaning |
|---|---|---|
| `SyncId` | decimal string | A position in the server's event log. String, so it stays exact beyond `Number.MAX_SAFE_INTEGER`. |
| `ModelName` | non-empty string | A synced model's wire name, e.g. `"Todo"`. Open on the wire, narrowed to a closed union per app. |
| `ModelId` | non-empty string | One entity's id within a model, typically a UUID. |
| `Epoch` | non-empty string | The identity of one log timeline. See [backend contract](./backend.md#invariants). |

Brands are minted at boundaries (`SyncId.make(...)` inside a decoder or mapper), never cast mid-app.

**Ordering:** compare `SyncId`s with `compareSyncId` (an `Order<SyncId>` that compares as `bigint`) — never lexically, never via `Number(...)`. Cursors are gap-tolerant; consumers order by syncId and never assume contiguity.

```ts
import { Order } from "effect"
import { compareSyncId } from "@triargos/live-collection-protocol"

const advanced = Order.max(compareSyncId)(previous, next)
```

## Sync events

Every event is one of four actions — `Insert` / `Update` / `Delete` / `Resync` — and appears in three forms across its lifecycle. All three are `Schema.TaggedUnion`s: construct arms with `cases` (e.g. `SyncEvent.cases.Delete.make({...})`) and branch with `match`/`guards`.

| Form | Stage | Carries data? |
|---|---|---|
| `PendingSyncEvent` | handed to the log by a producer, before persistence | no — and no `syncId`/`createdAt` yet |
| `SyncEvent` | at rest in the log | no — reference-only: `(modelName, modelId, syncGroups, syncId, createdAt)` |
| `HydratedSyncEvent<T>` | delivered to subscribers | `Insert`/`Update` carry `data: T`; `Delete` carries none |

Data presence is **structural**: a `Delete` has no `data` key at all, and a `Resync` carries a `target` instead. Events never store entity data at rest — data is attached at delivery time ("hydration"), so subscribers always see the entity's current state.

`HydratedSyncEvent` is a function of the entity schema: pass your model's schema, get the typed union back. When decoding a frame whose model you don't know yet — an SSE frame, a catchup page — use the envelope, which leaves `data` as `unknown` for a later per-model decode:

```ts
import { Schema } from "effect"
import { HydratedSyncEventEnvelope } from "@triargos/live-collection-protocol"

const decodeFrame = Schema.decodeEffect(Schema.fromJsonString(HydratedSyncEventEnvelope))
```

A frame that fails to decode should be logged and dropped, never fatal — that's what keeps an older client compatible with a newer server.

## Sync groups

A **sync group** is a routing key: a `:`-delimited path of non-empty segments — `"organization:abc"`, `"user:42"`. Groups are purely structural; what the segments mean is your app's business. Events carry concrete literal groups — no wildcards on the wire. Per-user visibility is just a `user:<id>` group.

```ts
deriveGroup(["organization", "abc"])  // SyncGroup "organization:abc"
parseGroup(g)                         // { segments: ["organization", "abc"] }
```

Two relations, deliberately separate:

- **`intersects(a, b)`** — the delivery test. Do two group sets share a group, by **exact equality**? Never hierarchical, so a private sub-group can never leak to members of a broader one. This is the ACL-critical check.
- **`isUnder(scope, group)`** — the containment test, by segment prefix including equality. `isUnder("org:a", "org:a:channel:x")` is true; `isUnder("org:a", "org:ab")` is false (segments, not substrings). Used to decide which groups a resync target clears.

Build groups with `deriveGroup`, not string concatenation.

## Resync targets

A `Resync` event tells subscribers to discard local state and refetch — for changes deltas can't express (a permission change, a bulk correction). The blast radius is a typed value, never a sentinel string:

```ts
export const ResyncTarget = Schema.TaggedUnion({
  All: {},                      // reset everything
  Group: { group: SyncGroup },  // reset one sync group
  Model: { model: ModelName },  // reset one model
})
```

Build one with `ResyncTarget.cases.Group.make({ group })`.

## The squasher

`squash` collapses a `syncId`-ordered list of at-rest `SyncEvent`s into the smallest equivalent list, so a catching-up client receives one event per entity instead of its full history:

```ts
export const squash = (events: ReadonlyArray<SyncEvent>) => ReadonlyArray<SyncEvent>
```

Per entity, a run of changes folds to a single terminal event:

| prev \ next | `Insert` | `Update` | `Delete` |
|---|---|---|---|
| (none) | `Insert` | `Update` | `Delete` |
| `Insert` | — | `Insert` | **drop both** |
| `Update` | — | `Update` | `Delete` |
| `Delete` | `Update` | — | — |

An `Insert` followed by a `Delete` cancels entirely — the client never needed to know. A `Resync` drops the earlier events it supersedes (`All`: everything; `Group(g)`: events whose groups fall under `g`; `Model(n)`: that model's events) and survives into the output.

Guarantees, property-tested: a folded event carries the **latest** syncId of its run (so cursors advance past everything absorbed); output stays ordered; the fold is idempotent (`squash(squash(xs)) === squash(xs)`) and gap-tolerant; and applying the squashed list from any starting cursor reaches the same state as applying the original events one by one.

Note that squashing never reads entity data — there is none at rest. The access-lost/entity-gone downgrade to `Delete` happens later, in hydration, which needs I/O.

## Catchup schemas

```ts
export const CatchupRequest = Schema.Struct({ from: SyncId })

export const CatchupResponse = Schema.Struct({
  events: Schema.Array(HydratedSyncEventEnvelope),
  lastSyncId: SyncId,
  epoch: Schema.OptionFromOptionalKey(Epoch),
})
```

Schemas only — the route, method, errors, and auth are your backend's. There is deliberately no group parameter in the request: the server resolves the caller's visibility itself. See the [backend contract](./backend.md) for the semantics.

## Model registry types

Helpers for building a backend's hydration registry. Model names are open on the wire but a closed union inside each app; these types provide the one open→closed hop.

**`ModelDescriptor`** — how one model is decoded and hydrated:

```ts
interface ModelDescriptor<Name extends string, T, R> {
  readonly modelName: Name
  readonly schema: Schema.Codec<T, any, R, R>
  /** none ⇒ entity gone or caller lost access ⇒ delivered as Delete */
  readonly hydrate: (id: ModelId, syncGroups: ReadonlyArray<SyncGroup>) =>
    Effect.Effect<Option.Option<T>, never, R>
  /** optional batch — one lookup per model instead of one per event */
  readonly hydrateMany?: (ids: ReadonlyArray<ModelId>, syncGroups: ReadonlyArray<SyncGroup>) =>
    Effect.Effect<ReadonlyMap<ModelId, T>, never, R>
}
```

`syncGroups` is the caller's *current* visibility set — hydration is the authoritative access check, since the event-level filter uses groups stamped at log time.

**`defineModelRegistry`** — builds the registry record; each descriptor's `modelName` must equal its key (compile-checked), and the keys form your app's model-name union.

**`narrowModelName(known, raw)`** — the open→closed seam: returns `Result.Success` with the narrowed literal for a registered name, `Result.Failure(UnknownModelError)` otherwise. Callers log and drop unknown models — never fail the stream — so a client stays forward-compatible with a backend that knows more models than it does.

## See also

- [Backend contract](./backend.md) — the endpoints and invariants these schemas plug into.
- [`@triargos/live-collection-server`](../packages/server/README.md) — the Effect kernel that consumes these types for you.
