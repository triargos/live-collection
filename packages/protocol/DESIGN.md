# Bucket 0 — `@triargos/live-collection-protocol` design

> **Status: LOCKED design (pre-implementation).** Signatures + `Schema` wiring only —
> no implementation bodies until tests exist against this surface (design-first Phase 3).
> Derived from [`live-sync-system.md`](../../live-sync-system.md) §3–§13 and
> [`TASKS.md`](../../TASKS.md) Bucket 0 (0.1–0.6).

## What this package is

The shared **wire contract** between the frontend library (`live-collection`) and a
per-app backend. Pure: `effect` only, zero I/O.

**Storage-agnostic by construction.** The protocol knows nothing about DB rows, columns,
`BIGSERIAL`, or `now()`. It owns three kinds of thing, and nothing else:

1. **Pure protocol items** it fully owns — the event `Schema`s, the grammar, the squasher,
   `ResyncTarget`, branded ids.
2. **Expected interface *types*** the backend implements against — `ModelDescriptor`,
   `SyncContext`, `GroupsFor`. Pure types: **no `Tag`, no `Layer`, zero runtime cost.**
3. **The `/catchup` wire contract** — `CatchupRequest`/`CatchupResponse` `Schema`s. The *shapes*
   only; the backend defines the route, errors, and auth and wires these in (not an `HttpApi`).

Anything that is a *service implementation seam* — repository, dispatcher, event bus,
permission resolver — is **backend-only (Bucket C)** and deliberately absent here.

### Inclusion test

An item belongs in this package iff it **crosses the wire**, **both ends run it**, or it is a
**zero-cost typed blank** the backend fills. Everything below passes that test.

## Module layout

```
packages/protocol/src/
  ids.ts             branded scalars + compareSyncId
  sync-group.ts      grammar: deriveGroup / parseGroup / intersects / isUnder
  resync.ts          ResyncTarget tagged union
  sync-event.ts      PendingSyncEvent · SyncEvent (at rest) · HydratedSyncEvent<T> · envelope
  squash.ts          the pure §8 fold
  model-registry.ts  defineModelRegistry · narrowModelName · UnknownModelError
                     + expected types: ModelDescriptor · SyncContext · GroupsFor
  catchup.ts         CatchupRequest · CatchupResponse schemas (NOT the HTTP surface)
  index.ts           re-exports (single entrypoint; sideEffects:false)
```

Single entrypoint for now. A `./server` subpath (to fence the backend-facing types) is a
later `package.json`-only change if the hard wall is ever wanted — no code moves.

---

## `ids.ts` — branded scalars

```typescript
import { Order, Schema } from "effect"

// syncId is an opaque, monotonically-increasing cursor. Modeled as a canonical decimal
// STRING (no bigints — DEC-3); numeric ordering comes from compareSyncId, never lexicographic.
export const SyncId = Schema.String.pipe(
  Schema.pattern(/^(0|[1-9][0-9]*)$/),   // canonical: no leading zeros
  Schema.brand("SyncId"),
)
export type SyncId = typeof SyncId.Type

// Numeric order over the canonical form: shorter string < longer; equal length ⇒ lexicographic.
// advance cursor = Order.max(compareSyncId)(prev, next);  gap-tolerant (no contiguity assumed).
export const compareSyncId: Order.Order<SyncId>

export const ModelName = Schema.NonEmptyString.pipe(Schema.brand("ModelName"))
export type ModelName = typeof ModelName.Type

export const ModelId = Schema.NonEmptyString.pipe(Schema.brand("ModelId"))
export type ModelId = typeof ModelId.Type

export const UserId = Schema.UUID.pipe(Schema.brand("UserId"))       // SyncContext / resolver
export type UserId = typeof UserId.Type
```

> **DEC-11** No `ClientId` / echo suppression for now. `clientId` is removed from every event
> arm, from `SyncContext`, and from the HTTP contract. See the decisions log.

---

## `sync-group.ts` — the routing grammar (literal-only on the wire)

A sync group is a `:`-delimited path of non-empty segments. **Structural, app-agnostic** — the
protocol knows nothing of `organization`/`channel`/`user`; the app builds those on top
(Bucket B). **Events carry only literal groups; no wildcards on the wire** (DEC-4).

```typescript
import { Schema } from "effect"
import type { NonEmptyReadonlyArray } from "effect/Array"

// `:`-delimited, every segment non-empty, no reserved chars. App ids are UUIDs (colon-free),
// so no escaping. A SyncGroup is always literal — never a pattern.
export const SyncGroup = Schema.NonEmptyString.pipe(
  Schema.filter(/* every split(":") segment is non-empty */),
  Schema.brand("SyncGroup"),
)
export type SyncGroup = typeof SyncGroup.Type

export const deriveGroup: (segments: NonEmptyReadonlyArray<string>) => SyncGroup
export const parseGroup:  (g: SyncGroup) => { readonly segments: NonEmptyReadonlyArray<string> }

// DELIVERY filter (ACL-critical). Pure literal set overlap — NEVER hierarchical, or a private
// child group would leak to parent members. This is the spec's `sync_groups && groups`.
export const intersects: (a: ReadonlyArray<SyncGroup>, b: ReadonlyArray<SyncGroup>) => boolean

// SCOPE + resync-target relation. Segment-prefix containment, INCLUDING equality.
//   isUnder("organization:abc", "organization:abc")              === true
//   isUnder("organization:abc", "organization:abc:channel:xyz")  === true
//   isUnder("organization:abc", "organization:abcd")             === false  (segment-wise, not substring)
// Used to expand a requested scope against a user's literal perms, and to match resync targets.
export const isUnder: (scope: SyncGroup, group: SyncGroup) => boolean
```

> Subscriber-side brace/alternation sugar (`org:{a,b}:channel:x`) is **deferred** to a future
> client-side builder that expands to a list of literal scopes before transport (DEC-5). Regex
> as a subscription grammar is **rejected permanently** (ReDoS, non-finite, non-indexable).

---

## `resync.ts` — typed resync target

Structural tagged union. The wire carries the structure directly — **no sentinel strings**
(`__all`/`__group:…`) anywhere (DEC-9). Action is the event `_tag`, never a single char.

```typescript
import { Schema } from "effect"
import { ModelName } from "./ids.js"
import { SyncGroup } from "./sync-group.js"

export const ResyncAll   = Schema.TaggedStruct("All",   {})                  // the flare — reset everything
export const ResyncGroup = Schema.TaggedStruct("Group", { group: SyncGroup }) // one workspace — membership removal
export const ResyncModel = Schema.TaggedStruct("Model", { model: ModelName }) // one entity type — migration/correction

export const ResyncTarget = Schema.Union(ResyncAll, ResyncGroup, ResyncModel)
export type ResyncTarget = typeof ResyncTarget.Type
//   ResyncGroup.make({ group })  ⟶  { _tag: "Group", group }   (ootb constructor)
```

The blast-radius rationale (`All` vs `Group` vs `Model`) and the resync-vs-delivery
distinction live in §9 of the spec; the squasher (below) consumes `ResyncTarget` to drop
preceding events.

---

## `sync-event.ts` — the event family

Action is the discriminator at **both** levels (one tag vocabulary system-wide). Data presence
is **structural**: `Insert`/`Update` carry `data`; `Delete` has no `data` field; `Resync` carries
`target` (DEC-6). No `Option<data>` — absence is unrepresentable, not optional.

```typescript
import { Schema } from "effect"
import { ModelId, ModelName, SyncId } from "./ids.js"
import { SyncGroup } from "./sync-group.js"
import { ResyncTarget } from "./resync.js"

// Fields a projection supplies (syncId/createdAt are persistence-assigned, absent here).
const entityFields = {
  modelName:  ModelName,
  modelId:    ModelId,
  syncGroups: Schema.NonEmptyArray(SyncGroup),
} as const                                       // no clientId — DEC-11
const resyncFields = {
  target:     ResyncTarget,
  syncGroups: Schema.NonEmptyArray(SyncGroup),   // delivery routing (e.g. [user:bob])
} as const

// ── PENDING — what a projection constructs; the dispatcher accepts ONLY this (DEC-8) ──
export const PendingInsert = Schema.TaggedStruct("Insert", entityFields)
export const PendingUpdate = Schema.TaggedStruct("Update", entityFields)
export const PendingDelete = Schema.TaggedStruct("Delete", entityFields)
export const PendingResync = Schema.TaggedStruct("Resync", resyncFields)
export const PendingSyncEvent = Schema.Union(PendingInsert, PendingUpdate, PendingDelete, PendingResync)
export type PendingSyncEvent = typeof PendingSyncEvent.Type
//   PendingInsert.make({ modelName, modelId, syncGroups })  — ootb smart constructor

// ── AT REST — persisted; the squasher's input. Reference-only: NO data on any arm ──
const dbAssigned = { syncId: SyncId, createdAt: Schema.Date } as const
export const InsertEvent = Schema.TaggedStruct("Insert", { ...entityFields, ...dbAssigned })
export const UpdateEvent = Schema.TaggedStruct("Update", { ...entityFields, ...dbAssigned })
export const DeleteEvent = Schema.TaggedStruct("Delete", { ...entityFields, ...dbAssigned })
export const ResyncEvent = Schema.TaggedStruct("Resync", { ...resyncFields, ...dbAssigned })
export const SyncEvent = Schema.Union(InsertEvent, UpdateEvent, DeleteEvent, ResyncEvent)
export type SyncEvent = typeof SyncEvent.Type

// ── HYDRATED — the wire / read path.
//    Insert/Update gain data: T; Delete dataless; Resync carries target ──
const hydratedBase = {
  syncId:     SyncId,
  modelName:  ModelName,
  modelId:    ModelId,
  syncGroups: Schema.NonEmptyArray(SyncGroup),
  createdAt:  Schema.Date,
} as const

export const HydratedInsert = <T, R>(e: Schema.Schema<T, any, R>) =>
  Schema.TaggedStruct("Insert", { ...hydratedBase, data: e })
export const HydratedUpdate = <T, R>(e: Schema.Schema<T, any, R>) =>
  Schema.TaggedStruct("Update", { ...hydratedBase, data: e })
export const HydratedDelete = Schema.TaggedStruct("Delete", hydratedBase)             // no data
export const HydratedResync = Schema.TaggedStruct("Resync", {
  syncId: SyncId, target: ResyncTarget,
  syncGroups: Schema.NonEmptyArray(SyncGroup), createdAt: Schema.Date,
})

// Full union the transport decodes (entity arms + cross-cutting resync):
export const HydratedSyncEvent = <T, R>(e: Schema.Schema<T, any, R>) =>
  Schema.Union(HydratedInsert(e), HydratedUpdate(e), HydratedDelete, HydratedResync)

// What a per-model dispatch handler receives, after Resync split off + modelName narrowed:
export const HydratedEntityEvent = <T, R>(e: Schema.Schema<T, any, R>) =>
  Schema.Union(HydratedInsert(e), HydratedUpdate(e), HydratedDelete)

// Boundary decode where T is not yet known: validates the envelope, leaves data as JSON.
export const HydratedSyncEventEnvelope = HydratedSyncEvent(Schema.Unknown)
```

> `any` in `Schema.Schema<T, any, R>` is the schema's *Encoded* slot (Effect's own idiom for a
> schema generic), **not** an IO cast — the CLAUDE.md "no `any`" rule targets IO results, which
> this is not.

---

## `squash.ts` — the pure §8 fold (property-tested here)

Runs **server-side, before hydration**, on the at-rest union. Folds purely on
`(modelName, modelId, _tag, syncGroups, syncId)` — **never touches entity data** (there is none
at rest). This data-independence is why it lives here and is property-testable in isolation.

```typescript
import { SyncEvent } from "./sync-event.js"

// Input ordered by syncId; output is the minimal equivalent set, still syncId-ordered.
export const squash: (events: ReadonlyArray<SyncEvent>) => ReadonlyArray<SyncEvent>
```

**Per-`(modelName, modelId)` fold** (tag terms):

| prev \ next | `Insert` | `Update` | `Delete` |
|---|---|---|---|
| (none)   | `Insert` | `Update` | `Delete` |
| `Insert` | —        | `Insert` | **drop both** |
| `Update` | —        | `Update` | `Delete` |
| `Delete` | `Update` | —        | — |

**Resync overrides** (forward pass): `All` drops all preceding events; `Group(g)` drops preceding
entity events `e` where `e.syncGroups.some((sg) => isUnder(g, sg))`; `Model(n)` drops preceding
entity events with `modelName === n`. The resync event itself survives.

**Locked semantics (DEC, property-tested):**
1. A folded entity event carries the **latest** `syncId`/`syncGroups`/`createdAt` of its run, so
   the durable cursor advances past every absorbed event.
2. `Insert→Delete` within a window **drops both** (client never knew it existed).
3. Output is `syncId`-ordered and `squash(squash(xs)) === squash(xs)` (idempotent).

**Contract properties:** for any starting `from`, applying `squash(events)` reaches the same
per-entity terminal state as applying `events` one by one; non-contiguous `syncId`s never break it.

> The `Option.none → synthetic Delete` downgrade (entity gone / ACL lost) happens in the
> backend's **hydration** step, not here — it needs I/O. The squasher stays pure.

---

## `model-registry.ts` — model-name type safety + expected types

Model names are an **open branded string on the wire** (a newer backend may emit a name this
client doesn't know) but a **closed union inside each app**, derived from the registry it builds
(DEC-1, Fork A). The single open→closed hop is `narrowModelName`.

```typescript
import { Effect, Either, Option, Schema } from "effect"
import { ModelId, ModelName, SyncGroup, UserId } from "./ids.js"   // (SyncGroup re-exported via ids barrel)

// ── Expected interface TYPES (no Tag, no Layer — backend fills these blanks) ──
export interface SyncContext {
  readonly userId:     UserId
  readonly syncGroups: ReadonlyArray<SyncGroup>   // the recipient's current literal groups
}

export interface ModelDescriptor<Name extends string, T, R> {
  readonly modelName:    Name                                  // literal, not just branded
  readonly schema:       Schema.Schema<T, any, R>
  readonly hydrate:      (id: ModelId, ctx: SyncContext) => Effect.Effect<Option.Option<T>, never, R>
  readonly hydrateMany?: (ids: ReadonlyArray<ModelId>, ctx: SyncContext) =>
                           Effect.Effect<ReadonlyMap<ModelId, T>, never, R>   // /catchup N+1 elimination
}

// Permission-resolver signature (impl backend C.7):
export type GroupsFor = (args: { readonly userId: UserId }) => Effect.Effect<ReadonlyArray<SyncGroup>>

// ── Registry builder: keys ARE the model-name union; each descriptor's modelName literal must
//    equal its key (mistyped key OR mismatched modelName ⇒ compile error). ──
export const defineModelRegistry: <const R extends Record<string, ModelDescriptor<string, any, any>>>(
  r: { [K in keyof R]: R[K] & ModelDescriptor<K & string, any, any> },
) => R
//   type SyncedModelName = keyof typeof registry

// ── The one open→closed seam. Right(name) if registered; Left carries context to log + drop. ──
export class UnknownModelError extends Schema.TaggedError<UnknownModelError>()("UnknownModelError", {
  modelName: ModelName,                 // the unrecognized wire name
  known:     Schema.Array(ModelName),   // what this client's registry knows
}) {}

export const narrowModelName: <N extends string>(
  known: ReadonlyArray<N>,
  raw:   ModelName,
) => Either.Either<N, UnknownModelError>
//   Left ⇒ caller logs a warning and DROPS the event: never fails the stream, healed by
//   catchup/resync. Forward-compatible with a backend that's ahead of this client.
```

> A cross-model union keyed by `modelName` (so matching a model name narrows its `data`) is
> deliberately **not** defined here. The canonical hydrated event is `HydratedEntityEvent<T>` —
> action-discriminated (`_tag`) with structural data — and a model-name-keyed union must mirror that
> shape, not reintroduce `Option<data>`. It belongs at the client dispatch call site that needs it,
> not as a speculative type in the contract.

---

## `catchup.ts` — the `/catchup` contract (schemas, not an API)

The protocol ships the **request + response schemas** for catchup, and nothing else. It does **not**
define the HTTP surface — route, method, status codes, errors, headers, auth. The implementing
backend owns all of that and wires these schemas into its own `HttpApi`/router however it likes
(DEC-7). The protocol is `effect`-only; `@effect/platform` is **not** a dependency.

**No `group` parameter (DEC-12).** A client cannot narrow scope from the wire. The server resolves
the caller's full set of sync groups from their permissions (`GroupsFor`) and returns everything
visible since `from`. `/sync` (SSE) is likewise a backend detail and absent from the contract.

```typescript
import { Schema } from "effect"
import { SyncId } from "./ids.js"
import { HydratedSyncEventEnvelope } from "./sync-event.js"

// What a client must supply to catch up: just the durable cursor.
export const CatchupRequest = Schema.Struct({ from: SyncId })

// events carry data as validated JSON (unknown); per-entity decode happens at the dispatch seam.
export const CatchupResponse = Schema.Struct({
  events:     Schema.Array(HydratedSyncEventEnvelope),
  lastSyncId: SyncId,
})
//   backend: defines GET /catchup (or whatever) + error responses, decodes CatchupRequest,
//            encodes CatchupResponse. frontend: decodes CatchupResponse at its transport seam.
```

---

## Call graph

**Frontend (`live-collection`) — read path**

```
SSE frame / catchup JSON
  └─ decode HydratedSyncEventEnvelope            (boundary; data: unknown)
       └─ Match _tag
            ├─ "Resync"  → resync handler (A.8): interpret target via isUnder over collection scopes
            └─ Entity    → narrowModelName(registryKeys, modelName)
                              ├─ Right(name) → descriptor.schema decode data → typed HydratedEntityEvent
                              │                  └─ Match _tag → collection insert / update / delete (A.5)
                              └─ Left(err)   → logWarning(err); DROP                              (DEC-2)
  cursor: Order.max(compareSyncId) advances durable lastSyncId (A.7)
```

**Backend (`examples/server`) — implements against the contract (Bucket C)**

```
projection: PendingInsert.make({…}) → SyncEventDispatcher.dispatch   (C.4 — backend Tag, NOT in protocol)
                                          └─ repo.append → SyncEvent  (C.3)

catchup handler (C.8 — backend-defined route/errors; decodes CatchupRequest ◄── protocol):
  GroupsFor(userId)                       (C.7 resolver — backend; ALL the user's groups, DEC-12)
   → repo.query(groups via intersects)    (C.3)
   → squash(events)                        ◄── protocol
   → hydrateMany per modelName via ModelDescriptor; Option.none ⇒ synthetic HydratedDelete
   → CatchupResponse { events, lastSyncId }   (encoded against the schema ◄── protocol)
```

---

## Decisions log (load-bearing — do not re-litigate without a new reason)

- **DEC-1** Model names: open branded `ModelName` on the wire; closed union per app via
  `keyof typeof registry` (Fork A). `defineModelRegistry` enforces key ↔ `modelName`.
- **DEC-2** Unknown wire model name ⇒ `Left(UnknownModelError)` → log + **drop**, never fail the
  stream. Forward-compatible; healed by catchup/resync.
- **DEC-3** `SyncId` is an opaque canonical-decimal **string** + `compareSyncId: Order`. No bigints.
- **DEC-4** Wire grammar is **literal-only**. Delivery = `intersects` (literal, ACL-critical);
  scope/resync = `isUnder` (prefix incl. equality). No `*`/braces/regex on the wire.
- **DEC-5** Subscriber brace-expansion sugar **deferred** to a client builder; regex rejected.
- **DEC-6** Action-as-`_tag` four-arm unions (`Insert`/`Update`/`Delete`/`Resync`); `data` present
  only on `Insert`/`Update`; `Delete` dataless; data-absence is structural, not `Option`.
- **DEC-7** The protocol ships catchup **request/response schemas only — not the HTTP API**. No
  `HttpApi`/`HttpApiEndpoint`, no routes, no error/status definitions, no headers; the backend owns
  the surface and wires the schemas in. `/sync` SSE is likewise out of the contract (web-app detail).
  Consequence: `@effect/platform` is **not** a protocol dependency (`effect`-only).
- **DEC-8** No `DispatchArgs`. Projections build a complete `PendingSyncEvent` via ootb tagged
  constructors; the dispatcher accepts only that; `append` returns the persisted `SyncEvent`.
- **DEC-9** Sentinel codec (`__all`/`__group`/`__model`) and single-char `SyncAction` **removed** —
  resync is structural-only, action is `_tag`-only.
- **DEC-10** The catchup contract carries **no auth/identity** (no headers, no client id field).
  Authentication and identification are the backend's HTTP-surface concern (subsumed by DEC-7).
- **DEC-11** **No `clientId` / echo suppression — removed for now.** Dropped from every event arm
  (`Pending*`, at-rest `*Event`), the squasher, `SyncContext`, and the HTTP contract; the `ClientId`
  brand is deleted. Rationale: server-side echo suppression (skip sending a client its own events) is
  in tension with TanStack DB's optimistic-mutation reconciliation, which *expects* the synced store
  to confirm your own writes, and it withholds the authoritative server-transformed value from the
  originator. The mature alternative (Replicache-style) is to broadcast to everyone and reconcile on
  the client by a mutation/`lastMutationID` key — so if we reintroduce an id, it belongs as a
  **client-side reconciliation key**, not a server filter. Frontend-first: re-add only if testing
  proves we need it. The `HydratedSyncEvent` wire shape never carried `clientId`, so the read path is
  unchanged.
- **DEC-12** **Sync groups are resolved entirely server-side from user permissions.** Catchup takes
  no `group` parameter; a client cannot narrow scope from the wire. The server runs `GroupsFor` over
  the authenticated user and returns everything visible since `from`. Keeps ACL authority on the
  server and the request trivial; per-workspace narrowing, if ever needed, is a later additive change.
- **Storage-agnostic / seam placement** Service Tags (repository, dispatcher, bus, resolver) are
  **Bucket C**, not protocol. Protocol keeps only pure items, the `/catchup` request/response
  schemas, and the zero-cost expected types (`ModelDescriptor`/`SyncContext`/`GroupsFor`).

## Test plan (design-first Phase 3 — against this interface only)

- **`sync-group`** (`@effect/vitest`, property): `intersects` literal-overlap laws; `isUnder`
  reflexive + segment-prefix (not substring); `deriveGroup ∘ parseGroup` round-trip.
- **`sync-event`** (`@effect/vitest`): decode/encode round-trip for each arm; `_tag` discrimination;
  envelope leaves `data` opaque; `Delete` has no `data` key.
- **`squash`** (property — the hard one, §8/§12): random sequences converge to the same terminal
  state from any `from`; `syncId` gap-tolerance; idempotence; resync-override drops; latest-syncId
  carry.
- **`model-registry`**: `narrowModelName` Right/Left; `UnknownModelError` context; `defineModelRegistry`
  rejects key↔`modelName` mismatch (type-level).
- **`catchup`**: `CatchupRequest` validates the cursor and has no `group` field; `CatchupResponse`
  decodes events with opaque `data`, round-trips an empty page, and enforces envelope invariants.
```
