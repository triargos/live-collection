# Protocol — the wire contract (`@triargos/live-collection-protocol`)

**What this is.** The shared, pure-`effect` contract kit between the frontend `live-collection`
library and *your* backend. It owns the *shapes* that cross the wire — sync-event schemas, the
sync-group routing grammar, the squasher, resync targets, branded ids, the model-registry types,
and the `/catchup` request/response schemas — and **nothing about transport**. It depends only on
`effect` (no `@effect/platform`, no I/O). The backend owns the HTTP surface (routes, methods,
status codes, errors, auth) and wires these schemas into it; the frontend decodes the same schemas
at its read-path boundary. Both ends run the **same squasher** code.

**When you use it.** You import from here when you implement a backend (you fill the typed
blanks — `ModelDescriptor`, `SyncContext`, `GroupsFor` — and decode/encode the catchup + event
schemas), or when you work on the client read path (decode SSE / catchup bodies, never cast them).
Everything here is frontend-shareable and storage-agnostic — the protocol knows nothing of DB rows,
`BIGSERIAL`, or `now()`.

This package is **LOCKED + SHIPPED**. Every symbol below exists in `packages/protocol/src/` with a
body and tests; cited as `file.ts:line`. For the client transport that consumes it see
[`read-path.md`](./read-path.md); for the backend obligations see [`backend.md`](./backend.md).

---

## Branded ids

All identity scalars are branded — minted only at boundaries (`SyncId.make(...)`, `ModelName.make(...)`)
inside mappers/decoders, never cast inside the app.

| Symbol | Schema | Notes |
|---|---|---|
| `SyncId` | `String` + pattern `/^(0\|[1-9][0-9]*)$/` + brand | a sync cursor: opaque, monotonic position in the global log, encoded as a **canonical decimal string** (no leading zeros) |
| `ModelName` | `NonEmptyString` + brand | a synced model's name, e.g. `"Webhook"` — open string on the wire, closed union per app |
| `ModelId` | `NonEmptyString` + brand | id of one entity within a model, typically a UUID |
| `UserId` | `NonEmptyString` + brand | the authenticated user a sync session belongs to |

`packages/protocol/src/ids.ts:10` (`SyncId`), `:28` (`ModelName`), `:32` (`ModelId`), `:36` (`UserId`).

### `SyncId` ordering — `compareSyncId`

A `SyncId` is a **string** so it stays exact well beyond `Number.MAX_SAFE_INTEGER` without bigints
on the wire. **Never compare `SyncId`s lexicographically or via `Number(...)`.** Order them with
`compareSyncId`, which parses each to `bigint` so magnitude comparison stays exact for very large
cursors (`ids.ts:21`):

```typescript
import { Order } from "effect"
import { compareSyncId } from "@triargos/live-collection-protocol"

// Advance a stored cursor past the next event (gap-tolerant — cursors need not be contiguous):
const advanced = Order.max(compareSyncId)(previous, next)
```

`compareSyncId: Order.Order<SyncId>` (`ids.ts:21`). This is the canonical way the client advances
its durable watermark (`lastSyncId`).

---

## Sync events — three forms, one tag vocabulary

Every sync event is one of four actions: `Insert` / `Update` / `Delete` / `Resync`. The same tag
vocabulary appears at three lifecycle stages, and **data presence is structural, not optional** —
`Insert`/`Update` carry `data`, `Delete` carries none, `Resync` carries a `target`. There is no
`Option<data>` field; absence is unrepresentable, not modeled.

| Form | Schema | Who builds it | Data |
|---|---|---|---|
| `PendingSyncEvent` | `sync-event.ts:36` | a backend producer, before persistence | no `syncId`/`createdAt` yet |
| `SyncEvent` (at rest) | `sync-event.ts:50` | persisted, reference-only | **no data on any arm** — the squasher's input |
| `HydratedSyncEvent<T>` | `sync-event.ts:76` | delivered to subscribers | `Insert`/`Update` gain typed `data: T` |

### At-rest `SyncEvent`

The persisted union — what the squasher folds. Reference-only: `(modelName, modelId, syncGroups,
syncId, createdAt)` plus the resync `target`, never entity data.

- `InsertEvent` / `UpdateEvent` / `DeleteEvent` carry `entityFields` (`modelName`, `modelId`,
  `syncGroups`) + `dbAssigned` (`syncId`, `createdAt`) — `sync-event.ts:46-48`.
- `ResyncEvent` carries `target` + `syncGroups` (the groups the resync is delivered to) + `dbAssigned`
  — `sync-event.ts:49`.
- `SyncEvent = Union(InsertEvent, UpdateEvent, DeleteEvent, ResyncEvent)` — `sync-event.ts:50`.

### Hydrated events (the wire / read path) — `data: T | null`, decode to `Option`

`HydratedSyncEvent` is a **function of the entity schema** — you pass your model's `Schema.Schema<T, I, R>`
and get back the full union for that model:

```typescript
export const HydratedSyncEvent = <T, I, R>(entity: Schema.Schema<T, I, R>) =>
  Schema.Union(
    HydratedInsert(entity),   // adds data: entity
    HydratedUpdate(entity),   // adds data: entity
    HydratedDelete,           // no data
    HydratedResync,           // target + syncGroups
  )
```

`sync-event.ts:63` (`HydratedInsert`), `:65` (`HydratedUpdate`), `:67` (`HydratedDelete`), `:68`
(`HydratedResync`), `:76` (`HydratedSyncEvent`).

> **Wire `data` is `T | null` → decode to `Option<T>` at the boundary.** On the wire an
> `Insert`/`Update` may carry an absent body (entity gone, or ACL lost at hydration time). Per
> CLAUDE.md, decode that nullable wire field into `Option<T>` at the client boundary — never let
> `null`/`undefined` flow inside the app, and never cast the wire shape. The structural arms here
> mean a `Delete` simply has *no* `data` key (`assert(!("data" in del))` is meaningless — it never
> had one); the `null`-to-`Option` conversion is the *value*-absence case at hydration.

`HydratedEntityEvent<T>` (`sync-event.ts:85`) is the same union with `Resync` separated out — what a
per-model dispatch handler receives once resync events are routed elsewhere and the `modelName` is
narrowed.

### Decoding without knowing `T` yet — `HydratedSyncEventEnvelope`

When you decode an SSE frame or a catchup page you don't yet know each event's model, so you can't
pick a per-model schema. `HydratedSyncEventEnvelope` validates the common envelope and leaves `data`
as opaque `unknown`, to be decoded later against the matching model schema at the dispatch seam:

```typescript
export const HydratedSyncEventEnvelope = HydratedSyncEvent(Schema.Unknown)
export type HydratedSyncEventEnvelope = typeof HydratedSyncEventEnvelope.Type
```

`sync-event.ts:93`. This is the schema the client read path and the playground fake backend decode
against — see the worked example below.

---

## Sync-group grammar

A **sync group** is a routing key: a `:`-delimited path of non-empty segments, e.g.
`"organization:abc"` or `"organization:abc:channel:xyz"`. Groups are **purely structural** — what
the segments *mean* (`organization`, `channel`, `user`, …) is the app's business, not the protocol's.
Events always carry **concrete, literal groups; there are no wildcards on the wire.**

`SyncGroup` is `NonEmptyString` + a `Schema.filter` that every `:`-split segment is non-empty + brand
(`sync-group.ts:11`).

### Building and parsing

```typescript
// segments → group (inverse of parseGroup):
deriveGroup(["organization", "abc"])              // SyncGroup "organization:abc"
// group → segments (inverse of deriveGroup):
parseGroup(g)                                     // { segments: ["organization", "abc"] }
```

`deriveGroup: (segments: NonEmptyReadonlyArray<string>) => SyncGroup` (`sync-group.ts:23`);
`parseGroup: (g: SyncGroup) => { readonly segments: NonEmptyReadonlyArray<string> }` (`sync-group.ts:28`).

### The two relations — `intersects` and `isUnder`

There is **no single `matches` function**; the protocol splits the two relations deliberately
(protocol DEC-4), because they have different semantics and one of them is ACL-critical.

**`intersects(a, b)` — the delivery test (ACL-critical).** Whether two group sets share at least one
group, by **exact equality, never hierarchical**. An event reaches a subscriber when the event's
groups intersect the subscriber's. Because matching is exact, a private sub-group can never leak to
members of a broader one.

```typescript
intersects(
  a: ReadonlyArray<SyncGroup>,
  b: ReadonlyArray<SyncGroup>,
): boolean
```

`sync-group.ts:40`.

**`isUnder(scope, group)` — the scope / resync-target relation.** Whether `group` lies within `scope`
by **segment-prefix, including equality** (matched per segment, not by substring):

```typescript
isUnder("organization:abc", "organization:abc")             // true  (equality)
isUnder("organization:abc", "organization:abc:channel:xyz") // true  (prefix)
isUnder("organization:abc", "organization:abcd")            // false (segment-wise, not substring)
```

`sync-group.ts:58`. Used to test which groups a resync target should clear (see the squasher), and
to expand a requested scope against a user's literal groups.

> Subscriber-side brace/alternation sugar (`org:{a,b}:channel:x`) is **deferred** to a future
> client-side builder that expands to literal scopes before transport (DEC-5). Regex as a
> subscription grammar is **rejected permanently** (ReDoS, non-finite, non-indexable).

---

## Resync targets — structural, no sentinel strings

A `Resync` event tells subscribers to discard part of their local state and re-fetch it — used when
deltas can't express a change (a permission change, a bulk correction). The blast radius is encoded
**structurally** as a tagged union — there are **no `__all` / `__group:<id>` / `__model:<Name>`
sentinel strings anywhere** (protocol DEC-9); the action is the event `_tag`, the target is a typed
value:

```typescript
export const ResyncAll   = Schema.TaggedStruct("All",   {})                   // reset everything
export const ResyncGroup = Schema.TaggedStruct("Group", { group: SyncGroup }) // reset one sync group
export const ResyncModel = Schema.TaggedStruct("Model", { model: ModelName }) // reset one model

export const ResyncTarget = Schema.Union(ResyncAll, ResyncGroup, ResyncModel)
```

`resync.ts:16-20`. Build one with the out-of-the-box smart constructor, e.g.
`ResyncGroup.make({ group })` → `{ _tag: "Group", group }`. The narrowest-to-widest ordering is
`Model` ⊂ `Group` ⊂ `All`. The squasher consumes `ResyncTarget` to drop preceding events.

---

## The squasher (`squash`) — the pure §8 fold

`squash` collapses a `syncId`-ordered list of **at-rest** `SyncEvent`s into the smallest equivalent
list, so a catching-up subscriber receives one event per entity instead of its full history. It
folds purely on event references — `(modelName, modelId, _tag, syncGroups, syncId)` — and **never
reads entity data** (there is none at rest). That data-independence is exactly why it lives in the
pure protocol and is property-tested in isolation; **both ends rely on it** (the backend squashes a
catchup page; the contract guarantees the same fold the client would compute).

```typescript
export const squash = (events: ReadonlyArray<SyncEvent>): ReadonlyArray<SyncEvent>
```

`squash.ts:73`. Input must be `syncId`-ordered; output is the minimal equivalent set, still
`syncId`-ordered.

**Per-`(modelName, modelId)` fold** (`squash.ts:22` `fold`; `:98` keying). Within one entity, a run
of changes folds to a single terminal event:

| prev \ next | `Insert` | `Update` | `Delete` |
|---|---|---|---|
| (none)   | `Insert` | `Update` | `Delete` |
| `Insert` | —        | `Insert` | **drop both** |
| `Update` | —        | `Update` | `Delete` |
| `Delete` | `Update` | —        | — |

An `Insert` then `Update` becomes one `Insert`; an `Insert` then `Delete` cancels out (the client
never knew it existed — `"Drop"`, `squash.ts:100`). The `—` cells can't occur in a well-formed stream.

**Resync overrides** (forward pass, `squash.ts:81-96`): a `Resync` drops the earlier events it
supersedes, then survives into the output —

- `All` — drop everything before it (`entities.clear()`).
- `Group(g)` — drop preceding entity events `e` where `e.syncGroups.some((sg) => isUnder(g, sg))`.
- `Model(n)` — drop preceding entity events with `modelName === n`.

**Locked, property-tested semantics:**

1. A folded entity event carries the **latest** `syncId` / `syncGroups` / `createdAt` of its run
   (via `retag`, `squash.ts:41`), so the durable cursor advances past every absorbed event.
2. `Insert→Delete` within a window drops both arms.
3. Output is `syncId`-ordered and the fold is **idempotent**: `squash(squash(xs)) === squash(xs)`.
4. **Gap-tolerant** — non-contiguous `syncId`s never break it.

The contract property (tested with `FastCheck` in `packages/protocol/test/`): for any starting
cursor `from`, applying `squash(events)` reaches the same per-entity terminal state as applying
`events` one by one.

> The `Option.none → synthetic Delete` downgrade (entity gone / ACL lost) happens in the backend's
> **hydration** step, not in `squash` — that needs I/O. The squasher stays pure.

---

## Model registry — types the backend fills

Model names are an **open branded string on the wire** (a newer backend may emit a name an older
client doesn't recognize) but a **closed union inside each app**, derived from the registry the app
builds. The single open→closed hop is `narrowModelName`.

These are **plain types with no runtime footprint** (`SyncContext`, `ModelDescriptor`, `GroupsFor`)
plus a builder and one narrowing function — the backend supplies the implementations.

### `SyncContext` — who is syncing

```typescript
export interface SyncContext {
  readonly userId: UserId
  readonly syncGroups: ReadonlyArray<SyncGroup>   // the recipient's current literal groups
}
```

`model-registry.ts:19`.

### `ModelDescriptor` — how one model is decoded and hydrated

```typescript
export interface ModelDescriptor<Name extends string, T, R> {
  readonly modelName: Name                                  // the literal, not just branded
  readonly schema: Schema.Schema<T, any, R>                 // any = the Encoded slot (a held schema)
  readonly hydrate: (id: ModelId, ctx: SyncContext) =>
    Effect.Effect<Option.Option<T>, never, R>              // Option.none ⇒ entity gone / ACL lost
  readonly hydrateMany?: (ids: ReadonlyArray<ModelId>, ctx: SyncContext) =>
    Effect.Effect<ReadonlyMap<ModelId, T>, never, R>       // optional batch — avoid /catchup N+1
}
```

`model-registry.ts:32`. The `any` in `schema` is the Encoded slot of a stored-but-not-yet-applied
schema (Effect's own idiom for holding a heterogeneous schema in a map) — **not** an IO cast, so the
"no `any`" rule (which targets IO results) doesn't apply. `hydrate` returns `Option<T>`: `none` is
how the backend signals the entity is gone or no longer visible, which becomes a synthetic
`HydratedDelete` downstream.

### `GroupsFor` — the permission resolver signature

```typescript
export type GroupsFor = (args: { readonly userId: UserId }) =>
  Effect.Effect<ReadonlyArray<SyncGroup>>
```

`model-registry.ts:46`. The backend implements this (it's the source of truth for which groups a user
may see); catchup uses it server-side (DEC-12 — see below).

### `defineModelRegistry` — keys *are* the model-name union

```typescript
export const defineModelRegistry: <const R extends Record<string, ModelDescriptor<string, any, any>>>(
  r: { [K in keyof R]: R[K] & ModelDescriptor<K & string, any, any> },
) => R
```

`model-registry.ts:55`. Each descriptor's `modelName` literal **must equal its key** — a mistyped key
or a mismatched `modelName` is a compile error. The result's keys form the app's model-name union:
`type SyncedModelName = keyof typeof registry`.

### `narrowModelName` — the one open→closed seam

```typescript
export const narrowModelName: <N extends string>(
  known: ReadonlyArray<N>,
  raw: ModelName,
) => Either.Either<N, UnknownModelError>
```

`model-registry.ts:76`. Returns `Right(name)` with the narrowed literal when the wire name is
registered, or `Left(UnknownModelError)` when it isn't. The caller **logs a warning and drops the
event** — it never fails the stream — so a client stays forward-compatible with a backend that knows
more models than it does (healed by catchup/resync). `UnknownModelError` is a `Schema.TaggedError`
carrying `modelName` (the unrecognized name) and `known` (the names this registry knows) —
`model-registry.ts:62`.

---

## The `/catchup` contract — schemas, not an HTTP API

The protocol ships the catchup **request + response schemas, and nothing else**. It does **not**
define the route, method, status codes, errors, headers, or auth — the implementing backend owns all
of that and wires these schemas into its own router (protocol DEC-7). The protocol is `effect`-only;
`@effect/platform` is **not** a dependency here. `/sync` (SSE) is likewise a backend detail and absent
from the contract.

```typescript
export const CatchupRequest = Schema.Struct({ from: SyncId })

export const CatchupResponse = Schema.Struct({
  events: Schema.Array(HydratedSyncEventEnvelope),   // data opaque (unknown); per-model decode later
  lastSyncId: SyncId,
})
```

`catchup.ts:19` (`CatchupRequest`), `:28` (`CatchupResponse`).

**No `group` parameter (DEC-12).** A client cannot narrow scope from the wire. The server resolves
the caller's full set of sync groups from their permissions (`GroupsFor`) and returns everything
visible since `from`. This keeps ACL authority on the server and the request trivial.

On the client side, `CatchupClient` (in `live-collection`) decodes the body against `CatchupResponse`
at the boundary — `packages/live-collection/src/client/catchup-client.ts:34` — and surfaces a modeled
`CatchupFailed` (a `Schema.TaggedError`, `catchup-client.ts:11`) on a non-2xx / decode failure, which
the sync loop logs and recovers from by tailing anyway. See [`read-path.md`](./read-path.md).

---

## Worked example — constructing and decoding events against the contract

The playground's cross-tab fake backend treats its localStorage event log and its `BroadcastChannel`
messages as *the wire*, so it builds and decodes everything against the protocol envelope schema —
never casting the wire shape. This is the same discipline a real backend and the real read path use.

From `examples/playground/src/live/shared-backend.ts`:

```typescript
import { Schema } from "effect"
import {
  HydratedSyncEventEnvelope, ModelId, ModelName, SyncGroup, SyncId,
} from "@triargos/live-collection-protocol"

// Brands minted once, at the boundary:
const GROUP = SyncGroup.make("playground")   // :67
const MODEL = ModelName.make("Webhook")      // :68

// Boundary codecs — the log and BroadcastChannel are the wire, so decode against the schema (:78-82):
const decodeEnvelope = Schema.decode(Schema.parseJson(HydratedSyncEventEnvelope))
const encodeEnvelope = Schema.encode(Schema.parseJson(HydratedSyncEventEnvelope))

// An Insert envelope carries typed data; a Delete carries none (structural, :84-101):
const insertEnvelope = (syncId: SyncId, w: Webhook): HydratedSyncEventEnvelope => ({
  _tag: "Insert", syncId, modelName: MODEL, modelId: ModelId.make(w.id),
  syncGroups: [GROUP], createdAt: new Date(), data: w,
})
const deleteEnvelope = (syncId: SyncId, id: ModelId): HydratedSyncEventEnvelope => ({
  _tag: "Delete", syncId, modelName: MODEL, modelId: id,
  syncGroups: [GROUP], createdAt: new Date(),   // no data key
})

// A cross-tab message is decoded at the boundary; a bad message is logged and dropped, never fatal (:238-248):
channel.onmessage = (event) => {
  const decoded = Effect.runSync(Effect.either(decodeEnvelope(String(event.data))))
  if (decoded._tag === "Left") { /* drop undecodable message */ return }
  const env = decoded.right
  // env is now a typed HydratedSyncEventEnvelope — discriminate on env._tag
}
```

The catchup endpoint in the same file builds a `CatchupResponse` directly from the schema's type —
filtering the log to events newer than the cursor and pairing them with the high-water `lastSyncId`
(`shared-backend.ts:222-234`):

```typescript
const events = log.filter((e) => Number(e.syncId) > Number(from))
const response: CatchupResponse = { events, lastSyncId: SyncId.make(String(rawSeq())) }
```

(Note: this fake backend compares with `Number(...)` because its sequence stays small; a real backend
and the client must use `compareSyncId` for cursors that can exceed `Number.MAX_SAFE_INTEGER`.)

---

## See also

- [`read-path.md`](./read-path.md) — the client read path that consumes this contract: SSE tail,
  `CatchupClient`, the `lastSyncId` watermark, and the resync handler.
- [`backend.md`](./backend.md) — the obligations on *your* backend: implementing `ModelDescriptor` /
  `GroupsFor`, the `/catchup` and `/sync` routes, hydration, and the dispatcher.
- `packages/protocol/DESIGN.md` — the decisions log (DEC-1…DEC-12) and rationale. Where this doc and
  DESIGN.md disagree on a signature, **the `src/` line citations above win.**
