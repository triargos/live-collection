# Implementing a backend for `@triargos/live-collection`

This library is **frontend-only**. It ships no server. The authoritative backend lives in *your* app, and this document is the spec of **obligations** that backend must satisfy for the frontend read path (`GET /sync`, `GET /catchup`) and write path to work. It is a contract, not code you can import — except for the one piece you *do* import: [`@triargos/live-collection-protocol`](./protocol.md), the contract kit of schemas, branded ids, the sync-group grammar, the resync targets, and the **squasher**, which both ends rely on bit-for-bit.

> **What your backend must provide.** An append-only `sync_events` store (the global ordered log), a `SyncEventBus` fan-out seam, a thin dispatcher that appends a row then best-effort publishes it, a permission resolver that maps a user to the sync groups they may currently see, and two HTTP surfaces: `GET /catchup?from=` (one-shot, squashed + hydrated backfill since a cursor) and `GET /sync` (long-lived SSE, live hydrated events). Resolve a caller's groups **server-side from their permissions** on every request — never trust groups the client sends. Hydration is yours (the model registry); the wire shapes and the squash are the protocol kit's.

> **Two deliberate contract choices worth calling out:**
> - **No `clientId` / no echo suppression.** Echo suppression conflicts with TanStack DB's optimistic-mutation reconciliation, which expects the synced store to confirm the client's own writes. There is no `clientId` filter and no `X-Client-Id` header; no `clientId` on events, `SyncContext`, or the wire. Do not implement a `client_id != :clientId` `WHERE` clause.
> - **Resync targets are structural, not sentinel strings.** A resync is a first-class `Resync` event arm with a `target` — the kit's `ResyncTarget` union: `ResyncAll` / `ResyncGroup({group})` / `ResyncModel({model})` (`packages/protocol/src/resync.ts:16-20`) — not an entity event with a magic `modelName` like `'__all'` or `'__model:Webhook'`.

---

## The data model — `sync_events`

One append-only table is the only schema this system adds. It is the global ordered log; there is **no `data` column** — events at rest are reference-only and `data` is attached at read time by hydration.

```sql
CREATE TABLE sync_events (
  sync_id      BIGSERIAL PRIMARY KEY,          -- the global cursor / watermark
  model_name   TEXT      NOT NULL,
  model_id     TEXT      NOT NULL,
  action       CHAR(1)   NOT NULL CHECK (action IN ('I','U','D','R')),
  sync_groups  TEXT[]    NOT NULL,
  target       JSONB,                          -- only for R: the ResyncTarget
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_sync_events_groups ON sync_events USING GIN (sync_groups);
CREATE INDEX idx_sync_events_lookup ON sync_events (model_name, model_id, sync_id);
```

The two indexes cover the two access patterns the read path needs:

- **Query-by-groups** — `sync_groups && :groups`, for catchup backfill (the GIN index).
- **Query-by-syncId** — `sync_id > :from ORDER BY sync_id`, the cursor scan; plus `(model_name, model_id, sync_id)` for per-entity history.

`sync_id` is your **watermark**: monotonically increasing, but **not contiguous** — `BIGSERIAL` leaves gaps on rollback. Clients order by it and tolerate gaps; never assume `n+1`. Encode it on the wire as the protocol's `SyncId` — a canonical decimal string compared with `compareSyncId`, never lexically or via `Number(...)` (`packages/protocol/src/ids.ts:10-21`).

The `action` column maps to the protocol's tag vocabulary `Insert` / `Update` / `Delete` / `Resync` (`packages/protocol/src/sync-event.ts:36-51`). Mint the branded ids (`SyncId`, `ModelName`, `ModelId`) **only at this boundary** — decoding a DB row is exactly where `unknown` becomes a domain value. Inside the app the rows already carry branded types; never re-mint or cast them downstream.

---

## `SyncEventBus` — the fan-out seam

The bus is the pub/sub primitive: a producer publishes one persisted event, every live `/sync` connection receives it, and each connection filters by its own groups before forwarding. Start with an in-memory Effect `PubSub`; the seam lets you swap in Redis pub/sub or Postgres `LISTEN`/`NOTIFY` for multi-node later without changing call sites.

Model it as a `Context.Tag` + `interface …Shape` + separate `make` + `.layer` — **never `Effect.Service`**.

```typescript
import { Context, Effect, Layer, PubSub, Queue, Scope } from "effect"
import type { SyncEvent } from "@triargos/live-collection-protocol"

interface SyncEventBusShape {
  readonly publish: (event: SyncEvent) => Effect.Effect<void>
  readonly subscribe: () => Effect.Effect<Queue.Dequeue<SyncEvent>, never, Scope.Scope>
}

class SyncEventBus extends Context.Tag("SyncEventBus")<SyncEventBus, SyncEventBusShape>() {
  static readonly layer = Layer.scoped(
    SyncEventBus,
    Effect.gen(function* () {
      const hub = yield* PubSub.unbounded<SyncEvent>()
      return {
        publish: (event) => PubSub.publish(hub, event).pipe(Effect.asVoid),
        subscribe: () => PubSub.subscribe(hub) // scoped: closes with the connection
      } satisfies SyncEventBusShape
    })
  )
}
```

A single broadcast channel; every subscriber sees every event. Group filtering is **per-connection**, downstream of the bus — the bus does not know about ACLs. `subscribe` returns a scoped `Dequeue`, so disposing a connection's `Scope` tears the subscription down.

---

## The dispatcher — append, then best-effort publish

The dispatcher is the one shared write path every producer (your domain-event projections) calls. It is **intentionally thin**: append the row (the DB assigns `syncId` + `createdAt`), then publish on the bus. The DB row is authoritative; **bus publish is best-effort** — a missed live delivery heals on the next catchup, so a publish failure is logged, not propagated.

```typescript
import { Context, Effect, Layer } from "effect"
import type { PendingSyncEvent, SyncEvent } from "@triargos/live-collection-protocol"

interface SyncEventDispatcherShape {
  readonly dispatch: (event: PendingSyncEvent) => Effect.Effect<SyncEvent>
}

class SyncEventDispatcher extends Context.Tag("SyncEventDispatcher")<
  SyncEventDispatcher,
  SyncEventDispatcherShape
>() {
  static readonly layer = Layer.effect(
    SyncEventDispatcher,
    Effect.gen(function* () {
      const repo = yield* SyncEventRepository
      const bus = yield* SyncEventBus
      return {
        dispatch: (event) =>
          Effect.gen(function* () {
            const persisted = yield* repo.append(event) // DB fills syncId + createdAt
            yield* bus.publish(persisted).pipe(
              Effect.catchAll((cause) =>
                // Best-effort: the row is durable, catchup heals missed live deliveries.
                Effect.logWarning("sync bus publish failed").pipe(Effect.annotateLogs({ cause }))
              )
            )
            return persisted
          })
      } satisfies SyncEventDispatcherShape
    })
  )
}
```

The producer hands the dispatcher a `PendingSyncEvent` — the protocol's pre-persistence form, which carries `modelName` / `modelId` / `syncGroups` (or `target` for a resync) but **no** `syncId` / `createdAt` (`packages/protocol/src/sync-event.ts:31-42`). `repo.append` returns the at-rest `SyncEvent` with those filled. Group derivation lives in the projection that builds the `PendingSyncEvent`, from domain-event fields only — never fetched inside the dispatcher.

### `SyncEventRepository` obligations

The repo is the store seam. Three operations:

- **`append(event: PendingSyncEvent): Effect<SyncEvent>`** — insert one row, return it with `syncId` + `createdAt`.
- **query-by-groups** — events whose `sync_groups` intersect a caller's resolved groups, since a cursor (catchup).
- **query-by-syncId** — events with `sync_id > :from ORDER BY sync_id` (the cursor scan; also per-entity history for debugging).

DB-driver failures are **defects**, not domain errors — `Effect.orDie` them so the error channel stays limited to modeled domain failures. The only place this seam yields an `unknown` is row decoding, which is a boundary: decode each row through the protocol schemas and mint the branded ids there.

---

## The permission resolver — `groupsFor({userId})`

A single service that, given a user, returns the set of sync groups they may **currently** see — resolved dynamically against the source of truth (org memberships, channel rosters, team memberships, whatever drives access). It is the only place that knows how to derive groups from permissions, and it is called from both `/sync` (at connect and on refresh) and `/catchup` (every call).

The protocol kit gives you the exact signature to fill — `GroupsFor` (`packages/protocol/src/model-registry.ts:46-48`):

```typescript
import type { GroupsFor } from "@triargos/live-collection-protocol"
import { deriveGroup } from "@triargos/live-collection-protocol"

// GroupsFor = (args: { userId: UserId }) => Effect.Effect<ReadonlyArray<SyncGroup>>
const groupsFor: GroupsFor = ({ userId }) =>
  Effect.gen(function* () {
    const orgIds = yield* OrganizationRepository.orgIdsFor(userId)
    const channels = yield* ChannelRepository.channelsFor(userId)
    return [
      deriveGroup(["user", userId]),
      ...orgIds.map((id) => deriveGroup(["organization", id])),
      ...channels.map((c) => deriveGroup(["organization", c.orgId, "channel", c.channelId]))
    ]
  })
```

Build groups with `deriveGroup(segments)`, not string concatenation — it produces a validated branded `SyncGroup` whose every `:`-delimited segment is non-empty (`packages/protocol/src/sync-group.ts:11-25`). The `args` object is a single object parameter (the kit's signature), so the caller can't transpose anything.

This resolver is **load-bearing for security**: it is the *server's* notion of what the caller may see. The client never narrows or supplies groups — see the next section.

---

## `GET /catchup?from=` — squashed, hydrated backfill

One-shot, authenticated. Returns every event the caller missed since `from`, squashed and hydrated. The protocol gives you the typed blanks — `CatchupRequest` and `CatchupResponse` (`packages/protocol/src/catchup.ts:19-32`) — but **these are schemas, not an `HttpApi`**: you own the route, method, status codes, errors, and auth, and you decode the request / encode the response in your own handler.

```typescript
import { Schema } from "effect"
import { CatchupRequest, CatchupResponse, squash } from "@triargos/live-collection-protocol"

// Decode the query at the boundary — never read `from` as a raw string downstream.
const { from } = yield* Schema.decodeUnknown(CatchupRequest)(query)
```

The handler's obligations, in order:

1. **Authenticate** → `userId`. (No `clientId`.)
2. **Resolve groups server-side**: `groups = yield* groupsFor({ userId })`. **Note there is no `group` query parameter the client controls.** The kit's `CatchupRequest` is `{ from }` only, by design — "the server decides which groups the caller may see from their permissions and returns everything visible since `from`" (`packages/protocol/src/catchup.ts:14-16`). Never trust a client-narrowed group set.
3. **Retention check**: if `from` is older than the retention window (the rows that far back have been pruned), you cannot honor it with deltas. Return a single inline `Resync(All)` instead of an event list, plus the current `lastSyncId`. This row is **not** written to the log — it's a synthesized response telling the client to drop everything and rebootstrap.
4. **Query** the store by groups since the cursor: `sync_id > :from AND sync_groups && :groups ORDER BY sync_id`.
5. **Squash** the raw rows with the protocol's `squash` (`packages/protocol/src/squash.ts:73`). This is the same pure fold the client relies on — a run of changes per entity folds to one terminal event, an insert-then-delete cancels out, and a `Resync` drops everything it supersedes (`All` → all prior, `Group` → prior events under that group via `isUnder`, `Model` → prior events of that model). The fold is idempotent. Both ends import the *same* function so the squash is identical — do not reimplement it.
6. **Hydrate** with the model registry's batched `hydrateMany` — **one call per `modelName`, not one per event**. Without batching this is the N+1 problem in disguise. Hydration carries the caller's `SyncContext` so it applies ACL: an entity the caller can no longer see hydrates to `Option.none()`.
7. **Return** `{ events, lastSyncId }` encoded through `CatchupResponse`, where each event is a `HydratedSyncEventEnvelope` (`data` as opaque JSON, decoded against the model schema on the client).

The retention-too-old case must be a typed outcome, not a throw. Model it as a `Schema.TaggedError` your route maps to its status code:

```typescript
class CatchupTooOld extends Schema.TaggedError<CatchupTooOld>()("CatchupTooOld", {
  from: SyncId,
  oldestRetained: SyncId
}) {}
```

### The two retention axes — don't conflate them

There are **two independent retention limits**, and they live on opposite ends:

| Axis | Lives where | Unit | Surfaces as |
|------|-------------|------|-------------|
| **Server wall-clock retention** | `sync_events` table (this doc) | time (e.g. 7 days) | a `Resync(All)` from `/catchup` when `from` predates it |
| **Client event-count cap** | the frontend EventLog (see [replay-on-mount](./replay-on-mount.md)) | a fixed number of events | nothing on the wire — purely client-local trimming |

The server prunes by **age**; the client trims its local replay log by **count**. They never coordinate. A client that falls behind the server's wall-clock window can no longer be served deltas, so the server's only honest answer is "resync from scratch."

---

## `GET /sync` (SSE) — the live tail

Long-lived, authenticated. Pushes hydrated events as they happen, each filtered to the connection's current groups. Each connection holds an Effect `Scope`; disposing it cleans up the bus subscription (and the membership-refresh subscription below).

On connect:

1. **Authenticate** → `userId`, build the `SyncContext` (`{ userId, syncGroups }` — `packages/protocol/src/model-registry.ts:18-22`).
2. **Resolve the initial group set**: `groups = yield* groupsFor({ userId })`.
3. If a `Last-Event-ID` header is present, run an **implicit catchup** from that `syncId` first and stream those events before going live (the same squash + hydrate path as `/catchup`).
4. **Subscribe to the bus** (`yield* bus.subscribe()`), scoped to the connection.
5. **Subscribe to the membership-refresh signal** for this user (see *Live-connection refresh* below).

Per event off the bus:

- **Skip** if the event's `syncGroups` don't intersect the connection's current `groups` — `intersects(event.syncGroups, connection.groups)` (`packages/protocol/src/sync-group.ts:40-46`). Matching is exact per group; a child group never matches its parent, so a private sub-group can't leak to members of a broader scope.
- **Hydrate** the event with the caller's `SyncContext`.
- **Synthetic-delete on ACL loss**: if hydration returns `Option.none()` (the entity vanished *or* the caller lost access to it), emit a synthetic `Delete` for that `modelId` rather than the original event — the client must remove the row it can no longer see. This is the one place the read path *changes* an event's tag.
- **Push** the hydrated (or synthetic-delete) event as an SSE frame, with `syncId` as the SSE event id so a reconnect can resume via `Last-Event-ID`.

> **`data: T | null` is a wire contract — decode it to `Option` at the boundary.** The hydrated wire shape carries `data: T | null` (null for `Delete`), but internally absence is modeled with `Option` (CLAUDE.md). When you hydrate, your `hydrate`/`hydrateMany` already return `Option<T>` (`packages/protocol/src/model-registry.ts:37-43`); `Option.none()` is precisely the "emit a synthetic delete" trigger. Convert to the nullable wire field only at the final encode step.

**Hydration failure for a single event is not fatal**: log it, skip that event, continue the stream. The next event for that entity, or a future catchup, heals it. Do not kill the connection over one bad hydrate.

### Live-connection refresh

Membership changes do not write a subscription table. They emit a domain event that the per-connection handler listens for, filtered to `userId === connection.userId`; on receipt it **re-runs `groupsFor` and replaces the connection's cached group set**. No DB writes, no denormalization, no propagation delay beyond the publish.

> **The membership-change event shape is deliberately not fixed by this library.** The name `MembershipChangedEvent` and its fields are a **placeholder**, not a fixed API: it is a domain event from your membership aggregate carrying at minimum the affected `userId` and enough to know which group changed. Pin its shape in your own app before wiring the refresh; this library does not define it.

---

## Resync emission — all three variants

A resync tells subscribers to discard part of their local state and re-fetch, for changes deltas can't express (a permission change, a bulk correction). The protocol's `ResyncTarget` encodes how much to reset, narrowest to widest (`packages/protocol/src/resync.ts:16-20`):

| Variant | Constructor | Effect on the client |
|---------|-------------|----------------------|
| **Per-model** | `ResyncModel.make({ model })` | client clears that model's collections within the affected groups and rebootstraps |
| **Per-group** | `ResyncGroup.make({ group })` | client clears all collections for entities under that group (matched by `isUnder`) |
| **Global** | `ResyncAll` | client drops everything and rebootstraps |

All three are emitted **through the dispatcher** like any other event — built as a `PendingResync` (`packages/protocol/src/sync-event.ts:35`) with a `target` and the `syncGroups` it's delivered to — **except** the catchup-too-old `Resync(All)`, which is synthesized inline in the `/catchup` response and **not written to the log** (the retention case above).

**Membership removal → per-group resync**: when a user loses access to a group, the live-connection refresh stops *new* events for that group from reaching them — but it does nothing about the **stale data already in their collections**. So the membership-change projection must also emit a per-group resync **tagged with `user:<removedUserId>`** so it lands on exactly that user's session and tells their client to clear the group's local data. Without it, the client silently keeps stale rows forever.

```typescript
import { deriveGroup, ResyncGroup } from "@triargos/live-collection-protocol"

// On membership removal: deliver a per-group resync to just the removed user's session.
const pending = {
  _tag: "Resync" as const,
  target: ResyncGroup.make({ group: removedGroup }),
  syncGroups: [deriveGroup(["user", removedUserId])] // delivery target = that user only
}
yield* dispatcher.dispatch(pending)
```

Per-model and global resyncs are emitted the same way (from admin/data-correction tooling, typically). The squasher already understands all three targets, so a resync correctly supersedes the right preceding events on catchup.

---

## Failure modes worth internalizing

The correctness boundaries:

- **Projection handler fails after the domain event committed.** The sync event is missing; live clients miss it; the next catchup heals it *only if the projection retries successfully*. A deterministically-crashing handler loses the event permanently (manual resync recovers). Keep projection handlers trivially simple — no I/O beyond the dispatcher — and test them hard. This is the most important boundary.
- **Row written, bus publish failed.** Already handled: best-effort publish, catchup heals. Log a warning.
- **Catchup hydration explosion.** A client offline for a week catching up thousands of squashed events: `hydrateMany` is the first defense. Beyond a threshold (e.g. >10K squashed events) return partial results with a continuation cursor, or refuse and force `Resync(All)`. Pick the threshold and document it.
- **`syncId` gaps.** `BIGSERIAL` gaps on rollback. Clients order, never assume contiguity. Assert gap-tolerance in tests.

All of these are infrastructure defects, not domain failures: `Effect.orDie` them and keep the error channel reserved for modeled outcomes like `CatchupTooOld` and `UnknownModelError` (`packages/protocol/src/model-registry.ts:62-68`).

---

## Conventions for any code you write here

This backend is per-app and lives outside this repo, but the same conventions the library holds itself to make the seams interoperate cleanly:

- **No `throw`, no `new Error(...)` across boundaries.** Model domain failures as `Schema.TaggedError` (e.g. `CatchupTooOld`); infrastructure failures are defects — `Effect.orDie`. Never `Effect.catchAllCause` (it swallows defects).
- **`Option` over null.** The wire's `data: T | null` decodes to `Option<T>` at the boundary; pass `Option` through hydration; convert back to the nullable field only at the final encode.
- **Object args when a function has more than one of its own parameters** — `groupsFor({ userId })`, `ResyncGroup.make({ group })`.
- **Seams are `Context.Tag` + `interface <Name>Shape` + a separate `make` + `<Name>.layer`** — never `Effect.Service` (it fuses tag/impl/default-layer and is being removed in Effect v4).
- **Brand ids only at boundaries.** `SyncId.make` / `ModelName.make` / `deriveGroup` belong in row decoders and request handlers; inside the app the values already carry their brands.

---

## See also

- [`./protocol.md`](./protocol.md) — the wire contract and the typed blanks you fill: the event schemas, the sync-group grammar, the squasher, the resync targets, the model registry types, and the `/catchup` request/response schemas.
- [`./read-path.md`](./read-path.md) — the *client* side of `/sync` and `/catchup`: how the frontend consumes the SSE tail, runs catchup against your endpoint, and stores the durable `lastSyncId` cursor.
- [`./replay-on-mount.md`](./replay-on-mount.md) — the client EventLog and its **event-count** retention cap (the other retention axis).
- A miniature reference of every seam this doc describes — store, dispatcher (`commit`), `/catchup`, the SSE tail — wired as a single fake authority: [`examples/playground/src/live/shared-backend.ts`](../examples/playground/src/live/shared-backend.ts).
