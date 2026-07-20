# The backend contract for `@triargos/live-collection`

This library is **frontend-only**. It ships no server, and it does not care how yours is built —
what database, what runtime, what service architecture. This document specifies the only thing the
library can observe: **the two HTTP surfaces the client calls and the semantic invariants its
correctness depends on**. Satisfy these and any backend works.

The one piece you import is [`@triargos/live-collection-protocol`](./protocol.md) — the schemas to
decode/encode at your edges, the branded ids, the sync-group grammar, and the squasher. Everything
else in this document is behavior, not code.

For working implementations of this contract, see the
[reference implementations](#reference-implementations) below — they are typechecked and tested in
this repository, so prefer reading them over any prose sketch.

---

## `GET {catchup.url}?from=<SyncId>` — one-shot backfill

What the client does (`CatchupClient.layer({ url })`,
`packages/live-collection/src/client/catchup-client.ts`): sends `GET {url}?from=<cursor>` and
decodes the JSON body against the protocol's `CatchupResponse`. A non-2xx response or an
undecodable body becomes a modeled `CatchupFailed`, which the client logs and **tails the live
stream anyway** — a transient catchup miss heals on the next reconnect.

What your handler must return — `CatchupResponse`, i.e. `{ events, lastSyncId, epoch? }`:

- **`events`**: every event since `from` that the caller may see, in `syncId` order, **hydrated**
  (`Insert`/`Update` carry the entity's *current* data; an entity that is gone or no longer visible
  arrives as a `Delete`). Squash with the protocol's `squash` before hydrating — it is
  semantics-preserving and both ends rely on the same fold; skipping it costs payload and
  hydration work, not correctness.
- **`lastSyncId`**: the log's current head — the cursor the client stores durably.
- **`epoch`**: required only if your log's history can reset (see invariants below).
- **Retention**: if `from` predates what you retain, you cannot honor it with deltas — return a
  single `Resync` event with target `All` (synthesized inline, not written to your log) plus the
  current `lastSyncId`. The client drops everything and rebootstraps.

`CatchupRequest` is `{ from }` **only** — there is deliberately no `group` parameter. The caller's
visibility is resolved server-side from their permissions on every call; never trust a
client-supplied group set.

## `GET {sync.url}` — SSE live tail

What the client does (`SyncTransport.layer({ url, keepAlive })`,
`packages/live-collection/src/client/sync-transport.ts`): opens a long-lived request and reads SSE
frames. Each event's `data:` payload must be **one JSON-encoded `HydratedSyncEventEnvelope`**. A
frame that fails to decode is logged and dropped, never fatal — so a newer server may emit shapes
an older client doesn't know. When the connection drops or falls silent, the client reconnects and
**re-runs catchup first**, then tails again.

What your endpoint must do:

- **Authenticate**, resolve the caller's current groups server-side, and push only events whose
  `syncGroups` intersect them (`intersects` from the protocol; matching is exact per group — a
  child group never matches its parent).
- **Hydrate** each event before pushing, same rules as catchup: current data on `Insert`/`Update`;
  entity gone or access lost ⇒ push a `Delete` instead. A single failed hydration is logged and
  skipped, not a reason to kill the connection.
- **Ping**: emit some line (an SSE comment like `:ka` is enough) more often than the client's
  configured `keepAlive` window — silence beyond it counts as a dropped connection.
- **Nothing else.** The client never sends `Last-Event-ID` and ignores SSE event ids; reconnect
  healing is entirely client-driven via catchup. You do not need replay-from-id on this endpoint.

If a caller's permissions change while connected, deliver a per-group `Resync` targeted at that
user (see invariants) — the connection-level group refresh alone does not clear data the client
already holds.

---

## Invariants the client's correctness depends on

- **`SyncId` semantics.** Monotonically increasing within one log timeline, **gap-tolerant**
  (clients order by it and never assume `n+1`), encoded as the protocol's canonical decimal
  string, compared numerically (`compareSyncId`) — never lexically.
- **The epoch invariant.** SyncIds are only comparable within one epoch. If your log's history can
  be destroyed or replaced (in-memory store that resets on restart, truncation, backup restore),
  mint an opaque `Epoch` and return it on catchup; the client detects a mismatch and self-heals by
  wiping its local sync state and rebootstrapping. A log that is durable for the server's lifetime
  can omit it. It is not a software version — a redeploy must not change it.
- **Catchup is authoritative.** Live delivery may miss events (connection down, publish lost);
  every event a caller may see must remain reachable through catchup until retention expires.
  The client treats the SSE tail as best-effort and the catchup path as truth.
- **No echo suppression.** Originating clients must receive their own writes back through normal
  sync. Client-minted ids make the self-echo idempotent, and TanStack DB's optimistic-mutation
  reconciliation expects the synced store to confirm the client's own writes — an originator
  filter (a `clientId` column, an `X-Client-Id` header, a `client_id != :clientId` clause) breaks
  it. Do not add one.
- **Resync targets are structural.** A resync is a first-class `Resync` event arm carrying a
  `ResyncTarget` (`All` / `Group({group})` / `Model({model})`) — never an entity event with a
  sentinel `modelName` like `'__all'`. On membership removal, emit a `Group` resync delivered to
  `user:<removedUserId>` so exactly that user's client clears the group's local data; without it,
  stale rows persist forever.
- **Access loss surfaces as `Delete`.** Hydration receives the caller's current `syncGroups` and
  is the authoritative visibility check (the event-level group filter uses groups stamped at log
  time; access may have changed since). `Option.none()` from your hydration ⇒ the client receives
  a `Delete` and removes the row.
- **Retention is two independent axes.** Your server prunes its log by age and answers too-old
  cursors with `Resync(All)`; the client separately trims its local replay log by event count
  (see [replay-on-mount.md](./replay-on-mount.md)). They never coordinate.

---

## The kernel package — the contract as code (optional)

Everything above remains satisfiable by hand on any stack. For backends built on Effect,
[`@triargos/live-collection-server`](../packages/server/README.md) ships the invariant-bearing
orchestration as code instead of prose: `SyncFeed.catchup` (filter → squash → batched hydration →
`Option.none` ⇒ `Delete` → retention ⇒ `Resync(All)` → epoch passthrough), `SyncFeed.streamEvents`
(hydrated, group-filtered SSE frames with keepalive; drop-don't-kill), and `SyncDispatcher`
(persist-then-best-effort-publish, no echo suppression). You supply two ports — a `SyncEventStore`
(`appendEvent` / `listEvents({ cursor })` / `getLatestSyncId` / `getCurrentEpoch`) and a model
registry built with the make-pattern (`ModelRegistry.layer(Effect.gen(...))` — repos resolved once,
descriptors close over them, requirements inferred from the build effect) — and keep auth, routes,
and storage. The pi-demo reference backend below consumes it.

## What the kit offers (all optional except the schemas)

Decode and encode the wire shapes with the protocol schemas — `CatchupRequest`,
`CatchupResponse`, `HydratedSyncEventEnvelope`, `SyncEvent`, `PendingSyncEvent` — and never cast.
Beyond that, the kit ships helpers you may use or ignore:

- **`squash`** — the pure catchup fold. If you compact at all, import this rather than
  reimplementing it; the client relies on its exact semantics.
- **`ModelDescriptor` / `defineModelRegistry` / `narrowModelName`** — typed scaffolding for a
  hydration registry: per-model schema + `hydrate`/`hydrateMany` (batch it — one call per model,
  not one per event, or catchup becomes an N+1), with registry keys forming your closed model-name
  union. How you organize hydration is your choice; only its observable behavior (above) is
  contract.
- **`deriveGroup` / `isUnder` / `intersects`** — the sync-group grammar. Build groups with
  `deriveGroup(segments)`, not string concatenation.

There is deliberately **no auth surface in the kit** — no principal type, no resolver signature.
Who is syncing and how you map them to groups is your backend's own design.

---

## Reference implementations

- [`examples/pi-demo/server`](../examples/pi-demo/server) — a real Effect HTTP backend consuming
  `@triargos/live-collection-server`: memory adapters behind the kernel's ports, a two-model
  registry, and `/catchup` + SSE routes that reduce to `SyncFeed` calls.
- [`examples/playground/src/live/shared-backend.ts`](../examples/playground/src/live/shared-backend.ts)
  — the same contract as a browser-local fake authority (localStorage log + `BroadcastChannel`),
  useful for seeing the contract with zero HTTP.

---

## See also

- [`./protocol.md`](./protocol.md) — the wire contract in full: event schemas, the sync-group
  grammar, the squasher, resync targets, and the `/catchup` schemas.
- [`./read-path.md`](./read-path.md) — the *client* side of these two endpoints: the SSE tail,
  catchup, and the durable `lastSyncId` cursor.
- [`./replay-on-mount.md`](./replay-on-mount.md) — the client event log and its event-count
  retention cap.
