# The backend contract

The library ships no server and doesn't care how yours is built. This page specifies the only thing the client can observe: **two HTTP surfaces and the invariants its correctness depends on**. Satisfy these and any backend works.

Import [`@triargos/live-collection-protocol`](./protocol.md) for the schemas to decode/encode at your edges. Backends on Effect can skip most of the hand-rolling with [`@triargos/live-collection-server`](../packages/server/README.md), which implements this contract as code — see [the kernel](#the-kernel-package) below.

## `GET /catchup?from=<syncId>` — one-shot backfill

The client sends its stored cursor and expects a JSON `CatchupResponse`: `{ events, lastSyncId, epoch? }`.

- **`events`** — every event since `from` that the caller may see, in `syncId` order, **hydrated**: `Insert`/`Update` carry the entity's *current* data; an entity that is gone or no longer visible arrives as a `Delete`. Compact the page with the protocol's `squash` before hydrating — one event per entity, not its history.
- **`lastSyncId`** — the log's current head. The client stores it durably as its next cursor.
- **`epoch`** — required only if your log's history can reset (see [invariants](#invariants)).
- **Retention** — if `from` predates what you retain, return a single synthetic `Resync` event with target `All` (not written to your log) plus the current `lastSyncId`. The client wipes and re-bootstraps.

The request is `{ from }` and nothing else — there is deliberately no group parameter. Resolve the caller's visibility from their auth on every call; never trust a client-supplied group set.

A failed catchup is not fatal to the client: it logs, tails the live stream anyway, and retries on the next reconnect.

## `GET /sync` — SSE live tail

A long-lived SSE response. Each event's `data:` payload is one JSON-encoded `HydratedSyncEventEnvelope`.

- **Authenticate**, resolve the caller's current groups server-side, and push only events whose `syncGroups` intersect them (`intersects` from the protocol — exact match per group, never hierarchical).
- **Hydrate** each event before pushing, same rules as catchup. A single failed hydration is logged and skipped, never a reason to kill the connection.
- **Keepalive** — emit something (an SSE comment like `:ka` is enough) more often than the client's configured silence window; silence beyond it counts as a dropped connection.
- **Nothing else.** The client ignores SSE event ids and never sends `Last-Event-ID`; reconnect healing is entirely client-driven via catchup. No replay-from-id needed here.

If a caller's permissions change while connected, deliver a `Resync` with target `Group` to that user (via a `user:<id>` group) — otherwise data they can no longer see stays on their device forever.

## Invariants

- **`SyncId` semantics.** Monotonically increasing within one log timeline, gap-tolerant (clients order by it, never assume `n+1`), encoded as a canonical decimal string, compared numerically (`compareSyncId`) — never lexically.
- **Epoch.** SyncIds are only comparable within one timeline. If your log's history can be destroyed or replaced — an in-memory store that resets on restart, a truncation, a backup restore — mint an opaque `Epoch` and return it on catchup. The client detects a change and self-heals by wiping local sync state and re-bootstrapping. Without it, clients holding old cursors freeze silently: every new event is minted "below" their stale head and discarded. It is *not* a software version — a redeploy over a durable log must not change it; a backup restore must.
- **Catchup is authoritative.** Live delivery may miss events; every event a caller may see must remain reachable through catchup until retention expires. The client treats SSE as best-effort and catchup as truth.
- **No echo suppression.** Originating clients must receive their own writes back through normal sync. Client-minted ids make the self-echo idempotent, and the client's optimistic-write reconciliation depends on it — a `clientId` filter anywhere breaks it. Do not add one.
- **Resync targets are structural.** A resync is the `Resync` event arm carrying a typed `ResyncTarget` (`All` / `Group` / `Model`) — never an entity event with a sentinel model name.
- **Access loss surfaces as `Delete`.** Hydration receives the caller's *current* groups and is the authoritative visibility check (the event-level filter uses groups stamped at log time; access may have changed since). Hydration returning nothing ⇒ the client receives a `Delete` and removes the row.

## The kernel package

For Effect backends, [`@triargos/live-collection-server`](../packages/server/README.md) enforces all of the above: `SyncFeed.catchup` (filter → squash → batched hydration → access-loss-as-`Delete` → retention-as-`Resync(All)` → epoch), `SyncFeed.streamEvents` (hydrated, group-filtered SSE frames with keepalive), and `SyncDispatcher` (persist-then-publish, no echo suppression). You supply two ports — a `SyncEventStore` over your database and a model registry describing how each entity hydrates — and keep auth, routes, and storage. Its README is the integration guide.

## Reference implementations

- [`examples/pi-demo/server`](../examples/pi-demo/server) — a complete Effect HTTP backend consuming the kernel: repos, a model registry, session auth, and `/catchup` + SSE routes.
- [`examples/playground/src/live/shared-backend.ts`](../examples/playground/src/live/shared-backend.ts) — the same contract as a browser-local fake (localStorage log + `BroadcastChannel`), useful for seeing the contract with zero HTTP.

## See also

- [Protocol reference](./protocol.md) — the schemas, the sync-group grammar, the squasher.
- [Architecture](./architecture.md) — the client side of these two endpoints.
