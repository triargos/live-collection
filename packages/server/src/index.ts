/**
 * `@triargos/live-collection-server` — the optional backend kernel for the
 * live-collection sync system.
 *
 * This whole package is optional: the client contract is two endpoints
 * (`/catchup` + an SSE stream) plus the invariants documented in
 * `docs/backend.md`, and any backend that satisfies them by hand works. This
 * package is the enforced-by-code option: it owns the invariant-bearing
 * orchestration between two ports the app supplies — a `SyncEventStore` and a
 * model registry — while the app keeps auth, routes, storage, repos, and
 * sync-group resolution.
 *
 * Depends only on `effect` and `@triargos/live-collection-protocol`: no HTTP,
 * no storage driver, no auth surface.
 */
export { CursorOutOfRetentionError, SyncEventStore, type SyncEventStoreShape } from "./sync-event-store.js"
export { SyncEventBus, type SyncEventBusShape } from "./sync-event-bus.js"
export { SyncDispatcher, type SyncDispatcherShape } from "./sync-dispatcher.js"
export { ModelRegistry, type ModelRegistryShape, type ResolvedModel } from "./model-registry.js"
export { SyncFeed, type SyncFeedShape } from "./sync-feed.js"
