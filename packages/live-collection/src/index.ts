/**
 * `@triargos/live-collection` — local-first live collections for Effect + TanStack DB.
 *
 * Collections are **native** TanStack collections, persisted locally (SQLite) and kept
 * in sync with your backend over a live event stream plus catchup. Getting started:
 *
 * 1. Build the app-wide runtime once at startup: `makeLiveRuntime`.
 * 2. Declare one `defineCollection` per synced model — it returns the collection handle.
 * 3. Start the sync loop near your app root: `useLiveSync(runtime, [webhookCollection, …])`
 *    (from `@triargos/live-collection-react`), or `runtime.forkLoop(models)` outside React.
 * 4. Read with `useLiveQuery`, write optimistically with `collection.insert/update/delete`.
 *
 * The hero type is `LiveCollection<T>` — what a collection handle returns. The wire
 * contract shared with your backend lives in `@triargos/live-collection-protocol`.
 */
import type { SyncEvent } from "@triargos/live-collection-protocol"

/** Re-exported from `@triargos/live-collection-protocol` so consumers share one contract type. */
export type { SyncEvent }

// registry/ — generic collection cache + the runtime-bound collection handle. makeRegistry stays
// public: composing syncLoop manually (without makeLiveRuntime) needs the registry as a VALUE shared
// between the mount path and the loop's layer.
export * from "./registry/collection-key.js"
export * from "./registry/collection-registry.js"
export * from "./registry/define-collection.js"

// dispatch/ — the synced-store write path contract
export * from "./dispatch/sync-write.js"

// client/ — SSE transport, catchup, durable cursor, the durable event log, the sync loop.
// (mount-decision, mount-healer, prune-plan, and sync-session are internal policy/plumbing —
// reachable behaviorally through syncLoop and the collection utils, not part of the API.)
export * from "./client/last-sync-id-store.js"
export * from "./client/catchup-client.js"
export * from "./client/sync-transport.js"
export * from "./client/event-log-store.js"
export * from "./client/sync-loop.js"

// runtime/ — the live runtime that owns infra and forks the loop
export * from "./runtime/live-runtime.js"

// persistence/ — LiveCollection<T> (the hero type) and the building blocks for assembling
// a persisted collection by hand (defineCollection uses them internally).
export type { LiveCollection } from "./persistence/live-collection.js"
export * from "./persistence/live-collection-options.js"
export * from "./persistence/schema-version.js"

/** The library's version, for diagnostics. */
export const LIB_VERSION = "0.0.0"
