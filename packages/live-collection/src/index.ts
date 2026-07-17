/**
 * `@triargos/live-collection` — local-first live collections for Effect + TanStack DB.
 *
 * Collections are **native** TanStack collections, persisted locally (SQLite) and kept
 * in sync with your backend over a live event stream plus catchup. Getting started:
 *
 * 1. Build the app-wide runtime once at startup: `makeLiveRuntime`.
 * 2. Declare one `defineCollection` per synced model — it returns the collection handle.
 * 3. Start broker ingest near your app root with `useLiveSync(runtime)`
 *    (from `@triargos/live-collection-react`), or `runtime.forkSync()` outside React.
 * 4. Read with `useLiveQuery`, write optimistically with `collection.insert/update/delete`.
 *
 * The hero type is `LiveCollection<T>` — what a collection handle returns. The wire
 * contract shared with your backend lives in `@triargos/live-collection-protocol`.
 */
import type { SyncEvent } from "@triargos/live-collection-protocol"

/** Re-exported from `@triargos/live-collection-protocol` so consumers share one contract type. */
export type { SyncEvent }

// registry/ — collection lifetime table, structured keys, and runtime-bound handles.
export * from "./registry/collection-key.js"
export * from "./registry/collection-registry.js"
export * from "./registry/define-collection.js"

// dispatch/ — the synced-store write path contract
export * from "./dispatch/sync-write.js"

// client/ — SSE transport, catchup, durable cursor/log, and the subscription broker.
// (prune-plan and sync-session are internal policy/plumbing.)
export * from "./client/last-sync-id-store.js"
export * from "./client/catchup-client.js"
export * from "./client/sync-transport.js"
export * from "./client/event-log-store.js"
export * from "./client/sync-broker.js"

// runtime/ — the live runtime that owns infra and forks broker ingest
export * from "./runtime/live-runtime.js"

// persistence/ — LiveCollection<T> (the hero type) and the building blocks for assembling
// a persisted collection by hand (defineCollection uses them internally).
export type { LiveCollection } from "./persistence/live-collection.js"
export * from "./persistence/live-collection-options.js"
export * from "./persistence/schema-version.js"

/** The library's version, for diagnostics. */
export const LIB_VERSION = "0.0.0"
