/**
 * `@triargos/live-collection` — the frontend live-sync engine.
 *
 * Internal module layout:
 *   registry/     CollectionRegistry, CollectionKey, defineCollection (runtime-bound handle)
 *   dispatch/     the SyncWrite contract (the synced-store write path)
 *   persistence/  liveCollectionOptions (inner creator) over TanStack DB 0.6
 *   client/       SSE transport, catchup, lastSyncId store, the sync loop
 *   runtime/      makeLiveRuntime (two-surface: sync registry+persistence | async loop)
 *
 * The app makes a `persistence` value + a `LiveRuntime`, declares one `defineCollection` per model,
 * and wires them with an explicit `SyncMap` to `useLiveSync` (in `@triargos/live-collection-react`).
 * Collections are native TanStack collections — pass them straight to `useLiveQuery`.
 */
import type { SyncEvent } from "@triargos/live-collection-protocol"

/** Re-exported so consumers and the dispatch layer share one contract type. */
export type { SyncEvent }

// registry/ — generic collection cache + the runtime-bound collection handle
export * from "./registry/collection-key.js"
export * from "./registry/collection-registry.js"
export * from "./registry/define-collection.js"

// dispatch/ — the synced-store write path contract
export * from "./dispatch/sync-write.js"

// client/ — SSE transport, catchup, durable cursor, the durable event log, the sync loop
export * from "./client/last-sync-id-store.js"
export * from "./client/catchup-client.js"
export * from "./client/sync-transport.js"
export * from "./client/event-log-store.js"
export * from "./client/mount-decision.js"
export * from "./client/sync-loop.js"

// runtime/ — the live runtime that owns infra and forks the loop
export * from "./runtime/live-runtime.js"

// persistence/ — the per-entity options creator over TanStack DB 0.6
//   LiveCollection<T> is the hero type: a native TanStack Collection whose `utils` host the synced
//   write path. liveCollectionOptions is the inner creator defineCollection's `make` spreads into
//   persistedCollectionOptions; deriveSchemaVersion drives DEC-A6 dump-and-rebuild.
export type { LiveCollection } from "./persistence/live-collection.js"
export * from "./persistence/live-collection-options.js"
export * from "./persistence/schema-version.js"

export const LIB_VERSION = "0.0.0"
