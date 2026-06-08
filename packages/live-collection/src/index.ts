/**
 * `@triargos/live-collection` — the frontend live-sync engine.
 *
 * Internal module layout (each lands task by task — see TASKS.md Bucket A):
 *   registry/     CollectionRegistry, globToRegex, lifecycle helpers   (A.1, A.2)
 *   dispatch/     dispatch registry + entity-agnostic resolver         (A.5)
 *   persistence/  effectCollectionOptions over TanStack DB 0.6         (A.3, A.4)
 *   client/       SSE transport, catchup, lastSyncId store             (A.6, A.7)
 *   bootstrap/    cold/warm start + workspace-switch orchestration     (A.9)
 *
 * `core` modules declare the service tags (SyncTransport,
 * PersistedCollectionFactory, LastSyncIdStore); `persistence`/`client` modules
 * provide their default Layers; the app composes them at the edge.
 *
 * This file is the package skeleton.
 */
import type { SyncEvent } from "@triargos/live-collection-protocol"

/** Re-exported so consumers and the dispatch layer share one contract type. */
export type { SyncEvent }

// registry/ — generic collection cache + scoping (A.1/A.2)
export * from "./registry/collection-key.js"
export * from "./registry/collection-registry.js"
export * from "./registry/define-collection.js"

// dispatch/ — entity-agnostic event routing (A.5)
export * from "./dispatch/sync-write.js"
export * from "./dispatch/sync-dispatcher.js"

// client/ — SSE transport, catchup, durable cursor, bootstrap orchestrator (A.6–A.9)
export * from "./client/last-sync-id-store.js"
export * from "./client/catchup-client.js"
export * from "./client/sync-transport.js"
export * from "./client/sync-client.js"

// persistence/ — the per-entity factory seam over TanStack DB 0.6 (A.3/A.4)
//   LiveCollection<T> is the hero type: a TanStack Collection whose `utils` host the synced-write
//   path. effectCollectionOptions is the factory `defineCollection.make` calls; PersistenceBase is
//   the shared SQLite base; deriveSchemaVersion drives DEC-A6 dump-and-rebuild.
export type { LiveCollection } from "./persistence/live-collection.js"
export * from "./persistence/effect-collection.js"
export * from "./persistence/persistence-base.js"
export * from "./persistence/schema-version.js"

export const LIB_VERSION = "0.0.0"
