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

/**
 * The hero type. A TanStack DB collection that is locally persisted and kept
 * live-synced with the authoritative server. The factory `createCollection`
 * (task A.4) is the only seam where the persistence backend is chosen.
 *
 * TODO(A.4): replace this placeholder with the real collection surface.
 */
export interface LiveCollection<T> {
  readonly id: string
  readonly __entity?: T
}

export const LIB_VERSION = "0.0.0"
