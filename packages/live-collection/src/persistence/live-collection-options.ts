import { Effect } from "effect"
import type { SyncConfig } from "@tanstack/db"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "./sync-write.js"
import { makeSyncWrite, type ReconcileWrite, type SyncSession } from "./sync-session.js"

/**
 * The fields {@link liveCollectionOptions} contributes to a persisted collection — the
 * *inner* options to spread into TanStack's `persistedCollectionOptions`, exactly as the
 * TanStack docs spread `queryCollectionOptions`. It is **not** a full collection config:
 * `persistence`, `schemaVersion`, and `id` are added at the outer level (by
 * `defineCollection`, or by you when assembling a collection manually).
 */
export interface LiveCollectionOptions<T extends object> {
  /** Extracts the entity's primary key. */
  readonly getKey: (entity: T) => ModelId
  /** `Infinity` — the registry owns collection lifetime; TanStack never GCs a live collection. */
  readonly gcTime: number
  /** `"eager"` — load the saved rows when the collection starts, not query-driven. */
  readonly syncMode: "eager"
  /** `true` — start sync on mount, so the write session is captured and hydration runs. */
  readonly startSync: true
  /** The synced-store write path ({@link SyncWrite}) the collection drain applies server events through. */
  readonly utils: SyncWrite<T>
  /** The network-free TanStack sync config wiring `utils` to the store. */
  readonly sync: SyncConfig<T, ModelId>
}

/**
 * The inner options creator — the live-sync analogue of TanStack's
 * `queryCollectionOptions`. Most apps never call it: `defineCollection` does, internally.
 * Reach for it only when assembling a persisted collection by hand (e.g. a custom mount
 * path), spreading the result into `persistedCollectionOptions`.
 *
 * The returned `sync` is **network-free**: it only installs the write session behind
 * `utils.writeSynced`/`deleteSynced`/`replaceSynced` and signals ready. Server truth
 * reaches the store through the collection drain writing to `utils`, never through this `sync`.
 *
 * Synchronous by design: `createCollection` (its caller) is sync, so the one-shot session
 * `Deferred` is built with `Effect.runSync` — pure, no async boundary.
 */
export const liveCollectionOptions = <T extends object>(config: {
  readonly getKey: (entity: T) => ModelId
}): LiveCollectionOptions<T> => makeLiveCollectionOptions(config).options

/**
 * Internal: {@link liveCollectionOptions} plus the {@link ReconcileWrite} path `defineCollection`'s
 * mutation-handler bridge uses. Kept off `utils` so apps only see the durable {@link SyncWrite}.
 */
export const makeLiveCollectionOptions = <T extends object>(config: {
  readonly getKey: (entity: T) => ModelId
}): { readonly options: LiveCollectionOptions<T>; readonly reconcileWrite: ReconcileWrite<T> } => {
  const { syncWrite, reconcileWrite, provide } = Effect.runSync(makeSyncWrite<T>())
  const options: LiveCollectionOptions<T> = {
    getKey: config.getKey,
    gcTime: Infinity,
    syncMode: "eager",
    startSync: true,
    utils: syncWrite,
    sync: {
      sync: (params) => {
        const session: SyncSession<T> = {
          collectionId: params.collection.id,
          upsert: (entity) => {
            params.begin()
            params.write({ type: "update", value: entity })
            return params.commit()
          },
          remove: (id) => {
            params.begin()
            params.write({ type: "delete", key: id })
            return params.commit()
          },
          replace: (rows) => {
            params.begin()
            params.truncate() // clears store + table atomically with the writes below (one tx)
            for (const row of rows) params.write({ type: "update", value: row })
            return params.commit()
          },
          patch: ({ deleteKeys, rows }) => {
            params.begin()
            for (const key of deleteKeys) params.write({ type: "delete", key })
            for (const row of rows) params.write({ type: "update", value: row })
            return params.commit()
          },
        }
        // Hand out the session only once the persisted wrapper has hydrated; writes issued earlier
        // wait on the session Deferred. Guards a TanStack bug (db-sqlite-persistence-core 0.4.x):
        // SingleProcessCoordinator.requestApplyCommittedTx drops the hydration-scoped adapter, so a
        // sync commit made during startup hydration waits for the scheduler that hydration holds.
        // It never settles and wedges every collection on the shared persistence.
        // BrowserCollectionCoordinator passes the scoped adapter through and is unaffected.
        params.collection.onFirstReady(() => provide(session))
        params.markReady() // wrapper defers this until internal hydration completes
      },
    },
  }
  return { options, reconcileWrite }
}
