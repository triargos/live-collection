import { Effect } from "effect"
import type { SyncConfig } from "@tanstack/db"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "../dispatch/sync-write.js"
import { makeSyncWrite, type SyncSession } from "./sync-session.js"

/**
 * The fields {@link liveCollectionOptions} contributes to a persisted collection — the *inner* options
 * an app spreads into `persistedCollectionOptions`, exactly as the TanStack docs spread
 * `queryCollectionOptions` (DEC-R4). It is **not** a full collection config: `persistence`,
 * `schemaVersion`, and `id` are added by `defineCollection`'s `make` at the outer level.
 */
export interface LiveCollectionOptions<T extends object> {
  readonly getKey: (entity: T) => ModelId
  readonly gcTime: number // Infinity — registry is the sole GC (DEC-A10)
  readonly syncMode: "eager" // load the persisted base on start, not query-driven (DEC-A5)
  readonly startSync: true // start sync on mount → session captured + hydration runs
  readonly utils: SyncWrite<T> // writeSynced / deleteSynced, hosted in utils (DESIGN §1)
  readonly sync: SyncConfig<T, ModelId>
}

/**
 * The inner options creator — the live-sync analogue of TanStack's `queryCollectionOptions` (DEC-R4).
 * Returns a **network-free** `sync` (it only installs the {@link SyncSession} holder behind
 * `utils.writeSynced`/`deleteSynced` and signals ready, DEC-T1) plus the `SyncWrite` utils. Server
 * truth reaches the store through the dispatcher writing to `utils`, never through this `sync`.
 *
 * Synchronous by design: `createCollection` (its caller, in `defineCollection`'s `make`) is sync, so
 * the one-shot session `Deferred` is built with `Effect.runSync` — pure, no async boundary.
 */
export const liveCollectionOptions = <T extends object>(config: {
  readonly getKey: (entity: T) => ModelId
}): LiveCollectionOptions<T> => {
  const { syncWrite, provide } = Effect.runSync(makeSyncWrite<T>())
  return {
    getKey: config.getKey,
    gcTime: Infinity,
    syncMode: "eager",
    startSync: true,
    utils: syncWrite,
    sync: {
      sync: (params) => {
        const session: SyncSession<T> = {
          upsert: (entity) => {
            params.begin()
            params.write({ type: "update", value: entity })
            params.commit()
          },
          remove: (id) => {
            params.begin()
            params.write({ type: "delete", key: id })
            params.commit()
          },
          replace: (rows) => {
            params.begin()
            params.truncate() // clears store + table atomically with the writes below (one tx)
            for (const row of rows) params.write({ type: "update", value: row })
            params.commit()
          },
        }
        provide(session)
        params.markReady() // wrapper defers this until internal hydration completes
      },
    },
  }
}
