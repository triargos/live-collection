import { Effect } from "effect"
import type { SyncAppliedReceipt, SyncConfig } from "@tanstack/db"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "./sync-write.js"
import { makeSyncWrite, type SyncSession } from "./sync-session.js"

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
 * Surfaces a failed commit receipt without waiting on it. The persisted wrapper's receipt
 * settles after the SQLite write and swallows its failure, so ignoring it would make a
 * lost write silent. `AbortError` is TanStack's documented cleanup/abort rejection and is
 * expected. Awaiting the receipt (so writes complete only once durable) is a separate decision.
 */
const warnOnFailedReceipt = (collectionId: string, receipt: SyncAppliedReceipt): void => {
  if (receipt === true) return
  receipt.catch((error: unknown) => {
    if (error instanceof Error && error.name === "AbortError") return
    Effect.runFork(
      Effect.logWarning(`[liveCollection] sync transaction for "${collectionId}" failed to persist`, error),
    )
  })
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
        const commit = () => warnOnFailedReceipt(params.collection.id, params.commit())
        const session: SyncSession<T> = {
          upsert: (entity) => {
            params.begin()
            params.write({ type: "update", value: entity })
            commit()
          },
          remove: (id) => {
            params.begin()
            params.write({ type: "delete", key: id })
            commit()
          },
          replace: (rows) => {
            params.begin()
            params.truncate() // clears store + table atomically with the writes below (one tx)
            for (const row of rows) params.write({ type: "update", value: row })
            commit()
          },
          patch: ({ deleteKeys, rows }) => {
            params.begin()
            for (const key of deleteKeys) params.write({ type: "delete", key })
            for (const row of rows) params.write({ type: "update", value: row })
            commit()
          },
        }
        provide(session)
        params.markReady() // wrapper defers this until internal hydration completes
      },
    },
  }
}
