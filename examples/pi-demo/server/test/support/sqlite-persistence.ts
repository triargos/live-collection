import {
  type PersistedCollectionMode,
  type PersistedCollectionPersistence,
  type PersistenceAdapter,
  type SQLiteDriver,
  createSQLiteCorePersistenceAdapter,
  SingleProcessCoordinator,
} from "../../../../../packages/live-collection/node_modules/@tanstack/db-sqlite-persistence-core/dist/esm/index.js"
import { makeNodeSqliteDriver } from "./node-sqlite-driver.js"

/**
 * Builds the persistence value shared by all client collections. Adapters are cached by
 * mismatch policy and schema version; threading each collection's schema version here is
 * required for the sync-present reset policy to invalidate incompatible persisted rows.
 */
export const makeSqlitePersistence = (driver: SQLiteDriver): PersistedCollectionPersistence => {
  const coordinator = new SingleProcessCoordinator()
  const adapters = new Map<string, PersistenceAdapter>()
  const policyFor = (mode: PersistedCollectionMode): "sync-present-reset" | "sync-absent-error" =>
    mode === "sync-present" ? "sync-present-reset" : "sync-absent-error"

  const adapterFor = (mode: PersistedCollectionMode, schemaVersion: number | undefined) => {
    const policy = policyFor(mode)
    const key = `${policy}|${schemaVersion ?? "default"}`
    let adapter = adapters.get(key)
    if (adapter === undefined) {
      adapter = createSQLiteCorePersistenceAdapter({
        driver,
        schemaMismatchPolicy: policy,
        ...(schemaVersion === undefined ? {} : { schemaVersion }),
      })
      adapters.set(key, adapter)
    }
    return adapter
  }

  const forCollection = (
    mode: PersistedCollectionMode,
    schemaVersion: number | undefined,
  ): PersistedCollectionPersistence => ({ adapter: adapterFor(mode, schemaVersion), coordinator })

  return {
    ...forCollection("sync-absent", undefined),
    resolvePersistenceForCollection: ({ mode, schemaVersion }) => forCollection(mode, schemaVersion),
    resolvePersistenceForMode: (mode) => forCollection(mode, undefined),
  }
}

export const makeNodeSqlitePersistence = (): PersistedCollectionPersistence =>
  makeSqlitePersistence(makeNodeSqliteDriver())
