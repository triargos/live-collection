import {
  type PersistedCollectionMode,
  type PersistedCollectionPersistence,
  type PersistenceAdapter,
  type SQLiteDriver,
  createSQLiteCorePersistenceAdapter,
  SingleProcessCoordinator,
} from "@tanstack/db-sqlite-persistence-core"
import { makeNodeSqliteDriver } from "./node-sqlite-driver.js"

/**
 * Builds the shared {@link PersistedCollectionPersistence} **value** every collection reuses — the
 * thing an app makes once and passes to `makeLiveRuntime` (prod uses `createOpfsSQLitePersistence`;
 * this is the node/test analogue, DEC-R3/DEC-A8). It replicates `createBrowserWASQLitePersistence`'s
 * logic over a raw {@link SQLiteDriver}: an adapter cache keyed by `(policy, schemaVersion)` plus
 * `resolvePersistenceForCollection`, which threads each collection's `schemaVersion` to a version-aware
 * adapter (without it, DEC-A6's reset never fires). Test infrastructure only — never shipped.
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

/** A fresh in-memory persistence value over one `node:sqlite` connection (survives mount→dispose→remount). */
export const makeNodeSqlitePersistence = (): PersistedCollectionPersistence =>
  makeSqlitePersistence(makeNodeSqliteDriver())
