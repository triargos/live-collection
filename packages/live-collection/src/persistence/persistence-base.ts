import { Context, Layer } from "effect"
import {
  type PersistedCollectionMode,
  type PersistedCollectionPersistence,
  type PersistenceAdapter,
  type SQLiteDriver,
  createSQLiteCorePersistenceAdapter,
  SingleProcessCoordinator,
} from "@tanstack/db-sqlite-persistence-core"

/**
 * The shared, app-wide SQLite-backed persistence every collection reuses (each passes its own
 * `collectionId`; they share one DB). The seam sits at the {@link PersistedCollectionPersistence}
 * level, not the raw driver — `BrowserWASQLiteDriver` is internal to the browser adapter and the
 * package `exports` map forbids deep imports, so prod can never hand us a raw `SQLiteDriver`
 * (DESIGN §3 / DEC-A7).
 */
export interface PersistenceBaseShape {
  /** The TanStack persistence object the factory hands to `persistedCollectionOptions`. */
  readonly persistence: PersistedCollectionPersistence
}

/**
 * The seam: `yield* PersistenceBase`. The browser/OPFS `layer` lives in a separate module
 * (`persistence-base-opfs.ts`) so this one stays node-safe — importing it never loads wa-sqlite.
 */
export class PersistenceBase extends Context.Tag("PersistenceBase")<
  PersistenceBase,
  PersistenceBaseShape
>() {
  /**
   * Test/node adapter: the caller supplies a raw {@link SQLiteDriver} (e.g. a `node:sqlite`
   * wrapper); this replicates `createBrowserWASQLitePersistence`'s logic over that driver — an
   * adapter cache keyed by `(policy, schemaVersion)` plus `resolvePersistenceForCollection`, which is
   * what threads each collection's `schemaVersion` to a version-aware adapter (without it, the
   * per-collection version is dropped and DEC-A6's reset never fires). Faithful to prod; only the
   * driver differs. The library ships no node driver — it's test infra (DEC-A8).
   */
  static readonly layerSqliteDriver = (driver: SQLiteDriver): Layer.Layer<PersistenceBase> => {
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

    const persistence: PersistedCollectionPersistence = {
      ...forCollection("sync-absent", undefined),
      resolvePersistenceForCollection: ({ mode, schemaVersion }) => forCollection(mode, schemaVersion),
      resolvePersistenceForMode: (mode) => forCollection(mode, undefined),
    }
    return Layer.succeed(PersistenceBase, { persistence })
  }
}
