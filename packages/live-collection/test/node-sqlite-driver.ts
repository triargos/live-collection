import { DatabaseSync } from "node:sqlite"
import type { SQLiteDriver } from "@tanstack/db-sqlite-persistence-core"

/**
 * A {@link SQLiteDriver} over Node's built-in `node:sqlite` — **test infrastructure only**, never
 * shipped (the library is frontend-only; DEC-A8). It runs the *same* core persistence adapter the
 * browser path runs, so a headless node test exercises the real persist/hydrate/reset semantics.
 *
 * `node:sqlite` is synchronous; each method wraps a call in a resolved promise. Nested transactions
 * use savepoints so the adapter's transaction usage composes.
 */
export class NodeSqliteDriver implements SQLiteDriver {
  #depth = 0

  constructor(private readonly db: DatabaseSync) {}

  exec = (sql: string): Promise<void> => {
    this.db.exec(sql)
    return Promise.resolve()
  }

  query = <T>(sql: string, params: ReadonlyArray<unknown> = []): Promise<ReadonlyArray<T>> =>
    Promise.resolve(
      this.db.prepare(sql).all(...(params as Array<never>)) as unknown as ReadonlyArray<T>,
    )

  run = (sql: string, params: ReadonlyArray<unknown> = []): Promise<void> => {
    this.db.prepare(sql).run(...(params as Array<never>))
    return Promise.resolve()
  }

  transaction = <T>(fn: (tx: SQLiteDriver) => Promise<T>): Promise<T> => {
    const d = this.#depth++
    const open = d === 0 ? "BEGIN" : `SAVEPOINT s${d}`
    const release = d === 0 ? "COMMIT" : `RELEASE s${d}`
    const rollback = d === 0 ? "ROLLBACK" : `ROLLBACK TO s${d}; RELEASE s${d}`
    this.db.exec(open)
    return Promise.resolve()
      .then(() => fn(this))
      .then((result) => {
        this.db.exec(release)
        return result
      })
      .catch((error: unknown) => {
        this.db.exec(rollback)
        throw error
      })
      .finally(() => {
        this.#depth--
      })
  }
}

/** An in-memory `node:sqlite` driver. One connection holds the data across the two adapter builds a
 *  "reload" test makes (mount → dispose → re-mount), so a fresh, cold adapter reads persisted rows. */
export const makeNodeSqliteDriver = (): NodeSqliteDriver =>
  new NodeSqliteDriver(new DatabaseSync(":memory:"))
