import { AsyncLocalStorage } from "node:async_hooks"
import { DatabaseSync } from "node:sqlite"
import type { SQLiteDriver } from "@tanstack/db-sqlite-persistence-core"

/**
 * A {@link SQLiteDriver} over Node's built-in `node:sqlite` — **test infrastructure only**, never
 * shipped (the library is frontend-only; DEC-A8). It runs the *same* core persistence adapter the
 * browser path runs, so a headless node test exercises the real persist/hydrate/reset semantics.
 *
 * `node:sqlite` is synchronous; each method wraps a call in a resolved promise. Collections sharing
 * the connection persist concurrently, so top-level transactions are serialized; `AsyncLocalStorage`
 * tells true nesting apart, which uses savepoints. Mirrors pi-demo's server test driver.
 */
export class NodeSqliteDriver implements SQLiteDriver {
  readonly #transactionDepth = new AsyncLocalStorage<number>()
  #transactionTail: Promise<void> = Promise.resolve()

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
    const parentDepth = this.#transactionDepth.getStore()
    if (parentDepth !== undefined) return this.#runTransaction(parentDepth + 1, fn)

    const result = this.#transactionTail.then(() => this.#runTransaction(0, fn))
    this.#transactionTail = result.then(() => undefined, () => undefined)
    return result
  }

  #runTransaction = <T>(depth: number, fn: (tx: SQLiteDriver) => Promise<T>): Promise<T> =>
    this.#transactionDepth.run(depth, () => {
      const open = depth === 0 ? "BEGIN" : `SAVEPOINT s${depth}`
      const release = depth === 0 ? "COMMIT" : `RELEASE s${depth}`
      const rollback = depth === 0 ? "ROLLBACK" : `ROLLBACK TO s${depth}; RELEASE s${depth}`
      this.db.exec(open)
      return fn(this).then(
        (result) => {
          this.db.exec(release)
          return result
        },
        (error: unknown) => {
          this.db.exec(rollback)
          throw error
        },
      )
    })
}

/** An in-memory `node:sqlite` driver. One connection holds the data across the two adapter builds a
 *  "reload" test makes (mount → dispose → re-mount), so a fresh, cold adapter reads persisted rows. */
export const makeNodeSqliteDriver = (): NodeSqliteDriver =>
  new NodeSqliteDriver(new DatabaseSync(":memory:"))
