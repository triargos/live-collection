import { AsyncLocalStorage } from "node:async_hooks"
import { DatabaseSync } from "node:sqlite"
import type { SQLiteDriver } from "../../../../../packages/live-collection/node_modules/@tanstack/db-sqlite-persistence-core/dist/esm/index.js"

/**
 * A SQLite driver over Node's built-in `node:sqlite`. Calls are synchronous underneath
 * but expose the promise-based adapter contract. Nested transactions use savepoints so
 * persistence operations can safely compose transactions.
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

    // Multiple collections share one connection and can persist concurrently. Serialize top-level
    // transactions while AsyncLocalStorage distinguishes true nesting, which must use savepoints.
    const result = this.#transactionTail.then(() => this.#runTransaction(0, fn))
    this.#transactionTail = result.then(() => undefined, () => undefined)
    return result
  }

  #runTransaction = async <T>(
    depth: number,
    fn: (tx: SQLiteDriver) => Promise<T>,
  ): Promise<T> => this.#transactionDepth.run(depth, async () => {
    const open = depth === 0 ? "BEGIN" : `SAVEPOINT s${depth}`
    const release = depth === 0 ? "COMMIT" : `RELEASE s${depth}`
    const rollback = depth === 0 ? "ROLLBACK" : `ROLLBACK TO s${depth}; RELEASE s${depth}`
    this.db.exec(open)
    try {
      const result = await fn(this)
      this.db.exec(release)
      return result
    } catch (error) {
      this.db.exec(rollback)
      throw error
    }
  })
}

/** One connection retains the in-memory database for the persistence value's lifetime. */
export const makeNodeSqliteDriver = (): NodeSqliteDriver =>
  new NodeSqliteDriver(new DatabaseSync(":memory:"))
