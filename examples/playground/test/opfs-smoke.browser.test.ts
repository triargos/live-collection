import { Duration, Effect, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { createCollection } from "@tanstack/db"
import { ModelId } from "@triargos/live-collection-protocol"
import { deriveSchemaVersion, liveCollectionOptions, type LiveCollection, type SyncWrite } from "@triargos/live-collection"
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
  type PersistedCollectionPersistence,
  persistedCollectionOptions,
} from "@tanstack/browser-db-sqlite-persistence"

// The A.3 persistence gate over **real OPFS** — the one thing node cannot prove. This mirrors the node
// gate (`live-collection-options.test.ts`) exactly, swapping only the persistence builder: node's
// `makeNodeSqlitePersistence()` → the official `createBrowserWASQLitePersistence` over an OPFS database.
// What it asserts (spec §A.1): a write persists, and a FRESH mount rehydrates it from storage with no
// re-list (the collection's network-free `sync` never lists; only OPFS feeds the cold mount).

const Row = Schema.Struct({ id: Schema.String, name: Schema.String })
type Row = typeof Row.Type
const k = (s: string) => ModelId.make(s)

interface Opts<T extends object> {
  readonly id: string
  readonly schema: Schema.Codec<T, any>
  readonly getKey: (r: T) => ModelId
}
const rowOpts: Opts<Row> = { id: "gate-row", schema: Row, getKey: (r) => k(r.id) }

/** A fresh OPFS-backed persistence value over a unique database (so reruns don't see stale OPFS state). */
const makeOpfsPersistence = (): Effect.Effect<PersistedCollectionPersistence> =>
  Effect.promise(async () => {
    const database = await openBrowserWASQLiteOPFSDatabase({
      databaseName: `smoke-${crypto.randomUUID()}.sqlite`,
    })
    return createBrowserWASQLitePersistence({ database })
  })

/** Build one native collection over the shared persistence — exactly the production composition. */
const mount = <T extends object>(persistence: PersistedCollectionPersistence, opts: Opts<T>): LiveCollection<T> =>
  createCollection(
    persistedCollectionOptions<T, ModelId, never, SyncWrite<T>>({
      persistence,
      id: opts.id,
      schemaVersion: deriveSchemaVersion(opts.schema),
      ...liveCollectionOptions({ getKey: opts.getKey }),
    }),
  )

/** Mount, await readiness, run `use`, then `cleanup` (one "reload"). */
const withMount = <T extends object, A>(
  persistence: PersistedCollectionPersistence,
  opts: Opts<T>,
  use: (coll: LiveCollection<T>) => Effect.Effect<A>,
): Effect.Effect<A> =>
  Effect.gen(function* () {
    const coll = mount<T>(persistence, opts)
    yield* Effect.promise(() => coll.preload())
    const result = yield* use(coll)
    yield* Effect.promise(() => coll.cleanup())
    return result
  })

/** Synced writes persist fire-and-forget; poll a remount + read until `predicate` holds (or time out). */
const reloadUntil = <T extends object, A>(
  persistence: PersistedCollectionPersistence,
  opts: Opts<T>,
  read: (coll: LiveCollection<T>) => A,
  predicate: (a: A) => boolean,
): Effect.Effect<A> => {
  const attempt = (): Effect.Effect<A> =>
    withMount(persistence, opts, (c) => Effect.sync(() => read(c))).pipe(
      Effect.flatMap((a) =>
        predicate(a) ? Effect.succeed(a) : Effect.sleep(Duration.millis(10)).pipe(Effect.andThen(attempt())),
      ),
    )
  return attempt().pipe(
    Effect.timeoutOrElse({ duration: Duration.seconds(10), orElse: () => Effect.fail(new Error("persisted OPFS state did not settle within 10s")) }),
    Effect.orDie,
  )
}

describe("OPFS A.3 gate (browser)", () => {
  it.live("a fresh mount rehydrates a row a previous mount persisted to OPFS", () =>
    Effect.gen(function* () {
      const persistence = yield* makeOpfsPersistence()
      yield* withMount(persistence, rowOpts, (c) => c.utils.writeSynced({ id: "r1", name: "alpha" }))
      const present = yield* reloadUntil(persistence, rowOpts, (c) => c.has(k("r1")), (p) => p === true)
      assert.strictEqual(present, true)
    }))

  it.live("catchup deltas accumulate on the OPFS base; latest value wins", () =>
    Effect.gen(function* () {
      const persistence = yield* makeOpfsPersistence()
      yield* withMount(persistence, rowOpts, (c) => c.utils.writeSynced({ id: "r1", name: "a" }))
      yield* reloadUntil(persistence, rowOpts, (c) => c.has(k("r1")), (p) => p)
      yield* withMount(persistence, rowOpts, (c) =>
        Effect.all(
          [c.utils.writeSynced({ id: "r2", name: "z" }), c.utils.writeSynced({ id: "r1", name: "b" })],
          { discard: true },
        ),
      )
      const [n1, n2] = yield* reloadUntil(
        persistence,
        rowOpts,
        (c) => [c.get(k("r1"))?.name, c.get(k("r2"))?.name] as const,
        ([a, b]) => a === "b" && b === "z",
      )
      assert.strictEqual(n1, "b")
      assert.strictEqual(n2, "z")
    }))
})
