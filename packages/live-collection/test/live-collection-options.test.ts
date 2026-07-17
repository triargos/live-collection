import { Duration, Effect, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { createCollection } from "@tanstack/db"
import { persistedCollectionOptions, type PersistedCollectionPersistence } from "@tanstack/db-sqlite-persistence-core"
import { ModelId } from "@triargos/live-collection-protocol"
import type { LiveCollection } from "../src/persistence/live-collection.js"
import { liveCollectionOptions } from "../src/persistence/live-collection-options.js"
import { deriveSchemaVersion } from "../src/persistence/schema-version.js"
import type { SyncWrite } from "../src/dispatch/sync-write.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

// The A.3 persistence gate over the real composition `defineCollection`'s `make` builds:
// createCollection(persistedCollectionOptions({ persistence, id, schemaVersion, ...liveCollectionOptions })).
const Row = Schema.Struct({ id: Schema.String, name: Schema.String })
type Row = typeof Row.Type
const k = (s: string) => ModelId.make(s)

interface Opts<T extends object> {
  readonly id: string
  readonly schema: Schema.Codec<T, any>
  readonly getKey: (r: T) => ModelId
}
const rowOpts: Opts<Row> = { id: "gate-row", schema: Row, getKey: (r) => k(r.id) }

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

/**
 * Synced writes persist fire-and-forget (the alpha exposes no durability handle), so a write that just
 * cleaned up may not be on disk for a few ticks. Poll: remount + read until `predicate` holds. Serializes
 * persistence between write phases too, so an orphaned persist can't clobber a later write across remounts.
 */
const reloadUntil = <T extends object, A>(
  persistence: PersistedCollectionPersistence,
  opts: Opts<T>,
  read: (coll: LiveCollection<T>) => A,
  predicate: (a: A) => boolean,
): Effect.Effect<A> => {
  const attempt = (): Effect.Effect<A> =>
    withMount(persistence, opts, (c) => Effect.sync(() => read(c))).pipe(
      Effect.flatMap((a) =>
        predicate(a) ? Effect.succeed(a) : Effect.sleep(Duration.millis(5)).pipe(Effect.andThen(attempt())),
      ),
    )
  return attempt().pipe(
    Effect.timeoutOrElse({ duration: Duration.seconds(2), orElse: () => Effect.fail(new Error("persisted state did not settle within 2s")) }),
    Effect.orDie,
  )
}

describe("liveCollectionOptions — A.3 persistence gate", () => {
  it.live("a fresh mount rehydrates a row a previous mount persisted", () =>
    Effect.gen(function* () {
      const persistence = makeNodeSqlitePersistence()
      yield* withMount(persistence, rowOpts, (c) => c.utils.writeSynced({ id: "r1", name: "alpha" }))
      const present = yield* reloadUntil(persistence, rowOpts, (c) => c.has(k("r1")), (p) => p === true)
      assert.strictEqual(present, true)
    }))

  it.live("writes accumulate on the persisted base and the latest value wins", () =>
    Effect.gen(function* () {
      const persistence = makeNodeSqlitePersistence()
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

  it.live("deleteSynced removes durably and re-delete is idempotent", () =>
    Effect.gen(function* () {
      const persistence = makeNodeSqlitePersistence()
      yield* withMount(persistence, rowOpts, (c) =>
        Effect.all(
          [c.utils.writeSynced({ id: "r1", name: "a" }), c.utils.writeSynced({ id: "r2", name: "b" })],
          { discard: true },
        ),
      )
      yield* reloadUntil(persistence, rowOpts, (c) => c.has(k("r1")) && c.has(k("r2")), (p) => p)
      yield* withMount(persistence, rowOpts, (c) =>
        Effect.all([c.utils.deleteSynced(k("r1")), c.utils.deleteSynced(k("r1"))], { discard: true }),
      )
      const [hasR1, hasR2] = yield* reloadUntil(
        persistence,
        rowOpts,
        (c) => [c.has(k("r1")), c.has(k("r2"))] as const,
        ([a]) => a === false,
      )
      assert.strictEqual(hasR1, false)
      assert.strictEqual(hasR2, true)
    }))

  it.live("replaceSynced replaces the whole baseline durably (truncate + writes, one tx)", () =>
    Effect.gen(function* () {
      const persistence = makeNodeSqlitePersistence()
      yield* withMount(persistence, rowOpts, (c) =>
        Effect.all(
          [c.utils.writeSynced({ id: "r1", name: "a" }), c.utils.writeSynced({ id: "r2", name: "b" })],
          { discard: true },
        ),
      )
      yield* reloadUntil(persistence, rowOpts, (c) => c.has(k("r1")) && c.has(k("r2")), (p) => p)
      // The new baseline: r2 (new value) + r3. r1 is absent from it ⇒ must be gone — durably.
      yield* withMount(persistence, rowOpts, (c) =>
        c.utils.replaceSynced([
          { id: "r2", name: "z" },
          { id: "r3", name: "c" },
        ]),
      )
      const [hasR1, n2, hasR3] = yield* reloadUntil(
        persistence,
        rowOpts,
        (c) => [c.has(k("r1")), c.get(k("r2"))?.name, c.has(k("r3"))] as const,
        ([a, b, c3]) => a === false && b === "z" && c3 === true,
      )
      assert.strictEqual(hasR1, false) // truncated away
      assert.strictEqual(n2, "z") // replaced value won
      assert.strictEqual(hasR3, true) // new row present
    }))

  it.live("a schema change resets the persisted base (dump-and-rebuild)", () =>
    Effect.gen(function* () {
      const persistence = makeNodeSqlitePersistence()
      const v1: Opts<Row> = { id: "evolving", schema: Row, getKey: (r) => k(r.id) }
      yield* withMount(persistence, v1, (c) => c.utils.writeSynced({ id: "r1", name: "a" }))
      yield* reloadUntil(persistence, v1, (c) => c.has(k("r1")), (p) => p)

      const RowV2 = Schema.Struct({ id: Schema.String, name: Schema.String, extra: Schema.String })
      const v2: Opts<typeof RowV2.Type> = { id: "evolving", schema: RowV2, getKey: (r) => k(r.id) }
      const present = yield* reloadUntil(persistence, v2, (c) => c.has(k("r1")), (p) => p === false)
      assert.strictEqual(present, false) // base dumped on the version mismatch
    }))
})
