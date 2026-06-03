import { Duration, Effect, Schema } from "effect"
import type { Layer } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelId } from "@triargos/live-collection-protocol"
import { effectCollectionOptions } from "../src/persistence/effect-collection.js"
import { PersistenceBase } from "../src/persistence/persistence-base.js"
import type { LiveCollection } from "../src/persistence/live-collection.js"
import { makeNodeSqliteDriver } from "./node-sqlite-driver.js"

// A tiny entity for the gate. `id` is the key; `name` is payload we assert survives a reload.
const Row = Schema.Struct({ id: Schema.String, name: Schema.String })
type Row = typeof Row.Type
const k = (s: string) => ModelId.make(s)

// T infers from `schema`; no explicit generic needed.
const options = {
  collectionId: "gate-row",
  schema: Row,
  getKey: (r: Row) => k(r.id),
} as const

type Options<T extends object> = Parameters<typeof effectCollectionOptions<T>>[0]

const freshBase = (): Layer.Layer<PersistenceBase> =>
  PersistenceBase.layerSqliteDriver(makeNodeSqliteDriver())

/** Mount, await readiness, run `use`, then dispose (scope close → cleanup). One "reload". */
const withMount = <T extends object, A>(
  base: Layer.Layer<PersistenceBase>,
  opts: Options<T>,
  use: (coll: LiveCollection<T>) => Effect.Effect<A>,
): Effect.Effect<A> =>
  effectCollectionOptions(opts).pipe(
    Effect.flatMap((coll) =>
      Effect.promise(() => coll.preload()).pipe(Effect.flatMap(() => use(coll))),
    ),
    Effect.scoped,
    Effect.provide(base),
  )

/**
 * Synced writes persist fire-and-forget (the alpha exposes no durability handle), so a write that
 * just disposed may not be on disk for a few ticks — its persist survives dispose but completes
 * asynchronously. So we poll: remount + read until `predicate` holds, or fail after a bound. Calling
 * this between write phases also serializes persistence, so an orphaned persist can't clobber a later
 * write across remounts (a test-only race — production is one long-lived mount).
 */
const reloadUntil = <T extends object, A>(
  base: Layer.Layer<PersistenceBase>,
  opts: Options<T>,
  read: (coll: LiveCollection<T>) => A,
  predicate: (a: A) => boolean,
): Effect.Effect<A> => {
  const attempt = (): Effect.Effect<A> =>
    withMount(base, opts, (c) => Effect.sync(() => read(c))).pipe(
      Effect.flatMap((a) =>
        predicate(a)
          ? Effect.succeed(a)
          : Effect.sleep(Duration.millis(5)).pipe(Effect.zipRight(attempt())),
      ),
    )
  return attempt().pipe(
    Effect.timeoutFail({
      duration: Duration.seconds(2),
      onTimeout: () => new Error("persisted state did not settle within 2s"),
    }),
    Effect.orDie,
  )
}

describe("effectCollectionOptions — A.3 persistence gate", () => {
  // Step 1: hydrate-from-storage. A row written on one mount is present on a fresh cold mount over
  // the same DB, before any network — the local SQLite base hydrates the collection on its own.
  it.live("a fresh mount rehydrates a row a previous mount persisted", () =>
    Effect.gen(function* () {
      const base = freshBase()
      yield* withMount(base, options, (c) => c.utils.writeSynced({ id: "r1", name: "alpha" }))
      const present = yield* reloadUntil(base, options, (c) => c.has(k("r1")), (p) => p === true)
      assert.strictEqual(present, true)
    }))

  // Step 3: deltas persist + converge. Writes accumulate on top of an existing persisted base, and a
  // later write to the same key wins — persistence is an ongoing delta log, not a one-shot snapshot.
  it.live("writes accumulate on the persisted base and the latest value wins", () =>
    Effect.gen(function* () {
      const base = freshBase()
      yield* withMount(base, options, (c) => c.utils.writeSynced({ id: "r1", name: "a" }))
      yield* reloadUntil(base, options, (c) => c.has(k("r1")), (p) => p) // serialize before phase 2
      yield* withMount(base, options, (c) =>
        Effect.all(
          [c.utils.writeSynced({ id: "r2", name: "z" }), c.utils.writeSynced({ id: "r1", name: "b" })],
          { discard: true },
        ),
      )
      const [n1, n2] = yield* reloadUntil(
        base,
        options,
        (c) => [c.get(k("r1"))?.name, c.get(k("r2"))?.name] as const,
        ([a, b]) => a === "b" && b === "z",
      )
      assert.strictEqual(n1, "b") // latest value won, durably
      assert.strictEqual(n2, "z")
    }))

  // The delete half of the seam: removal persists, and a re-delete is a harmless no-op (the
  // dispatcher fans deletes across every mounted collection of a model, so absent-key deletes happen).
  it.live("deleteSynced removes durably and re-delete is idempotent", () =>
    Effect.gen(function* () {
      const base = freshBase()
      yield* withMount(base, options, (c) =>
        Effect.all(
          [c.utils.writeSynced({ id: "r1", name: "a" }), c.utils.writeSynced({ id: "r2", name: "b" })],
          { discard: true },
        ),
      )
      yield* reloadUntil(base, options, (c) => c.has(k("r1")) && c.has(k("r2")), (p) => p)
      yield* withMount(base, options, (c) =>
        Effect.all([c.utils.deleteSynced(k("r1")), c.utils.deleteSynced(k("r1"))], { discard: true }),
      )
      const [hasR1, hasR2] = yield* reloadUntil(
        base,
        options,
        (c) => [c.has(k("r1")), c.has(k("r2"))] as const,
        ([a]) => a === false,
      )
      assert.strictEqual(hasR1, false) // removal persisted
      assert.strictEqual(hasR2, true) // sibling untouched
    }))

  // DEC-A6 end-to-end: a schema change (→ a different derived schemaVersion) drops the local base on
  // the next mount of the SAME collectionId and rebuilds it — no manual migration, no stale rows.
  it.live("a schema change resets the persisted base (dump-and-rebuild)", () =>
    Effect.gen(function* () {
      const base = freshBase()
      const v1: Options<Row> = { collectionId: "evolving", schema: Row, getKey: (r) => k(r.id) }
      yield* withMount(base, v1, (c) => c.utils.writeSynced({ id: "r1", name: "a" }))
      yield* reloadUntil(base, v1, (c) => c.has(k("r1")), (p) => p) // ensure v1 row is durable first

      const RowV2 = Schema.Struct({ id: Schema.String, name: Schema.String, extra: Schema.String })
      const v2: Options<typeof RowV2.Type> = {
        collectionId: "evolving", // same table, different schema → schemaVersion mismatch → reset
        schema: RowV2,
        getKey: (r) => k(r.id),
      }
      const present = yield* reloadUntil(base, v2, (c) => c.has(k("r1")), (p) => p === false)
      assert.strictEqual(present, false) // base was dumped on the version mismatch
    }))
})
