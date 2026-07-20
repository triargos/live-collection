import { Effect, Option } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { Epoch, ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import { SyncJournal, type JournalEvent, SchemaVersion, scopedKey } from "@triargos/live-collection"

// The durable SyncJournal.layer over **real IndexedDB** — the one thing node cannot prove (jsdom has no
// IDB, and even a polyfill can't model a fresh process over the same database). This mirrors the memory
// adapter's spec (`packages/live-collection/test/sync-journal.test.ts`) and adds the payoff the memory
// adapter structurally can't: a second layer scope over the SAME database name reads back what the first
// wrote — replay-on-mount survives a reload / workspace-switch.

const sid = (s: string) => SyncId.make(s)
const version = SchemaVersion.make(1)
const Webhook = ModelName.make("Webhook")
const insert = (syncId: string, scope: string, id: string): JournalEvent => ({
  syncId: sid(syncId),
  modelName: Webhook,
  tag: "Insert",
  modelId: ModelId.make(id),
  data: Option.some({ id, scope }),
})
const del = (syncId: string, id: string): JournalEvent => ({
  syncId: sid(syncId),
  modelName: Webhook,
  tag: "Delete",
  modelId: ModelId.make(id),
  data: Option.none(),
})

/** One "session" over a named IDB database: opens its own layer scope and closes it on the way out. */
const session = <A>(databaseName: string, use: Effect.Effect<A, never, SyncJournal>): Effect.Effect<A> =>
  use.pipe(Effect.provide(SyncJournal.layer({ databaseName })))

/** A fresh, unique database per test so reruns never see stale IDB state. */
const freshDb = () => `sync-journal-${crypto.randomUUID()}`

describe("SyncJournal.layer (IndexedDB, browser)", () => {
  it.live("append dedupes by syncId — a re-delivered event upserts instead of duplicating", () =>
    session(
      freshDb(),
      Effect.gen(function* () {
        const journal = yield* SyncJournal
        yield* journal.append([insert("1", "org-1", "w1")])
        yield* journal.append([insert("1", "org-1", "w1-v2")]) // same syncId, re-delivered on reconnect overlap
        const rows = yield* journal.read({ modelName: Webhook, since: sid("0") })
        assert.strictEqual(rows.length, 1)
        assert.strictEqual(rows[0]!.modelId, ModelId.make("w1-v2")) // the later append wins
      }),
    ))

  it.live("read returns every event for the model, syncId-ordered, after `since`", () =>
    session(
      freshDb(),
      Effect.gen(function* () {
        const journal = yield* SyncJournal
        yield* journal.append([insert("2", "org-1", "a"), insert("3", "org-2", "b"), del("4", "a"), insert("1", "org-1", "z")])
        const rows = yield* journal.read({ modelName: Webhook, since: sid("1") })
        assert.deepStrictEqual(
          rows.map((r) => r.syncId),
          [sid("2"), sid("3"), sid("4")],
        )
      }),
    ))

  it.live("read sorts by syncId magnitude, not lexicographically (10 after 2, not before)", () =>
    session(
      freshDb(),
      Effect.gen(function* () {
        const journal = yield* SyncJournal
        yield* journal.append([insert("2", "org-1", "a"), insert("10", "org-1", "b"), insert("1", "org-1", "c")])
        const rows = yield* journal.read({ modelName: Webhook, since: sid("0") })
        assert.deepStrictEqual(
          rows.map((r) => r.syncId),
          [sid("1"), sid("2"), sid("10")], // string-sorted this would be ["1","10","2"]
        )
      }),
    ))

  it.live("the journal survives a reload — a fresh layer scope reads back what the prior scope appended", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      yield* session(databaseName, SyncJournal.pipe(Effect.flatMap((journal) => journal.append([insert("5", "org-1", "x")]))))
      // A second, independent layer scope over the SAME database — the "reload".
      const rows = yield* session(
        databaseName,
        SyncJournal.pipe(Effect.flatMap((journal) => journal.read({ modelName: Webhook, since: sid("0") }))),
      )
      assert.deepStrictEqual(rows.map((r) => r.modelId), [ModelId.make("x")])
    }))

  it.live("prune keeps the newest `maxEventsPerModel` and records the max deleted syncId as the floor (durable)", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      yield* session(
        databaseName,
        Effect.gen(function* () {
          const journal = yield* SyncJournal
          yield* journal.append(["1", "2", "3", "4", "5"].map((s, i) => insert(s, "org-1", `w${i}`)))
          // A record at "0" keeps every row above the dead-weight line — this exercises the count cap.
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("0") })
          yield* journal.prune({ maxEventsPerModel: 3, maxEventsTotal: 100 }) // keep 3,4,5 ; delete 1,2 ; floor ⇒ 2
          const kept = yield* journal.read({ modelName: Webhook, since: sid("0") })
          assert.deepStrictEqual(kept.map((r) => r.syncId), [sid("3"), sid("4"), sid("5")])
        }),
      )
      // Floor persists across the reload (a remount must still see the prune boundary).
      const floor = yield* session(databaseName, SyncJournal.pipe(Effect.flatMap((journal) => journal.floor(Webhook))))
      assert.deepStrictEqual(floor, Option.some(sid("2")))
    }))

  it.live("prune deletes rows every collection has applied — durably, with the floor still None", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      yield* session(
        databaseName,
        Effect.gen(function* () {
          const journal = yield* SyncJournal
          yield* journal.append(["1", "2", "3"].map((s, i) => insert(s, "org-1", `w${i}`)))
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("2") })
          yield* journal.prune({ maxEventsPerModel: 100, maxEventsTotal: 100 }) // 1,2 ≤ min ⇒ dead weight
        }),
      )
      // The deletion is durable AND floor-neutral: dead weight can never force a Snapshot.
      yield* session(
        databaseName,
        Effect.gen(function* () {
          const journal = yield* SyncJournal
          const rows = yield* journal.read({ modelName: Webhook, since: sid("0") })
          assert.deepStrictEqual(rows.map((r) => r.syncId), [sid("3")])
          assert.deepStrictEqual(yield* journal.floor(Webhook), Option.none())
        }),
      )
    }))

  it.live("last-applied mark + lastResync advance monotonically and survive a reload", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      yield* session(
        databaseName,
        Effect.gen(function* () {
          const journal = yield* SyncJournal
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("5") })
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("3") }) // older ⇒ ignored
          yield* journal.setLastResync(sid("7"))
          yield* journal.setLastResync(sid("2")) // older ⇒ ignored
        }),
      )
      const [lastApplied, resync] = yield* session(
        databaseName,
        Effect.gen(function* () {
          const journal = yield* SyncJournal
          return [yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), yield* journal.getLastResync] as const
        }),
      )
      assert.deepStrictEqual(lastApplied, Option.some(sid("5")))
      assert.deepStrictEqual(resync, Option.some(sid("7")))
    }))

  it.live("a write under a new schema version supersedes the old record — across a reload", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      const oldVersion = SchemaVersion.make(1)
      const newVersion = SchemaVersion.make(2)
      yield* session(
        databaseName,
        SyncJournal.pipe(
          Effect.flatMap((journal) => journal.setCollectionLastAppliedSyncId({ key, schemaVersion: oldVersion, at: sid("10") })),
        ),
      )
      // The "reload after a schema change": before the drain writes anything, the dumped
      // table's mark must not be found under the new version.
      const stale = yield* session(
        databaseName,
        SyncJournal.pipe(Effect.flatMap((journal) => journal.getCollectionLastAppliedSyncId({ key, schemaVersion: newVersion }))),
      )
      assert.deepStrictEqual(stale, Option.none()) // new version ⇒ no mark ⇒ mount snapshots
      // The first new-version write replaces the record outright — even with a smaller syncId
      // (monotonicity holds only within a version) — and the old version's mark is gone.
      const [fresh, orphaned] = yield* session(
        databaseName,
        Effect.gen(function* () {
          const journal = yield* SyncJournal
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: newVersion, at: sid("4") })
          return [
            yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: newVersion }),
            yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: oldVersion }),
          ] as const
        }),
      )
      assert.deepStrictEqual(fresh, Option.some(sid("4")))
      assert.deepStrictEqual(orphaned, Option.none()) // superseded, not merely shadowed
    }))

  it.live("epoch survives a reload — a fresh layer scope reads back what the prior scope stored", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      yield* session(databaseName, SyncJournal.pipe(Effect.flatMap((journal) => journal.setEpoch(Epoch.make("epoch-a")))))
      const stored = yield* session(databaseName, SyncJournal.pipe(Effect.flatMap((journal) => journal.getEpoch)))
      assert.deepStrictEqual(stored, Option.some(Epoch.make("epoch-a")))
    }))

  it.live("reset wipes events + every meta record in one pass — and the wipe survives a reload", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      yield* session(
        databaseName,
        Effect.gen(function* () {
          const journal = yield* SyncJournal
          yield* journal.append(["1", "2", "3"].map((s, i) => insert(s, "org-1", `w${i}`)))
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("0") })
          yield* journal.prune({ maxEventsPerModel: 2, maxEventsTotal: 100 }) // floor ⇒ 1
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("3") })
          yield* journal.setLastResync(sid("2"))
          yield* journal.setEpoch(Epoch.make("old"))
          yield* journal.reset
        }),
      )
      // The "reload": a fresh scope over the same database must find cold-start state.
      yield* session(
        databaseName,
        Effect.gen(function* () {
          const journal = yield* SyncJournal
          assert.deepStrictEqual(yield* journal.read({ modelName: Webhook, since: sid("0") }), [])
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())
          assert.deepStrictEqual(yield* journal.floor(Webhook), Option.none())
          assert.deepStrictEqual(yield* journal.getLastResync, Option.none())
          assert.deepStrictEqual(yield* journal.getEpoch, Option.none())
        }),
      )
    }))
})
