import { Effect, Option } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import { EventLogStore, type LoggedEvent, scopedKey } from "@triargos/live-collection"

// The durable EventLogStore.layer over **real IndexedDB** — the one thing node cannot prove (jsdom has no
// IDB, and even a polyfill can't model a fresh process over the same database). This mirrors the memory
// adapter's spec (`packages/live-collection/test/event-log-store.test.ts`) and adds the payoff the memory
// adapter structurally can't: a second layer scope over the SAME database name reads back what the first
// wrote — replay-on-mount survives a reload / workspace-switch.

const sid = (s: string) => SyncId.make(s)
const Webhook = ModelName.make("Webhook")
const insert = (syncId: string, scope: string, id: string): LoggedEvent => ({
  syncId: sid(syncId),
  modelName: Webhook,
  tag: "Insert",
  modelId: ModelId.make(id),
  data: Option.some({ id, scope }),
})
const del = (syncId: string, id: string): LoggedEvent => ({
  syncId: sid(syncId),
  modelName: Webhook,
  tag: "Delete",
  modelId: ModelId.make(id),
  data: Option.none(),
})

/** One "session" over a named IDB database: opens its own layer scope and closes it on the way out. */
const session = <A>(databaseName: string, use: Effect.Effect<A, never, EventLogStore>): Effect.Effect<A> =>
  use.pipe(Effect.provide(EventLogStore.layer({ databaseName })))

/** A fresh, unique database per test so reruns never see stale IDB state. */
const freshDb = () => `eventlog-${crypto.randomUUID()}`

describe("EventLogStore.layer (IndexedDB, browser)", () => {
  it.live("append dedupes by syncId — a re-delivered event upserts instead of duplicating", () =>
    session(
      freshDb(),
      Effect.gen(function* () {
        const log = yield* EventLogStore
        yield* log.append([insert("1", "org-1", "w1")])
        yield* log.append([insert("1", "org-1", "w1-v2")]) // same syncId, re-delivered on reconnect overlap
        const rows = yield* log.read({ modelName: Webhook, since: sid("0") })
        assert.strictEqual(rows.length, 1)
        assert.strictEqual(rows[0]!.modelId, ModelId.make("w1-v2")) // the later append wins
      }),
    ))

  it.live("read returns every event for the model, syncId-ordered, after `since`", () =>
    session(
      freshDb(),
      Effect.gen(function* () {
        const log = yield* EventLogStore
        yield* log.append([insert("2", "org-1", "a"), insert("3", "org-2", "b"), del("4", "a"), insert("1", "org-1", "z")])
        const rows = yield* log.read({ modelName: Webhook, since: sid("1") })
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
        const log = yield* EventLogStore
        yield* log.append([insert("2", "org-1", "a"), insert("10", "org-1", "b"), insert("1", "org-1", "c")])
        const rows = yield* log.read({ modelName: Webhook, since: sid("0") })
        assert.deepStrictEqual(
          rows.map((r) => r.syncId),
          [sid("1"), sid("2"), sid("10")], // string-sorted this would be ["1","10","2"]
        )
      }),
    ))

  it.live("the log survives a reload — a fresh layer scope reads back what the prior scope appended", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      yield* session(databaseName, EventLogStore.pipe(Effect.flatMap((log) => log.append([insert("5", "org-1", "x")]))))
      // A second, independent layer scope over the SAME database — the "reload".
      const rows = yield* session(
        databaseName,
        EventLogStore.pipe(Effect.flatMap((log) => log.read({ modelName: Webhook, since: sid("0") }))),
      )
      assert.deepStrictEqual(rows.map((r) => r.modelId), [ModelId.make("x")])
    }))

  it.live("prune keeps the newest `perModel` and records the deleted high-water as the floor (durable)", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      yield* session(
        databaseName,
        Effect.gen(function* () {
          const log = yield* EventLogStore
          yield* log.append(["1", "2", "3", "4", "5"].map((s, i) => insert(s, "org-1", `w${i}`)))
          yield* log.prune({ perModel: 3, total: 100 }) // keep 3,4,5 ; delete 1,2 ; floor ⇒ 2
          const kept = yield* log.read({ modelName: Webhook, since: sid("0") })
          assert.deepStrictEqual(kept.map((r) => r.syncId), [sid("3"), sid("4"), sid("5")])
        }),
      )
      // Floor persists across the reload (a remount must still see the prune boundary).
      const floor = yield* session(databaseName, EventLogStore.pipe(Effect.flatMap((log) => log.floor(Webhook))))
      assert.deepStrictEqual(floor, Option.some(sid("2")))
    }))

  it.live("watermark + lastResync advance monotonically and survive a reload", () =>
    Effect.gen(function* () {
      const databaseName = freshDb()
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      yield* session(
        databaseName,
        Effect.gen(function* () {
          const log = yield* EventLogStore
          yield* log.setBaseWatermark({ key, at: sid("5") })
          yield* log.setBaseWatermark({ key, at: sid("3") }) // older ⇒ ignored
          yield* log.setLastResync(sid("7"))
          yield* log.setLastResync(sid("2")) // older ⇒ ignored
        }),
      )
      const [wm, resync] = yield* session(
        databaseName,
        Effect.gen(function* () {
          const log = yield* EventLogStore
          return [yield* log.getBaseWatermark(key), yield* log.getLastResync] as const
        }),
      )
      assert.deepStrictEqual(wm, Option.some(sid("5")))
      assert.deepStrictEqual(resync, Option.some(sid("7")))
    }))
})
