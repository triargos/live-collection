import { Effect, Option } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import { EventLogStore, type LoggedEvent } from "../src/client/event-log-store.js"

const sid = (s: string) => SyncId.make(s)
const insert = (syncId: string, scope: string, id: string): LoggedEvent => ({
  syncId: sid(syncId),
  modelName: ModelName.make("Webhook"),
  scope: Option.some(scope),
  tag: "Insert",
  modelId: ModelId.make(id),
  data: Option.some({ id, scope }),
})
const del = (syncId: string, id: string): LoggedEvent => ({
  syncId: sid(syncId),
  modelName: ModelName.make("Webhook"),
  scope: Option.none(), // a Delete carries no scope
  tag: "Delete",
  modelId: ModelId.make(id),
  data: Option.none(),
})

describe("EventLogStore (memory)", () => {
  it.effect("append dedupes by syncId — a re-delivered event upserts instead of duplicating", () =>
    Effect.gen(function* () {
      const log = yield* EventLogStore
      yield* log.append([insert("1", "org-1", "w1")])
      yield* log.append([insert("1", "org-1", "w1-v2")]) // same syncId, re-delivered on reconnect overlap
      const rows = yield* log.read({ modelName: ModelName.make("Webhook"), scope: Option.none(), since: sid("0") })
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0]!.modelId, ModelId.make("w1-v2")) // the later append wins
    }).pipe(Effect.provide(EventLogStore.layerMemory)))

  it.effect("read returns the scope's events plus scope-less Deletes, syncId-ordered, after `since`", () =>
    Effect.gen(function* () {
      const log = yield* EventLogStore
      yield* log.append([insert("2", "org-1", "a"), insert("3", "org-2", "b"), del("4", "a"), insert("1", "org-1", "z")])
      const rows = yield* log.read({ modelName: ModelName.make("Webhook"), scope: Option.some("org-1"), since: sid("1") })
      // "1" excluded (since is exclusive); "3" excluded (other scope); "2" (org-1) + "4" (Delete) included, in order.
      assert.deepStrictEqual(
        rows.map((r) => r.syncId),
        [sid("2"), sid("4")],
      )
    }).pipe(Effect.provide(EventLogStore.layerMemory)))
})
