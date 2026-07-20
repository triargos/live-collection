import { assert, describe, it } from "@effect/vitest"
import { Effect, Option } from "effect"
import {
  deriveGroup,
  ModelId,
  ModelName,
  PendingSyncEvent,
  SyncId
} from "@triargos/live-collection-protocol"
import { SyncEventStore } from "../src/sync-event-store.js"

const group = deriveGroup(["user", "alice"])

const pending = (id: string) =>
  PendingSyncEvent.cases.Insert.make({
    modelName: ModelName.make("Note"),
    modelId: ModelId.make(id),
    syncGroups: [group]
  })

describe("SyncEventStore.layerMemory", () => {
  it.effect("assigns ordered cursors and lists strictly after a cursor", () =>
    Effect.gen(function* () {
      const store = yield* SyncEventStore
      const first = yield* store.appendEvent(pending("one"))
      const second = yield* store.appendEvent(pending("two"))
      const third = yield* store.appendEvent(pending("three"))

      assert.strictEqual(first.syncId, "1")
      assert.strictEqual(second.syncId, "2")
      assert.strictEqual(third.syncId, "3")
      assert(first.createdAt instanceof Date)
      assert.deepStrictEqual(
        (yield* store.listEvents({ cursor: SyncId.make("0") })).map((e) => e.syncId),
        ["1", "2", "3"]
      )
      assert.deepStrictEqual(
        (yield* store.listEvents({ cursor: second.syncId })).map((e) => e.syncId),
        ["3"]
      )
      assert.strictEqual(yield* store.getLatestSyncId, "3")
    }).pipe(Effect.provide(SyncEventStore.layerMemory)))

  it.effect("mints a stable epoch per construction — memory logs are a fresh timeline", () =>
    Effect.gen(function* () {
      const store = yield* SyncEventStore
      const first = yield* store.getCurrentEpoch
      const second = yield* store.getCurrentEpoch
      assert(Option.isSome(first))
      assert.deepStrictEqual(first, second)
    }).pipe(Effect.provide(SyncEventStore.layerMemory)))
})
