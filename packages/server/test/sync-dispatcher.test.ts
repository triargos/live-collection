import { assert, describe, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Stream } from "effect"
import {
  deriveGroup,
  ModelId,
  ModelName,
  PendingSyncEvent,
  SyncId
} from "@triargos/live-collection-protocol"
import { SyncDispatcher } from "../src/sync-dispatcher.js"
import { SyncEventBus, type SyncEventBusShape } from "../src/sync-event-bus.js"
import { SyncEventStore } from "../src/sync-event-store.js"

const group = deriveGroup(["user", "alice"])
const pending = PendingSyncEvent.cases.Insert.make({
  modelName: ModelName.make("Note"),
  modelId: ModelId.make("note-1"),
  syncGroups: [group]
})

const TestLayer = SyncDispatcher.layer.pipe(
  Layer.provideMerge(Layer.merge(SyncEventStore.layerMemory, SyncEventBus.layerMemory))
)

describe("SyncDispatcher", () => {
  it.effect("persists before publishing the assigned event", () =>
    Effect.gen(function* () {
      const bus = yield* SyncEventBus
      const dispatcher = yield* SyncDispatcher
      const store = yield* SyncEventStore
      const subscription = yield* bus.subscribe
      const receive = yield* Stream.runHead(Stream.fromSubscription(subscription)).pipe(Effect.forkChild)

      const persisted = yield* dispatcher.dispatch(pending)
      const delivered = yield* Fiber.join(receive)

      assert.strictEqual(persisted.syncId, "1")
      assert.strictEqual(delivered._tag, "Some")
      if (delivered._tag === "Some") assert.deepStrictEqual(delivered.value, persisted)
      assert.strictEqual(yield* store.getLatestSyncId, SyncId.make("1"))
    }).pipe(Effect.scoped, Effect.provide(TestLayer)))

  it.effect("a publish failure never fails the write — catchup remains the source of truth", () =>
    Effect.gen(function* () {
      const store = yield* SyncEventStore
      const failingBus: SyncEventBusShape = {
        publish: () => Effect.die(new Error("bus down")),
        subscribe: Effect.die(new Error("bus down"))
      }
      const dispatcher = yield* Effect.provide(
        Effect.flatMap(SyncDispatcher, Effect.succeed),
        SyncDispatcher.layer.pipe(
          Layer.provide(Layer.succeed(SyncEventBus, failingBus)),
          Layer.provide(Layer.succeed(SyncEventStore, store))
        )
      )

      const persisted = yield* dispatcher.dispatch(pending)

      assert.strictEqual(persisted.syncId, "1")
      assert.deepStrictEqual(
        (yield* store.listEvents({ cursor: SyncId.make("0") })).map((e) => e.syncId),
        ["1"]
      )
    }).pipe(Effect.provide(SyncEventStore.layerMemory)))
})
