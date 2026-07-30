import { assert, describe, it } from "@effect/vitest"
import { Chunk, Effect, Fiber, Stream } from "effect"
import { deriveGroup, ModelId, ModelName, SyncEvent, SyncId } from "@triargos/live-collection-protocol"
import { SyncEventBus } from "../src/sync-event-bus.js"

const group = deriveGroup(["user", "alice"])

const event = (id: string): SyncEvent =>
  SyncEvent.cases.Insert.make({
    syncId: SyncId.make(id),
    modelName: ModelName.make("Note"),
    modelId: ModelId.make(`note-${id}`),
    syncGroups: [group],
    createdAt: new Date(0)
  })

// The forked runs attach their subscriptions on first pull, so this fiber has to
// yield before publishing or the events race ahead of the subscribers.
const letSubscribersAttach = Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow())

describe("SyncEventBus.layerMemory", () => {
  it.effect("each run of `events` is an independent subscriber — both see every publish", () =>
    Effect.gen(function* () {
      const bus = yield* SyncEventBus
      const first = yield* Stream.runCollect(Stream.take(bus.events, 2)).pipe(Effect.fork)
      const second = yield* Stream.runCollect(Stream.take(bus.events, 2)).pipe(Effect.fork)
      yield* letSubscribersAttach

      yield* bus.publish(event("1"))
      yield* bus.publish(event("2"))

      const seen = yield* Effect.all([Fiber.join(first), Fiber.join(second)])
      assert.deepStrictEqual(
        seen.map((run) => Chunk.toReadonlyArray(run).map((e) => e.syncId)),
        [["1", "2"], ["1", "2"]]
      )
    }).pipe(Effect.provide(SyncEventBus.layerMemory)))

  it.effect("a finished run stops consuming — a later run sees only what follows it", () =>
    Effect.gen(function* () {
      const bus = yield* SyncEventBus
      const early = yield* Stream.runCollect(Stream.take(bus.events, 1)).pipe(Effect.fork)
      yield* letSubscribersAttach
      yield* bus.publish(event("1"))
      assert.deepStrictEqual(
        Chunk.toReadonlyArray(yield* Fiber.join(early)).map((e) => e.syncId),
        ["1"]
      )

      // Published while nobody is subscribed: with a leaked subscriber these would
      // pile up in its buffer and the next run would replay them.
      yield* bus.publish(event("2"))
      yield* bus.publish(event("3"))

      const late = yield* Stream.runCollect(Stream.take(bus.events, 1)).pipe(Effect.fork)
      yield* letSubscribersAttach
      yield* bus.publish(event("4"))

      assert.deepStrictEqual(
        Chunk.toReadonlyArray(yield* Fiber.join(late)).map((e) => e.syncId),
        ["4"]
      )
    }).pipe(Effect.provide(SyncEventBus.layerMemory)))
})
