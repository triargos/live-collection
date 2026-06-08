import { Chunk, Effect, Queue, Stream } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncGroup,
  SyncId,
} from "@triargos/live-collection-protocol"
import { SyncConnectionLost, SyncTransport } from "../src/client/sync-transport.js"

const env = (id: string): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId: SyncId.make("1"),
  modelName: ModelName.make("Webhook"),
  modelId: ModelId.make(id),
  syncGroups: [SyncGroup.make("organization:o1")],
  createdAt: new Date(0),
  data: { id, orgId: "o1" },
})

describe("SyncTransport", () => {
  it.effect("layerMemory surfaces enqueued events on connect, in order", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      yield* Queue.offerAll(queue, [env("a"), env("b")])
      const taken = yield* Effect.flatMap(SyncTransport, (transport) =>
        transport.connect.pipe(Stream.take(2), Stream.runCollect),
      ).pipe(Effect.provide(SyncTransport.layerMemory(queue)))
      assert.deepStrictEqual(
        Chunk.toReadonlyArray(taken).map((e) => ("modelId" in e ? e.modelId : undefined)),
        [ModelId.make("a"), ModelId.make("b")],
      )
    }))

  it.effect("a closed connection fails with SyncConnectionLost (the reconnect signal)", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      yield* Queue.shutdown(queue)
      const exit = yield* Effect.flatMap(SyncTransport, (transport) =>
        Stream.runDrain(transport.connect),
      ).pipe(Effect.provide(SyncTransport.layerMemory(queue)), Effect.exit)
      assert.isTrue(exit._tag === "Failure")
      if (exit._tag === "Failure" && exit.cause._tag === "Fail") {
        assert.instanceOf(exit.cause.error, SyncConnectionLost)
      } else {
        assert.fail("expected a SyncConnectionLost failure")
      }
    }))
})
