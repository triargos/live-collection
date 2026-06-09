import { Effect, Exit, Fiber, Layer, Queue } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { type HydratedSyncEventEnvelope, SyncId } from "@triargos/live-collection-protocol"
import { makeLiveRuntime } from "../src/runtime/live-runtime.js"
import { LastSyncIdStore } from "../src/client/last-sync-id-store.js"
import { CatchupClient } from "../src/client/catchup-client.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { EventLogStore } from "../src/client/event-log-store.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

describe("makeLiveRuntime", () => {
  it.live("a second forkLoop interrupts the previous loop — two loops would split the mounts queue", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const loop = Layer.mergeAll(
        LastSyncIdStore.layerMemory,
        CatchupClient.layerMemory({ events: [], lastSyncId: SyncId.make("0") }),
        SyncTransport.layerMemory(queue),
        EventLogStore.layerMemory,
      )
      const runtime = makeLiveRuntime({ persistence: makeNodeSqlitePersistence(), loop, onResync: Effect.void })

      const first = runtime.forkLoop({})
      const second = runtime.forkLoop({}) // the registry's mounts queue has ONE consumer — last call wins

      const exit = yield* Fiber.await(first) // resolves only if the second fork interrupted the first
      assert.isTrue(Exit.isInterrupted(exit))
      yield* Fiber.interrupt(second)
      runtime.dispose()
    }))
})
