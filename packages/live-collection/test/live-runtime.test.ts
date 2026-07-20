import { Effect, Exit, Fiber, Layer, Option, Queue } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { type HydratedSyncEventEnvelope, SyncId } from "@triargos/live-collection-protocol"
import { CatchupClient } from "../src/client/catchup-client.js"
import { SyncJournal } from "../src/client/sync-journal.js"
import { LastSyncIdStore } from "../src/client/last-sync-id-store.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { makeLiveRuntime } from "../src/runtime/live-runtime.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

describe("makeLiveRuntime", () => {
  it.live("a second forkSync interrupts the previous broker ingest fiber", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const sync = Layer.mergeAll(
        LastSyncIdStore.layerMemory,
        CatchupClient.layerMemory({ events: [], lastSyncId: SyncId.make("0"), epoch: Option.none() }),
        SyncTransport.layerMemory(queue),
        SyncJournal.layerMemory,
      )
      const runtime = makeLiveRuntime({ persistence: makeNodeSqlitePersistence(), sync })
      const first = runtime.forkSync()
      const second = runtime.forkSync()
      const exit = yield* Fiber.await(first)
      assert.isTrue(Exit.hasInterrupts(exit))
      yield* Fiber.interrupt(second)
      runtime.dispose()
    }))
})
