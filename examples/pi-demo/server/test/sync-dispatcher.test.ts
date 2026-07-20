import { assert, describe, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Stream } from "effect"
import {
  projectKey,
  PROJECT_MODEL,
  ProjectId,
  SessionCode,
  sessionGroup,
} from "@pi-demo/shared"
import { PendingSyncEvent, SyncId } from "@triargos/live-collection-protocol"
import { SyncEventBus } from "../src/sync/sync-event-bus.js"
import { SyncDispatcher } from "../src/sync/sync-dispatcher.js"
import { SyncEventStore } from "../src/sync/sync-event-store.js"

const session = SessionCode.make("ABC234")

const TestLayer = SyncDispatcher.layer.pipe(
  Layer.provideMerge(Layer.merge(SyncEventStore.layerMemory, SyncEventBus.layer)),
)

describe("SyncDispatcher", () => {
  it.effect("persists before publishing the assigned event", () =>
    Effect.gen(function* () {
      const bus = yield* SyncEventBus
      const dispatcher = yield* SyncDispatcher
      const store = yield* SyncEventStore
      const queue = yield* bus.subscribe
      const receive = yield* Stream.runHead(Stream.fromSubscription(queue)).pipe(Effect.forkChild)

      const persisted = yield* dispatcher.dispatch(PendingSyncEvent.cases.Insert.make({
        modelName: PROJECT_MODEL,
        modelId: projectKey({
          id: ProjectId.make("project"),
          sessionId: session,
          name: "Project",
          color: "#000",
          createdAt: "2026-01-01T00:00:00.000Z",
        }),
        syncGroups: [sessionGroup(session)],
      }))
      const delivered = yield* Fiber.join(receive)

      assert.strictEqual(persisted.syncId, "1")
      assert.strictEqual(delivered._tag, "Some")
      if (delivered._tag === "Some") assert.deepStrictEqual(delivered.value, persisted)
      assert.strictEqual(yield* store.currentSyncId, SyncId.make("1"))
    }).pipe(Effect.scoped, Effect.provide(TestLayer)))
})
