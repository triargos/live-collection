import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import {
  projectKey,
  PROJECT_MODEL,
  ProjectId,
  SessionCode,
  sessionGroup,
} from "@pi-demo/shared"
import { PendingSyncEvent, SyncId } from "@triargos/live-collection-protocol"
import { SyncEventStore } from "../src/sync/sync-event-store.js"

const session = SessionCode.make("ABC234")

const pending = (id: string) => PendingSyncEvent.cases.Insert.make({
  modelName: PROJECT_MODEL,
  modelId: projectKey({
    id: ProjectId.make(id),
    sessionId: session,
    name: id,
    color: "#000",
    createdAt: "2026-01-01T00:00:00.000Z",
  }),
  syncGroups: [sessionGroup(session)],
})

describe("SyncEventStore", () => {
  it.effect("assigns ordered cursors and queries strictly after a cursor", () =>
    Effect.gen(function* () {
      const store = yield* SyncEventStore
      const first = yield* store.append(pending("one"))
      const second = yield* store.append(pending("two"))
      const third = yield* store.append(pending("three"))

      assert.strictEqual(first.syncId, "1")
      assert.strictEqual(second.syncId, "2")
      assert.strictEqual(third.syncId, "3")
      assert(first.createdAt instanceof Date)
      assert.deepStrictEqual((yield* store.since(SyncId.make("0"))).map((e) => e.syncId), ["1", "2", "3"])
      assert.deepStrictEqual((yield* store.since(second.syncId)).map((e) => e.syncId), ["3"])
      assert.strictEqual(yield* store.currentSyncId, "3")
    }).pipe(Effect.provide(SyncEventStore.layerMemory)))
})
