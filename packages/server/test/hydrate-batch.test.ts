import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Option, Schema } from "effect"
import {
  defineModelRegistry,
  deriveGroup,
  type HydrateBatchRequest,
  ModelId,
  ModelName,
  PendingSyncEvent,
  type SyncGroup
} from "@triargos/live-collection-protocol"
import { ModelRegistry } from "../src/model-registry.js"
import { SyncEventBus } from "../src/sync-event-bus.js"
import { SyncEventStore } from "../src/sync-event-store.js"
import { SyncFeed } from "../src/sync-feed.js"
import { makeKernelLayer } from "./support/layers.js"
import { Note, NoteId, NoteRepo } from "./support/test-registry.js"

const alice = deriveGroup(["user", "alice"])

const batch = (indexKey: string, keyValue = "t1", modelName = "Note"): HydrateBatchRequest => ({
  requests: [{ modelName: ModelName.make(modelName), indexKey, keyValue }]
})

const note = (id: string, title: string): Note => ({ id: NoteId.make(id), title })

/** Notes indexed by title; visibility refused for the "secret" key value. */
const indexedRegistry = Effect.gen(function* () {
  const repo = yield* NoteRepo
  return defineModelRegistry({
    Note: {
      modelName: "Note",
      schema: Note,
      hydrate: (id: ModelId) => repo.find(NoteId.make(id)),
      indexes: {
        title: (keyValue: string, _groups: ReadonlyArray<SyncGroup>) =>
          keyValue === "secret"
            ? Effect.succeedNone
            : keyValue === "empty"
              ? Effect.succeed(Option.some<ReadonlyArray<Note>>([]))
              : Effect.succeed(Option.some<ReadonlyArray<Note>>([note("n1", keyValue), note("n2", keyValue)]))
      }
    }
  })
})

describe("SyncFeed.hydrateBatch", () => {
  it.effect("answers a declared index with the subset's membership", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const response = yield* feed.hydrateBatch({ request: batch("title", "roadmap"), syncGroups: [alice] })
      assert.strictEqual(response.results.length, 1)
      const result = response.results[0]!
      assert.strictEqual(result._tag, "Members")
      if (result._tag === "Members") {
        const rows = yield* Effect.forEach(result.rows, (row) => Schema.decodeUnknownEffect(Note)(row))
        assert.deepStrictEqual(rows.map((r) => String(r.id)), ["n1", "n2"])
      }
    }).pipe(Effect.provide(makeKernelLayer(indexedRegistry))))

  it.effect("visibility refusal is Forbidden — distinct from valid empty membership", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const response = yield* feed.hydrateBatch(
        {
          request: {
            requests: [
              { modelName: ModelName.make("Note"), indexKey: "title", keyValue: "secret" },
              { modelName: ModelName.make("Note"), indexKey: "title", keyValue: "empty" }
            ]
          },
          syncGroups: [alice]
        }
      )
      assert.deepStrictEqual(response.results.map((r) => r._tag), ["Forbidden", "Members"])
      const empty = response.results[1]!
      if (empty._tag === "Members") assert.deepStrictEqual([...empty.rows], [])
    }).pipe(Effect.provide(makeKernelLayer(indexedRegistry))))

  it.effect("an undeclared index fails the whole batch — no partial results", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const error = yield* Effect.flip(
        feed.hydrateBatch(
          {
            request: {
              requests: [
                { modelName: ModelName.make("Note"), indexKey: "title", keyValue: "fine" },
                { modelName: ModelName.make("Note"), indexKey: "authorId", keyValue: "a1" }
              ]
            },
            syncGroups: [alice]
          }
        )
      )
      assert.strictEqual(error._tag, "UnknownIndexError")
      assert.strictEqual(error.indexKey, "authorId")
    }).pipe(Effect.provide(makeKernelLayer(indexedRegistry))))

  it.effect("an unknown model fails the batch the same way", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const error = yield* Effect.flip(
        feed.hydrateBatch({ request: batch("title", "t1", "Exotic"), syncGroups: [alice] })
      )
      assert.strictEqual(error._tag, "UnknownIndexError")
      assert.strictEqual(error.modelName, "Exotic")
    }).pipe(Effect.provide(makeKernelLayer(indexedRegistry))))

  it.effect("rows encode through the descriptor schema — a Date lands on the wire as an ISO string", () =>
    Effect.gen(function* () {
      const Stamped = Schema.Struct({ id: Schema.String, createdAt: Schema.Date })
      const stampedRow = { id: "s1", createdAt: new Date("2026-07-23T12:12:08.434Z") }
      const registry = Effect.succeed(
        defineModelRegistry({
          Stamped: {
            modelName: "Stamped",
            schema: Stamped,
            hydrate: () => Effect.succeedNone,
            indexes: {
              bucket: () => Effect.succeed(Option.some([stampedRow]))
            }
          }
        })
      )
      yield* Effect.gen(function* () {
        const feed = yield* SyncFeed
        const response = yield* feed.hydrateBatch({ request: batch("bucket", "b1", "Stamped"), syncGroups: [alice] })
        const result = response.results[0]!
        assert.strictEqual(result._tag, "Members")
        if (result._tag === "Members") {
          const wire = result.rows[0] as { readonly createdAt: unknown }
          assert.strictEqual(wire.createdAt, "2026-07-23T12:12:08.434Z")
        }
      }).pipe(Effect.provide(makeKernelLayer(registry)))
    }))

  it.effect("stamps the head read BEFORE the index queries — mid-batch appends stay above the stamp", () =>
    Effect.gen(function* () {
      // The index fetch itself appends an event: the response stamp must be the head
      // from before that append, so a client replaying from the stamp still sees it.
      const registry = Effect.gen(function* () {
        const store = yield* SyncEventStore
        return defineModelRegistry({
          Note: {
            modelName: "Note",
            schema: Note,
            hydrate: () => Effect.succeedNone,
            indexes: {
              title: () =>
                store
                  .appendEvent(
                    PendingSyncEvent.cases.Insert.make({
                      modelName: ModelName.make("Note"),
                      modelId: ModelId.make("mid-batch"),
                      syncGroups: [alice]
                    })
                  )
                  .pipe(Effect.as(Option.some<ReadonlyArray<Note>>([note("n1", "x")])))
            }
          }
        })
      })
      const infrastructure = Layer.mergeAll(SyncEventStore.layerMemory, SyncEventBus.layerMemory)
      const layer = SyncFeed.layer
        .pipe(Layer.provide(ModelRegistry.layer(registry)))
        .pipe(Layer.provideMerge(infrastructure))

      yield* Effect.gen(function* () {
        const feed = yield* SyncFeed
        const store = yield* SyncEventStore
        const headBefore = yield* store.getLatestSyncId
        const response = yield* feed.hydrateBatch({ request: batch("title"), syncGroups: [alice] })
        assert.strictEqual(response.lastSyncId, headBefore)
        // The append really happened — the head has since moved past the stamp.
        assert.notStrictEqual(yield* store.getLatestSyncId, response.lastSyncId)
      }).pipe(Effect.provide(layer))
    }))
})
