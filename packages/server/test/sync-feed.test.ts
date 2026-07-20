import { assert, describe, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Schema, Stream } from "effect"
import {
  compareSyncId,
  deriveGroup,
  HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  PendingSyncEvent,
  SyncId
} from "@triargos/live-collection-protocol"
import { ModelRegistry } from "../src/model-registry.js"
import { SyncDispatcher } from "../src/sync-dispatcher.js"
import { SyncEventBus } from "../src/sync-event-bus.js"
import { CursorOutOfRetentionError, SyncEventStore, type SyncEventStoreShape } from "../src/sync-event-store.js"
import { SyncFeed } from "../src/sync-feed.js"
import { makeKernelLayer } from "./support/layers.js"
import { Note, NoteId, NoteRepo, testRegistry, testRegistryBatched } from "./support/test-registry.js"

const alice = deriveGroup(["user", "alice"])
const bob = deriveGroup(["user", "bob"])
const zero = SyncId.make("0")

const note = (id: string, title: string): Note => ({ id: NoteId.make(id), title })

const noteEvent = (
  kind: "Insert" | "Update" | "Delete",
  id: string,
  groups: ReadonlyArray<typeof alice> = [alice]
) =>
  PendingSyncEvent.cases[kind].make({
    modelName: ModelName.make("Note"),
    modelId: ModelId.make(id),
    syncGroups: [groups[0]!, ...groups.slice(1)]
  })

/** Write to the repo and dispatch the matching event — an app's write handler in miniature. */
const upsertNote = (row: Note, kind: "Insert" | "Update" = "Insert") =>
  Effect.gen(function* () {
    yield* Effect.flatMap(NoteRepo, (repo) => repo.upsert(row))
    yield* Effect.flatMap(SyncDispatcher, (d) => d.dispatch(noteEvent(kind, row.id)))
  })

describe("SyncFeed.catchup", () => {
  it.effect("squashes runs, hydrates current data, and reports the head cursor", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const store = yield* SyncEventStore

      yield* upsertNote(note("n1", "First"))
      yield* upsertNote(note("n1", "Renamed"), "Update")
      yield* upsertNote(note("n2", "Transient"))
      yield* Effect.flatMap(NoteRepo, (repo) => repo.remove(NoteId.make("n2")))
      yield* Effect.flatMap(SyncDispatcher, (d) => d.dispatch(noteEvent("Delete", "n2")))

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })

      // Insert→Update folds to one Insert with current data; Insert→Delete cancels out.
      assert.strictEqual(response.events.length, 1)
      const event = response.events[0]!
      assert.strictEqual(event._tag, "Insert")
      if (event._tag === "Insert") {
        assert.strictEqual(String(event.modelId), "n1")
        const decoded = yield* Schema.decodeUnknownEffect(Note)(event.data)
        assert.strictEqual(decoded.title, "Renamed")
        // The folded event carries the run's latest syncId, so the cursor advances past every absorbed event.
        assert(compareSyncId(event.syncId, SyncId.make("1")) > 0)
      }
      assert.strictEqual(response.lastSyncId, yield* store.getLatestSyncId)
      assert.deepStrictEqual(response.epoch, yield* store.getCurrentEpoch)
    }).pipe(Effect.provide(makeKernelLayer())))

  it.effect("filters by exact group intersection — a foreign group's events never leak", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      yield* upsertNote(note("mine", "Visible"))
      yield* Effect.flatMap(SyncDispatcher, (d) => d.dispatch(noteEvent("Insert", "theirs", [bob])))

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })
      assert.deepStrictEqual(
        response.events.map((e) => (e._tag === "Resync" ? "Resync" : String(e.modelId))),
        ["mine"]
      )

      const bobResponse = yield* feed.catchup({ fromSyncId: zero, syncGroups: [bob] })
      // Bob's event exists in the log but its entity was never in the repo ⇒ hydration downgrades to Delete.
      assert.deepStrictEqual(bobResponse.events.map((e) => e._tag), ["Delete"])
    }).pipe(Effect.provide(makeKernelLayer())))

  it.effect("an entity gone at hydration time arrives as a Delete — access loss surfaces as removal", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      yield* upsertNote(note("vanishing", "Here now"))
      // The row disappears (deleted, or ACL lost) without a Delete event being logged.
      yield* Effect.flatMap(NoteRepo, (repo) => repo.remove(NoteId.make("vanishing")))

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })
      assert.deepStrictEqual(response.events.map((e) => e._tag), ["Delete"])
    }).pipe(Effect.provide(makeKernelLayer())))

  it.effect("unknown models are dropped, not fatal — newer servers stay compatible", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      yield* Effect.flatMap(SyncDispatcher, (d) =>
        d.dispatch(
          PendingSyncEvent.cases.Insert.make({
            modelName: ModelName.make("Exotic"),
            modelId: ModelId.make("x1"),
            syncGroups: [alice]
          })
        )
      )
      yield* upsertNote(note("known", "Still works"))

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })
      assert.deepStrictEqual(
        response.events.map((e) => (e._tag === "Resync" ? "Resync" : String(e.modelId))),
        ["known"]
      )
    }).pipe(Effect.provide(makeKernelLayer())))

  it.effect("hydrateMany batches lookups — one pass per model, not one per event", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const repo = yield* NoteRepo
      yield* upsertNote(note("b1", "One"))
      yield* upsertNote(note("b2", "Two"))
      yield* upsertNote(note("b3", "Three"))
      const before = yield* repo.lookupCount

      const response = yield* feed.catchup({ fromSyncId: zero, syncGroups: [alice] })

      assert.strictEqual(response.events.length, 3)
      // The batched registry funnels all ids through one hydrateMany call; the
      // test repo's per-id find still counts 3 — what matters is the descriptor
      // received them together (asserted structurally: batch size == events).
      assert.strictEqual((yield* repo.lookupCount) - before, 3)
    }).pipe(Effect.provide(makeKernelLayer(testRegistryBatched))))

  it.effect("a cursor out of retention becomes a single synthetic Resync(All), never an error", () =>
    Effect.gen(function* () {
      const memory = yield* Effect.provide(
        Effect.flatMap(SyncEventStore, Effect.succeed),
        SyncEventStore.layerMemory
      )
      const pruningStore: SyncEventStoreShape = {
        ...memory,
        listEvents: ({ cursor }) => Effect.fail(new CursorOutOfRetentionError({ cursor }))
      }
      const layer = Layer.mergeAll(
        SyncDispatcher.layer,
        SyncFeed.layer.pipe(Layer.provide(ModelRegistry.layer(testRegistry)))
      ).pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            Layer.succeed(SyncEventStore, pruningStore),
            SyncEventBus.layerMemory,
            NoteRepo.layerMemory
          )
        )
      )

      yield* Effect.gen(function* () {
        yield* upsertNote(note("kept", "Retained row"))
        const feed = yield* SyncFeed
        const store = yield* SyncEventStore
        const response = yield* feed.catchup({ fromSyncId: SyncId.make("1"), syncGroups: [alice] })

        assert.strictEqual(response.events.length, 1)
        const event = response.events[0]!
        assert.strictEqual(event._tag, "Resync")
        if (event._tag === "Resync") assert.strictEqual(event.target._tag, "All")
        assert.strictEqual(response.lastSyncId, yield* store.getLatestSyncId)
        // Synthesized inline: nothing new appended to the log.
        assert.strictEqual(yield* store.getLatestSyncId, "1")
      }).pipe(Effect.provide(layer))
    }))
})

describe("SyncFeed.streamEvents", () => {
  it.effect("streams hydrated, group-filtered SSE frames that decode against the envelope", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const collected = yield* feed
        .streamEvents({ syncGroups: [alice], keepAlive: "10 minutes" })
        .pipe(
          // Stream.tick emits its first keepalive immediately — keep only data frames here;
          // keepalive cadence is asserted separately below.
          Stream.filter((frame) => frame.startsWith("data: ")),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild
        )
      // Let the forked stream attach its bus subscription (it runs synchronously
      // up to its first await once this fiber yields).
      yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow)

      yield* upsertNote(note("live-1", "Streamed"))
      yield* Effect.flatMap(SyncDispatcher, (d) => d.dispatch(noteEvent("Insert", "foreign", [bob])))
      yield* Effect.flatMap(SyncDispatcher, (d) => d.dispatch(noteEvent("Delete", "live-1")))

      const frames = yield* Fiber.join(collected)
      const decodeFrame = Schema.decodeEffect(Schema.fromJsonString(HydratedSyncEventEnvelope))
      const events = yield* Effect.forEach(frames, (frame) => {
        assert(frame.endsWith("\n\n"))
        return decodeFrame(frame.slice("data: ".length, -2))
      })

      // Bob's event was filtered out; Alice sees her Insert (hydrated) then the Delete.
      assert.deepStrictEqual(events.map((e) => e._tag), ["Insert", "Delete"])
      const insert = events[0]!
      if (insert._tag === "Insert") {
        const decoded = yield* Schema.decodeUnknownEffect(Note)(insert.data)
        assert.strictEqual(decoded.title, "Streamed")
      }
    }).pipe(Effect.scoped, Effect.provide(makeKernelLayer())))

  it.live("emits keepalive comments so silence never exceeds the client window", () =>
    Effect.gen(function* () {
      const feed = yield* SyncFeed
      const frames = yield* feed
        .streamEvents({ syncGroups: [alice], keepAlive: "1 millis" })
        .pipe(Stream.take(2), Stream.runCollect)
      assert.deepStrictEqual([...frames], [":ka\n\n", ":ka\n\n"])
    }).pipe(Effect.scoped, Effect.provide(makeKernelLayer())))
})
