import { Duration, Effect, Fiber, Layer, Option, Queue, Ref, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  type CatchupResponse,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncGroup,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CollectionRegistry } from "../src/registry/collection-registry.js"
import { defineCollection } from "../src/registry/define-collection.js"
import { effectCollectionOptions } from "../src/persistence/effect-collection.js"
import { PersistenceBase } from "../src/persistence/persistence-base.js"
import { SyncDispatcher, dispatchEntry } from "../src/dispatch/sync-dispatcher.js"
import { LastSyncIdStore } from "../src/client/last-sync-id-store.js"
import { CatchupClient } from "../src/client/catchup-client.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { SyncClient, bootstrapSpec } from "../src/client/sync-client.js"
import { makeNodeSqliteDriver } from "./node-sqlite-driver.js"

// The whole read path end-to-end: a memory transport/catchup/cursor feeding the REAL dispatcher,
// registry, and persistence factory. Nothing below the transport is faked.
const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String })
type Webhook = typeof Webhook.Type
const k = (s: string) => ModelId.make(s)
const sid = (s: string) => SyncId.make(s)
const grp = SyncGroup.make("organization:org-1")

const webhookCollection = defineCollection({
  entity: "Webhook",
  scopeOf: (orgId: string) => orgId,
  make: ({ collectionId }) =>
    effectCollectionOptions({ collectionId, schema: Webhook, getKey: (w: Webhook) => k(w.id) }),
})

const DispatcherLive = SyncDispatcher.fromEntries({
  Webhook: dispatchEntry({ schema: Webhook, collection: webhookCollection, scopeOf: (w: Webhook) => w.orgId }),
})

const insertEnv = (id: string): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId: sid(id.replace(/\D/g, "") || "1"),
  modelName: ModelName.make("Webhook"),
  modelId: k(id),
  syncGroups: [grp],
  createdAt: new Date(0),
  data: { id, orgId: "org-1" },
})
const deleteEnv = (id: string, syncId: string): HydratedSyncEventEnvelope => ({
  _tag: "Delete",
  syncId: sid(syncId),
  modelName: ModelName.make("Webhook"),
  modelId: k(id),
  syncGroups: [grp],
  createdAt: new Date(0),
})
const resyncAllEnv = (syncId: string): HydratedSyncEventEnvelope => ({
  _tag: "Resync",
  syncId: sid(syncId),
  target: { _tag: "All" },
  syncGroups: [grp],
  createdAt: new Date(0),
})

const spec = (rows: ReadonlyArray<Webhook>) =>
  bootstrapSpec({
    mount: webhookCollection("org-1"),
    bootstrapFn: Effect.succeed(rows),
    getKey: (w: Webhook) => k(w.id),
  })

/** Poll an in-memory condition until it holds (the start fiber applies events on another fiber). */
const waitUntil = (cond: () => boolean): Effect.Effect<void> => {
  const attempt: Effect.Effect<void> = Effect.suspend(() =>
    cond() ? Effect.void : Effect.sleep(Duration.millis(5)).pipe(Effect.zipRight(attempt)),
  )
  return attempt.pipe(
    Effect.timeoutFail({ duration: Duration.seconds(2), onTimeout: () => new Error("condition not met") }),
    Effect.orDie,
  )
}

/** Build the full env: real registry/persistence/dispatcher + memory transport/catchup/cursor, with
 *  the memory layers shared between the SyncClient and the test (so cursor assertions see one store). */
const makeEnv = (args: {
  readonly catchup: CatchupResponse
  readonly queue: Queue.Dequeue<HydratedSyncEventEnvelope>
  readonly onResync: Effect.Effect<void>
}) => {
  const memory = Layer.mergeAll(
    LastSyncIdStore.layerMemory,
    CatchupClient.layerMemory(args.catchup),
    SyncTransport.layerMemory(args.queue),
  )
  return Layer.mergeAll(
    CollectionRegistry.layer,
    DispatcherLive,
    PersistenceBase.layerSqliteDriver(makeNodeSqliteDriver()),
    SyncClient.layer({ onResync: args.onResync }).pipe(Layer.provideMerge(memory)),
  )
}

describe("SyncClient — read path end-to-end", () => {
  it.live("cold start: a catchup Resync triggers a snapshot and seeds the cursor from lastSyncId", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const env = makeEnv({ catchup: { events: [resyncAllEnv("5")], lastSyncId: sid("5") }, queue, onResync: Effect.void })

      yield* Effect.gen(function* () {
        const client = yield* SyncClient
        const store = yield* LastSyncIdStore
        const coll = yield* webhookCollection("org-1")
        yield* Effect.promise(() => coll.preload())

        const fiber = yield* Effect.forkScoped(
          client.start([spec([{ id: "w1", orgId: "org-1" }, { id: "w2", orgId: "org-1" }])]),
        )
        yield* waitUntil(() => coll.has(k("w1")) && coll.has(k("w2")))

        assert.isTrue(coll.has(k("w1")))
        assert.deepStrictEqual(yield* store.get, Option.some(sid("5")))
        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.scoped, Effect.provide(env))
    }))

  it.live("warm start: catchup deltas are dispatched and the cursor advances", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const env = makeEnv({
        catchup: { events: [insertEnv("w1"), insertEnv("w2")], lastSyncId: sid("9") },
        queue,
        onResync: Effect.void,
      })

      yield* Effect.gen(function* () {
        const client = yield* SyncClient
        const store = yield* LastSyncIdStore
        const coll = yield* webhookCollection("org-1")
        yield* Effect.promise(() => coll.preload())

        const fiber = yield* Effect.forkScoped(client.start([spec([])]))
        yield* waitUntil(() => coll.has(k("w1")) && coll.has(k("w2")))

        assert.deepStrictEqual(yield* store.get, Option.some(sid("9")))
        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.scoped, Effect.provide(env))
    }))

  it.live("live: an Insert then a Delete pushed on the stream land in the collection", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const env = makeEnv({ catchup: { events: [], lastSyncId: sid("1") }, queue, onResync: Effect.void })

      yield* Effect.gen(function* () {
        const client = yield* SyncClient
        const coll = yield* webhookCollection("org-1")
        yield* Effect.promise(() => coll.preload())

        const fiber = yield* Effect.forkScoped(client.start([spec([])]))
        yield* Queue.offer(queue, insertEnv("w1"))
        yield* waitUntil(() => coll.has(k("w1")))
        assert.isTrue(coll.has(k("w1")))

        yield* Queue.offer(queue, deleteEnv("w1", "7"))
        yield* waitUntil(() => !coll.has(k("w1")))
        assert.isFalse(coll.has(k("w1")))
        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.scoped, Effect.provide(env))
    }))

  it.live("live: a Resync clears the cursor and fires onResync (no further dispatch)", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const fired = yield* Ref.make(false)
      const env = makeEnv({
        catchup: { events: [], lastSyncId: sid("3") },
        queue,
        onResync: Ref.set(fired, true),
      })

      yield* Effect.gen(function* () {
        const client = yield* SyncClient
        const store = yield* LastSyncIdStore

        const fiber = yield* Effect.forkScoped(client.start([spec([])]))
        yield* waitUntil(() => true) // let the cycle reach the tail
        yield* Queue.offer(queue, resyncAllEnv("8"))
        yield* Fiber.join(fiber) // start completes once the live resync stops the tail

        assert.isTrue(yield* Ref.get(fired))
        assert.isTrue(Option.isNone(yield* store.get))
      }).pipe(Effect.scoped, Effect.provide(env))
    }))

  it.live("snapshot reconcile: a row absent from the snapshot is removed (delete-absent)", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const env = makeEnv({ catchup: { events: [resyncAllEnv("5")], lastSyncId: sid("5") }, queue, onResync: Effect.void })

      yield* Effect.gen(function* () {
        const client = yield* SyncClient
        const coll = yield* webhookCollection("org-1")
        yield* Effect.promise(() => coll.preload())
        yield* coll.utils.writeSynced({ id: "stale", orgId: "org-1" }) // pre-existing row the snapshot omits

        const fiber = yield* Effect.forkScoped(client.start([spec([{ id: "w1", orgId: "org-1" }])]))
        yield* waitUntil(() => coll.has(k("w1")) && !coll.has(k("stale")))

        assert.isTrue(coll.has(k("w1")))
        assert.isFalse(coll.has(k("stale")))
        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.scoped, Effect.provide(env))
    }))
})
