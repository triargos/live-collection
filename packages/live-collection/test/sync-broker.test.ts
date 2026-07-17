import { Context, Effect, Exit, Fiber, Layer, Option, Queue, Ref, Scope, Stream } from "effect"
import { TestClock } from "effect/testing"
import { assert, describe, it } from "@effect/vitest"
import {
  deriveGroup,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  ResyncAll,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CatchupClient, CatchupFailed } from "../src/client/catchup-client.js"
import { EventLogStore, type EventLogStoreShape, type LoggedEvent } from "../src/client/event-log-store.js"
import { LastSyncIdStore, type LastSyncIdStoreShape } from "../src/client/last-sync-id-store.js"
import {
  SyncBroker,
  type SyncBrokerOptions,
  type SyncBrokerShape,
  type SyncSignal,
} from "../src/client/sync-broker.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { scopedKey } from "../src/registry/collection-key.js"

const sid = (value: string) => SyncId.make(value)
const modelId = (value: string) => ModelId.make(value)
const Webhook = ModelName.make("Webhook")
const Setting = ModelName.make("Setting")
const group = deriveGroup(["organization", "org-1"])

const insert = (syncId: string, modelName = Webhook, id = `w-${syncId}`): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId: sid(syncId),
  modelName,
  modelId: modelId(id),
  syncGroups: [group],
  createdAt: new Date(0),
  data: { id, orgId: "org-1" },
})

const del = (syncId: string, id: string): HydratedSyncEventEnvelope => ({
  _tag: "Delete",
  syncId: sid(syncId),
  modelName: Webhook,
  modelId: modelId(id),
  syncGroups: [group],
  createdAt: new Date(0),
})

const resync = (syncId: string): HydratedSyncEventEnvelope => ({
  _tag: "Resync",
  syncId: sid(syncId),
  target: ResyncAll.make({}),
  syncGroups: [group],
  createdAt: new Date(0),
})

const logged = (syncId: string): LoggedEvent => ({
  syncId: sid(syncId),
  modelName: Webhook,
  tag: "Insert",
  modelId: modelId(`w-${syncId}`),
  data: Option.some({ id: `w-${syncId}`, orgId: "org-1" }),
})

const collect = (broker: SyncBrokerShape, count: number) =>
  broker.subscribe({ modelName: Webhook, scope: Option.some("org-1") }).pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.forkScoped,
  )

const run = <A, E>(options: {
  readonly catchup?: Layer.Layer<CatchupClient>
  readonly broker?: SyncBrokerOptions
  readonly body: (services: {
    readonly broker: SyncBrokerShape
    readonly log: EventLogStoreShape
    readonly cursor: LastSyncIdStoreShape
    readonly events: Queue.Queue<HydratedSyncEventEnvelope>
  }) => Effect.Effect<A, E, Scope.Scope>
}): Effect.Effect<A, E> =>
  Effect.scoped(
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const sync = Layer.mergeAll(
        SyncTransport.layerMemory(events),
        options.catchup ?? CatchupClient.layerMemory({ events: [], lastSyncId: sid("0") }),
        LastSyncIdStore.layerMemory,
        EventLogStore.layerMemory,
      )
      const brokerLayer = SyncBroker.layer(options.broker).pipe(Layer.provide(sync))
      return yield* Effect.gen(function* () {
        const broker = yield* SyncBroker
        const log = yield* EventLogStore
        const cursor = yield* LastSyncIdStore
        return yield* options.body({ broker, log, cursor, events })
      }).pipe(Effect.provide(Layer.merge(sync, brokerLayer)))
    }),
  )

const tags = (signals: ReadonlyArray<SyncSignal>) => signals.map((signal) => signal._tag)

const waitUntil = (effect: Effect.Effect<boolean>): Effect.Effect<void> =>
  effect.pipe(
    Effect.flatMap((done) => (done ? Effect.void : Effect.yieldNow.pipe(Effect.andThen(waitUntil(effect))))),
  )

describe("SyncBroker", () => {
  it.effect("cold subscribe emits Snapshot at the cursor; a pending mark is visible to the next subscribe", () =>
    run({
      body: ({ broker, events }) =>
        Effect.gen(function* () {
          const cold = yield* collect(broker, 1)
          const coldSignals = yield* Fiber.join(cold)
          assert.deepStrictEqual(tags(coldSignals), ["Snapshot"])
          assert.strictEqual(coldSignals[0]!._tag === "Snapshot" && coldSignals[0]!.at, sid("0"))

          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), through: sid("0") })
          const warm = yield* collect(broker, 1)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1"))
          const warmSignals = yield* Fiber.join(warm)
          assert.deepStrictEqual(tags(warmSignals), ["Upsert"])
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("live ingest fans matching Upserts and Deletes out, logs them, and advances the cursor", () =>
    run({
      body: ({ broker, events, log, cursor }) =>
        Effect.gen(function* () {
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), through: sid("0") })
          yield* broker.markApplied({ modelName: Setting, scope: Option.none(), through: sid("0") })
          const mine = yield* collect(broker, 2)
          const other = yield* broker.subscribe({ modelName: Setting, scope: Option.none() }).pipe(
            Stream.take(1),
            Stream.runCollect,
            Effect.forkScoped,
          )
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1"))
          yield* Queue.offer(events, del("2", "w-1"))
          yield* Queue.offer(events, insert("3", Setting, "s-1"))
          assert.deepStrictEqual(tags(yield* Fiber.join(mine)), ["Upsert", "Delete"])
          assert.deepStrictEqual(tags(yield* Fiber.join(other)), ["Upsert"])
          assert.deepStrictEqual((yield* log.read({ modelName: Webhook, since: sid("0") })).map((row) => row.syncId), [sid("1"), sid("2")])
          assert.deepStrictEqual((yield* log.read({ modelName: Setting, since: sid("0") })).map((row) => row.syncId), [sid("3")])
          yield* waitUntil(cursor.get.pipe(Effect.map(Option.contains(sid("3")))))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("late subscribe replays a covered gap, tails live, and drops older buffered duplicates", () =>
    run({
      body: ({ broker, events, log, cursor }) =>
        Effect.gen(function* () {
          yield* log.append([logged("2")])
          yield* cursor.set(sid("2"))
          yield* log.setBaseWatermark({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), at: sid("1") })
          const signals = yield* collect(broker, 2)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1"))
          yield* Queue.offer(events, insert("3"))
          const received = yield* Fiber.join(signals)
          assert.deepStrictEqual(
            received.map((signal) => (signal._tag === "Snapshot" ? signal.at : signal.syncId)),
            [sid("2"), sid("3")],
          )
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("a prune floor above the watermark forces Snapshot", () =>
    run({
      body: ({ broker, log, cursor }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          yield* log.append([logged("1"), logged("2")])
          yield* log.setBaseWatermark({ key, at: sid("0") })
          yield* log.prune({ perModel: 1, total: 10 })
          yield* cursor.set(sid("2"))
          const signals = yield* collect(broker, 1)
          assert.deepStrictEqual(tags(yield* Fiber.join(signals)), ["Snapshot"])
        }),
    }))

  it.effect("a resync newer than the watermark forces Snapshot on remount", () =>
    run({
      body: ({ broker, log, cursor }) =>
        Effect.gen(function* () {
          yield* log.setBaseWatermark({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), at: sid("2") })
          yield* log.setLastResync(sid("3"))
          yield* cursor.set(sid("3"))
          const signals = yield* collect(broker, 1)
          assert.deepStrictEqual(tags(yield* Fiber.join(signals)), ["Snapshot"])
        }),
    }))

  it.effect("live Resync snapshots every active subscriber and ingest continues", () =>
    run({
      body: ({ broker, events, log }) =>
        Effect.gen(function* () {
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), through: sid("0") })
          const signals = yield* collect(broker, 2)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, resync("1"))
          yield* Queue.offer(events, insert("2"))
          const received = yield* Fiber.join(signals)
          assert.deepStrictEqual(tags(received), ["Snapshot", "Upsert"])
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("1"))
          assert.deepStrictEqual(yield* log.getLastResync, Option.some(sid("1")))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("a catchup Resync snapshots active subscribers at response.lastSyncId", () =>
    run({
      catchup: CatchupClient.layerMemory({ events: [resync("3")], lastSyncId: sid("5") }),
      body: ({ broker }) =>
        Effect.gen(function* () {
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), through: sid("0") })
          const signals = yield* collect(broker, 1)
          const start = yield* Effect.forkScoped(broker.start)
          const received = yield* Fiber.join(signals)
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("5"))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("catchup failure degrades to live tail and reconnect retries catchup", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      const failed = Layer.succeed(CatchupClient, {
        fetch: ({ from }) => Ref.updateAndGet(calls, (count) => count + 1).pipe(
          Effect.flatMap(() => Effect.fail(new CatchupFailed({ from, reason: "offline" }))),
        ),
      })
      yield* run({
        catchup: failed,
        body: ({ broker, events }) =>
          Effect.gen(function* () {
            yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), through: sid("0") })
            const signals = yield* collect(broker, 1)
            yield* Effect.forkScoped(broker.start)
            yield* Queue.offer(events, insert("1"))
            assert.deepStrictEqual(tags(yield* Fiber.join(signals)), ["Upsert"])
            yield* Queue.shutdown(events)
            yield* waitUntil(Ref.get(calls).pipe(Effect.map((count) => count === 1)))
            yield* TestClock.adjust("3 seconds")
            yield* waitUntil(Ref.get(calls).pipe(Effect.map((count) => count >= 2)))
          }),
      })
    }))

  it.effect("broker scope close flushes pending watermarks", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const log = yield* EventLogStore.pipe(Effect.provide(EventLogStore.layerMemory))
      const sync = Layer.mergeAll(
        SyncTransport.layerMemory(events),
        CatchupClient.layerMemory({ events: [], lastSyncId: sid("0") }),
        LastSyncIdStore.layerMemory,
        Layer.succeed(EventLogStore, log),
      )
      const scope = yield* Scope.make()
      const context = yield* Layer.build(
        SyncBroker.layer({ watermarkFlushEvery: "1 hour" }).pipe(Layer.provide(sync)),
      ).pipe(Scope.provide(scope))
      const broker = Context.get(context, SyncBroker)
      const key = scopedKey({ entity: "Webhook", scope: "org-1" })
      yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), through: sid("7") })
      assert.deepStrictEqual(yield* log.getBaseWatermark(key), Option.none())
      yield* Scope.close(scope, Exit.void)
      assert.deepStrictEqual(yield* log.getBaseWatermark(key), Option.some(sid("7")))
    }))

  it.effect("markApplied batches durable writes while subscribe sees the pending watermark immediately", () =>
    run({
      broker: { watermarkFlushEvery: "100 millis" },
      body: ({ broker, log }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), through: sid("4") })
          assert.deepStrictEqual(yield* log.getBaseWatermark(key), Option.none())
          const signals = yield* collect(broker, 1)
          yield* TestClock.adjust("100 millis")
          assert.deepStrictEqual(yield* log.getBaseWatermark(key), Option.some(sid("4")))
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), through: sid("5") })
          assert.deepStrictEqual(yield* log.getBaseWatermark(key), Option.some(sid("4")))
          yield* Fiber.interrupt(signals)
        }),
    }))
})
