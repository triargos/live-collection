import { Context, Effect, Exit, Fiber, Layer, Option, Queue, Ref, Scope, Stream } from "effect"
import { TestClock } from "effect/testing"
import { assert, describe, it } from "@effect/vitest"
import {
  deriveGroup,
  Epoch,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  ResyncTarget,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CatchupClient, CatchupFailed } from "../src/client/catchup-client.js"
import { SyncJournal, type SyncJournalShape, type JournalEvent } from "../src/client/sync-journal.js"
import {
  SyncBroker,
  type SyncBrokerOptions,
  type SyncBrokerShape,
  type SyncSignal,
} from "../src/client/sync-broker.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { SchemaVersion } from "../src/core/schema-version.js"
import { scopedKey } from "../src/core/collection-key.js"

const sid = (value: string) => SyncId.make(value)
const version = SchemaVersion.make(1)
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
  target: ResyncTarget.cases.All.make({}),
  syncGroups: [group],
  createdAt: new Date(0),
})

const logged = (syncId: string): JournalEvent => ({
  syncId: sid(syncId),
  modelName: Webhook,
  tag: "Insert",
  modelId: modelId(`w-${syncId}`),
  data: Option.some({ id: `w-${syncId}`, orgId: "org-1" }),
})

const collect = (broker: SyncBrokerShape, count: number, schemaVersion: SchemaVersion = version) =>
  broker.subscribe({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion }).pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.forkScoped,
  )

const run = <A, E>(options: {
  readonly catchup?: Layer.Layer<CatchupClient>
  readonly broker?: SyncBrokerOptions
  readonly body: (services: {
    readonly broker: SyncBrokerShape
    readonly journal: SyncJournalShape
    readonly events: Queue.Queue<HydratedSyncEventEnvelope>
  }) => Effect.Effect<A, E, Scope.Scope>
}): Effect.Effect<A, E> =>
  Effect.scoped(
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const sync = Layer.mergeAll(
        SyncTransport.layerMemory(events),
        options.catchup ?? CatchupClient.layerMemory({ events: [], lastSyncId: sid("0"), epoch: Option.none() }),
        SyncJournal.layerMemory,
      )
      const brokerLayer = SyncBroker.layer(options.broker).pipe(Layer.provide(sync))
      return yield* Effect.gen(function* () {
        const broker = yield* SyncBroker
        const journal = yield* SyncJournal
        return yield* options.body({ broker, journal, events })
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

          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("0") })
          const warm = yield* collect(broker, 1)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1"))
          const warmSignals = yield* Fiber.join(warm)
          assert.deepStrictEqual(tags(warmSignals), ["Upsert"])
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("live ingest fans matching Upserts and Deletes out, journals them, and advances the cursor", () =>
    run({
      body: ({ broker, events, journal }) =>
        Effect.gen(function* () {
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("0") })
          yield* broker.markApplied({ modelName: Setting, scope: Option.none(), schemaVersion: version, through: sid("0") })
          const mine = yield* collect(broker, 2)
          const other = yield* broker.subscribe({ modelName: Setting, scope: Option.none(), schemaVersion: version }).pipe(
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
          assert.deepStrictEqual((yield* journal.read({ modelName: Webhook, since: sid("0") })).map((row) => row.syncId), [sid("1"), sid("2")])
          assert.deepStrictEqual((yield* journal.read({ modelName: Setting, since: sid("0") })).map((row) => row.syncId), [sid("3")])
          yield* waitUntil(journal.getCursor.pipe(Effect.map(Option.contains(sid("3")))))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("late subscribe replays a covered gap, tails live, and drops older buffered duplicates", () =>
    run({
      body: ({ broker, events, journal }) =>
        Effect.gen(function* () {
          yield* journal.append([logged("2")])
          yield* journal.setCursor(sid("2"))
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("1") })
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

  it.effect("the trim tick flushes pending last-applied marks first, so applied rows are pruned as dead weight", () =>
    run({
      broker: { retention: { maxEventsPerModel: 100, maxEventsTotal: 100, trimEveryEvents: 3 } },
      body: ({ broker, events, journal }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          // Durable mark lags at 1; the fresher "applied through 2" exists only as a pending mark.
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("1") })
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("2") })
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1"))
          yield* Queue.offer(events, insert("2"))
          yield* Queue.offer(events, insert("3")) // 3rd ingest ⇒ trim tick: flush marks, then prune
          // Ingest is done once the cursor reaches 3; the trim tick runs within that same chain,
          // so the journal now holds the pruned state. With the flush, min last-applied is 2 ⇒
          // rows 1 AND 2 are dead weight; a stale (durable-only) prune would keep row 2.
          yield* waitUntil(journal.getCursor.pipe(Effect.map(Option.contains(sid("3")))))
          const rows = yield* journal.read({ modelName: Webhook, since: sid("0") })
          assert.deepStrictEqual(rows.map((row) => row.syncId), [sid("3")])
          assert.deepStrictEqual(yield* journal.floor(Webhook), Option.none()) // dead weight is floor-neutral
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("a prune floor above the collection's last-applied syncId forces Snapshot", () =>
    run({
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          yield* journal.append([logged("1"), logged("2")])
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("0") })
          yield* journal.prune({ maxEventsPerModel: 1, maxEventsTotal: 10 })
          yield* journal.setCursor(sid("2"))
          const signals = yield* collect(broker, 1)
          assert.deepStrictEqual(tags(yield* Fiber.join(signals)), ["Snapshot"])
        }),
    }))

  it.effect("a resync newer than the collection's last-applied syncId forces Snapshot on remount", () =>
    run({
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("2") })
          yield* journal.setLastResync(sid("3"))
          yield* journal.setCursor(sid("3"))
          const signals = yield* collect(broker, 1)
          assert.deepStrictEqual(tags(yield* Fiber.join(signals)), ["Snapshot"])
        }),
    }))

  it.effect("live Resync snapshots every active subscriber and ingest continues", () =>
    run({
      body: ({ broker, events, journal }) =>
        Effect.gen(function* () {
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("0") })
          const signals = yield* collect(broker, 2)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, resync("1"))
          yield* Queue.offer(events, insert("2"))
          const received = yield* Fiber.join(signals)
          assert.deepStrictEqual(tags(received), ["Snapshot", "Upsert"])
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("1"))
          assert.deepStrictEqual(yield* journal.getLastResync, Option.some(sid("1")))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("a catchup Resync snapshots active subscribers at response.lastSyncId", () =>
    run({
      catchup: CatchupClient.layerMemory({ events: [resync("3")], lastSyncId: sid("5"), epoch: Option.none() }),
      body: ({ broker }) =>
        Effect.gen(function* () {
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("0") })
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
            yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("0") })
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

  it.effect("broker scope close flushes pending last-applied marks", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      const journal = yield* SyncJournal.pipe(Effect.provide(SyncJournal.layerMemory))
      const sync = Layer.mergeAll(
        SyncTransport.layerMemory(events),
        CatchupClient.layerMemory({ events: [], lastSyncId: sid("0"), epoch: Option.none() }),
        Layer.succeed(SyncJournal, journal),
      )
      const scope = yield* Scope.make()
      const context = yield* Layer.build(
        SyncBroker.layer({ pendingLastAppliedFlushInterval: "1 hour" }).pipe(Layer.provide(sync)),
      ).pipe(Scope.provide(scope))
      const broker = Context.get(context, SyncBroker)
      const key = scopedKey({ entity: "Webhook", scope: "org-1" })
      yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("7") })
      assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())
      yield* Scope.close(scope, Exit.void)
      assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.some(sid("7")))
    }))

  it.effect("markApplied batches durable writes while subscribe sees the pending last-applied mark immediately", () =>
    run({
      broker: { pendingLastAppliedFlushInterval: "100 millis" },
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("4") })
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())
          const signals = yield* collect(broker, 1)
          yield* TestClock.adjust("100 millis")
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.some(sid("4")))
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("5") })
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.some(sid("4")))
          yield* Fiber.interrupt(signals)
        }),
    }))

  it.effect("a schema-version change orphans the old last-applied mark: remount snapshots and catchup events are not dropped", () =>
    run({
      body: ({ broker, journal, events }) =>
        Effect.gen(function* () {
          // A previous app version applied everything through #10 under schema version 1…
          const oldVersion = SchemaVersion.make(1)
          const newVersion = SchemaVersion.make(2)
          yield* journal.setCollectionLastAppliedSyncId({
            key: scopedKey({ entity: "Webhook", scope: "org-1" }),
            schemaVersion: oldVersion,
            at: sid("10"),
          })
          yield* journal.setCursor(sid("10"))

          // …then the schema changed: the saved rows were dumped, so the remount must
          // NOT trust the old last-applied mark. It snapshots at the cursor and keeps tailing.
          const signals = yield* collect(broker, 2, newVersion)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("11"))
          const received = yield* Fiber.join(signals)
          assert.deepStrictEqual(tags(received), ["Snapshot", "Upsert"])
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("10"))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("the first catchup epoch is adopted and the response applies normally", () =>
    run({
      catchup: CatchupClient.layerMemory({ events: [insert("1")], lastSyncId: sid("1"), epoch: Option.some(Epoch.make("a")) }),
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("0") })
          const signals = yield* collect(broker, 1)
          const start = yield* Effect.forkScoped(broker.start)
          assert.deepStrictEqual(tags(yield* Fiber.join(signals)), ["Upsert"])
          assert.deepStrictEqual(yield* journal.getEpoch, Option.some(Epoch.make("a")))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("a matching catchup epoch applies normally — no reset, state intact", () =>
    run({
      catchup: CatchupClient.layerMemory({ events: [insert("11")], lastSyncId: sid("11"), epoch: Option.some(Epoch.make("a")) }),
      body: ({ broker, journal, events }) =>
        Effect.gen(function* () {
          yield* journal.setEpoch(Epoch.make("a"))
          yield* journal.append([logged("10")])
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("10") })
          yield* journal.setCursor(sid("10"))
          const signals = yield* collect(broker, 1)
          const start = yield* Effect.forkScoped(broker.start)
          assert.deepStrictEqual(tags(yield* Fiber.join(signals)), ["Upsert"]) // #11, replayed/tail — no Snapshot
          assert.deepStrictEqual((yield* journal.read({ modelName: Webhook, since: sid("0") })).map((row) => row.syncId), [sid("10"), sid("11")])
          yield* Queue.shutdown(events)
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("an epoch mismatch self-heals: mounted subscribers snapshot at the new cursor past the stale tail guard", () =>
    run({
      // The server timeline reset: its new head (3) is far below the client's durable state (500).
      catchup: CatchupClient.layerMemory({ events: [], lastSyncId: sid("3"), epoch: Option.some(Epoch.make("b")) }),
      body: ({ broker, journal, events }) =>
        Effect.gen(function* () {
          yield* journal.setEpoch(Epoch.make("a"))
          yield* journal.append([logged("500")])
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("500") })
          yield* journal.setCursor(sid("500"))

          // Mounted BEFORE the reset: decision Skip, tail head = 500 — the poisoned guard.
          const signals = yield* collect(broker, 2)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("4")) // new-epoch live event, "below" the old head
          const received = yield* Fiber.join(signals)
          assert.deepStrictEqual(tags(received), ["Snapshot", "Upsert"])
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("3"))
          assert.strictEqual(received[1]!._tag === "Upsert" && received[1]!.syncId, sid("4"))

          // Local sync state was wiped and rebuilt under the new timeline.
          assert.deepStrictEqual(yield* journal.getEpoch, Option.some(Epoch.make("b")))
          yield* waitUntil(journal.getCursor.pipe(Effect.map(Option.contains(sid("4")))))
          assert.deepStrictEqual((yield* journal.read({ modelName: Webhook, since: sid("0") })).map((row) => row.syncId), [sid("4")])
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("after an epoch reset a fresh mount decides Snapshot and a pre-reset pending last-applied mark never resurfaces", () =>
    run({
      catchup: CatchupClient.layerMemory({ events: [], lastSyncId: sid("3"), epoch: Option.some(Epoch.make("b")) }),
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          yield* journal.setEpoch(Epoch.make("a"))
          yield* journal.setCursor(sid("500"))
          // An applied-but-unflushed old-epoch last-applied mark — it must die with the reset,
          // not flush later and re-poison the wiped journal.
          yield* broker.markApplied({ modelName: Webhook, scope: Option.some("org-1"), schemaVersion: version, through: sid("500") })

          const start = yield* Effect.forkScoped(broker.start)
          yield* waitUntil(journal.getEpoch.pipe(Effect.map(Option.contains(Epoch.make("b")))))
          yield* TestClock.adjust("100 millis") // the flush tick — pending must already be empty
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())

          // A mount after the reset finds no last-applied mark ⇒ Snapshot at the new cursor.
          const signals = yield* collect(broker, 1)
          const received = yield* Fiber.join(signals)
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("3"))
          yield* Fiber.interrupt(start)
        }),
    }))
})
