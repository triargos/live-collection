import { Context, DateTime, Deferred, Effect, Exit, Fiber, Layer, Option, Queue, Ref, Scope } from "effect"
import { TestClock } from "effect/testing"
import { assert, describe, it } from "@effect/vitest"
import {
  deriveGroup,
  Epoch,
  HydrateBatchResult,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  ResyncTarget,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CatchupClient, CatchupFailed } from "../src/client/catchup-client.js"
import { HydrateClient } from "../src/client/hydrate-client.js"
import { SyncJournal, type SyncJournalShape, type JournalEvent } from "../src/client/sync-journal.js"
import {
  SyncBroker,
  type SyncBrokerOptions,
  type SyncBrokerShape,
  type SyncSignal,
} from "../src/client/sync-broker.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { SchemaVersion } from "../src/core/schema-version.js"
import { globalKey, scopedKey } from "../src/core/collection-key.js"

const sid = (value: string) => SyncId.make(value)
const version = SchemaVersion.make(1)
const modelId = (value: string) => ModelId.make(value)
const Webhook = ModelName.make("Webhook")
const Setting = ModelName.make("Setting")
const group = deriveGroup(["organization", "org-1"])
const epoch = DateTime.makeUnsafe(0).pipe(DateTime.toDateUtc)

const insert = (syncId: string, modelName = Webhook, id = `w-${syncId}`): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId: sid(syncId),
  modelName,
  modelId: modelId(id),
  syncGroups: [group],
  createdAt: epoch,
  data: { id, orgId: "org-1" },
})

const del = (syncId: string, id: string): HydratedSyncEventEnvelope => ({
  _tag: "Delete",
  syncId: sid(syncId),
  modelName: Webhook,
  modelId: modelId(id),
  syncGroups: [group],
  createdAt: epoch,
})

const resync = (syncId: string): HydratedSyncEventEnvelope => ({
  _tag: "Resync",
  syncId: sid(syncId),
  target: ResyncTarget.cases.All.make({}),
  syncGroups: [group],
  createdAt: epoch,
})

const logged = (syncId: string): JournalEvent => ({
  syncId: sid(syncId),
  modelName: Webhook,
  tag: "Insert",
  modelId: modelId(`w-${syncId}`),
  data: Option.some({ id: `w-${syncId}`, orgId: "org-1" }),
})

/**
 * Attach a subscriber whose `apply` enqueues every signal (then runs the optional
 * `gate`, so a test can suspend a specific apply before it returns). The broker acks
 * each signal itself after `apply` returns — there is no manual ack anywhere below.
 */
const attach = (
  broker: SyncBrokerShape,
  options?: {
    readonly modelName?: ModelName
    readonly scope?: Option.Option<string>
    readonly schemaVersion?: SchemaVersion
    readonly gate?: (signal: SyncSignal) => Effect.Effect<void>
  },
): Effect.Effect<
  {
    readonly take: (count: number) => Effect.Effect<ReadonlyArray<SyncSignal>>
    readonly fiber: Fiber.Fiber<void>
  },
  never,
  Scope.Scope
> =>
  Effect.gen(function* () {
    const seen = yield* Queue.unbounded<SyncSignal>()
    const gate = options?.gate ?? (() => Effect.void)
    const fiber = yield* Effect.forkScoped(
      broker.attachSubscriber({
        modelName: options?.modelName ?? Webhook,
        scope: options?.scope ?? Option.some("org-1"),
        schemaVersion: options?.schemaVersion ?? version,
        apply: (signal) => Queue.offer(seen, signal).pipe(Effect.andThen(gate(signal)), Effect.asVoid),
      }),
    )
    const take = (count: number): Effect.Effect<ReadonlyArray<SyncSignal>> =>
      Effect.all(Array.from({ length: count }, () => Queue.take(seen)))
    return { take, fiber }
  })

const run = <A, E>(options: {
  readonly catchup?: Layer.Layer<CatchupClient>
  readonly hydrate?: Layer.Layer<HydrateClient>
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
      const base = Layer.mergeAll(
        SyncTransport.layerMemory(events),
        options.catchup ?? CatchupClient.layerMemory({ events: [], lastSyncId: sid("0"), epoch: Option.none() }),
        SyncJournal.layerMemory,
      )
      const sync: typeof base = options.hydrate === undefined ? base : Layer.mergeAll(base, options.hydrate)
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
  it.effect("a cold attach snapshots at the last-ingested syncId and the broker's own ack is visible to the next attach", () =>
    run({
      body: ({ broker, events }) =>
        Effect.gen(function* () {
          const cold = yield* attach(broker)
          const coldSignals = yield* cold.take(1)
          assert.deepStrictEqual(tags(coldSignals), ["Snapshot"])
          assert.strictEqual(coldSignals[0]!._tag === "Snapshot" && coldSignals[0]!.at, sid("0"))
          yield* Fiber.interrupt(cold.fiber)

          // No manual ack anywhere: applying the Snapshot made the broker record
          // "applied through 0" itself, so the next attach decides Skip, not Snapshot.
          const warm = yield* attach(broker)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1"))
          assert.deepStrictEqual(tags(yield* warm.take(1)), ["Upsert"])
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("live ingest fans matching Upserts and Deletes out, journals them, and advances the last-ingested syncId", () =>
    run({
      body: ({ broker, events, journal }) =>
        Effect.gen(function* () {
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("0") })
          yield* journal.setCollectionLastAppliedSyncId({ key: globalKey("Setting"), schemaVersion: version, at: sid("0") })
          const mine = yield* attach(broker)
          const other = yield* attach(broker, { modelName: Setting, scope: Option.none() })
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1"))
          yield* Queue.offer(events, del("2", "w-1"))
          yield* Queue.offer(events, insert("3", Setting, "s-1"))
          assert.deepStrictEqual(tags(yield* mine.take(2)), ["Upsert", "Delete"])
          assert.deepStrictEqual(tags(yield* other.take(1)), ["Upsert"])
          assert.deepStrictEqual((yield* journal.read({ modelName: Webhook, since: sid("0") })).map((row) => row.syncId), [sid("1"), sid("2")])
          assert.deepStrictEqual((yield* journal.read({ modelName: Setting, since: sid("0") })).map((row) => row.syncId), [sid("3")])
          yield* waitUntil(journal.getLastIngestedSyncId.pipe(Effect.map(Option.contains(sid("3")))))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("a late attach replays a covered gap, tails live, and drops older buffered duplicates", () =>
    run({
      body: ({ broker, events, journal }) =>
        Effect.gen(function* () {
          yield* journal.append([logged("2")])
          yield* journal.setLastIngestedSyncId(sid("2"))
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("1") })
          const sub = yield* attach(broker)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1"))
          yield* Queue.offer(events, insert("3"))
          const received = yield* sub.take(2)
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
          // Durable mark lags at 1; the fresher "applied through 2" exists only as the
          // pending ack the broker records when the subscriber applies event 2.
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("1") })
          const sub = yield* attach(broker, {
            // Event 3 must not be acked before the trim tick: its apply never returns.
            gate: (signal) => (signal._tag === "Upsert" && signal.syncId === sid("3") ? Effect.never : Effect.void),
          })
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("1")) // at the tail guard (lastApplied 1) — dropped, never applied
          yield* Queue.offer(events, insert("2"))
          yield* sub.take(1) // apply(2) returned ⇒ the broker's ack for 2 is pending
          yield* Queue.offer(events, insert("3")) // 3rd ingest ⇒ trim tick: flush marks, then prune
          // Ingest is done once the last-ingested mark reaches 3; the trim tick runs within that same chain,
          // so the journal now holds the pruned state. With the flush, min last-applied is 2 ⇒
          // rows 1 AND 2 are dead weight; a stale (durable-only) prune would keep row 2.
          yield* waitUntil(journal.getLastIngestedSyncId.pipe(Effect.map(Option.contains(sid("3")))))
          const rows = yield* journal.read({ modelName: Webhook, since: sid("0") })
          assert.deepStrictEqual(rows.map((row) => row.syncId), [sid("3")])
          assert.deepStrictEqual(yield* journal.highestPrunedSyncId(Webhook), Option.none()) // dead weight is boundary-neutral
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("a prune boundary above the collection's last-applied syncId forces Snapshot", () =>
    run({
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          yield* journal.append([logged("1"), logged("2")])
          yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("0") })
          yield* journal.prune({ maxEventsPerModel: 1, maxEventsTotal: 10 })
          yield* journal.setLastIngestedSyncId(sid("2"))
          const sub = yield* attach(broker)
          assert.deepStrictEqual(tags(yield* sub.take(1)), ["Snapshot"])
        }),
    }))

  it.effect("a resync newer than the collection's last-applied syncId forces Snapshot on remount", () =>
    run({
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("2") })
          yield* journal.setLastResync(sid("3"))
          yield* journal.setLastIngestedSyncId(sid("3"))
          const sub = yield* attach(broker)
          assert.deepStrictEqual(tags(yield* sub.take(1)), ["Snapshot"])
        }),
    }))

  it.effect("live Resync snapshots every active subscriber and ingest continues", () =>
    run({
      body: ({ broker, events, journal }) =>
        Effect.gen(function* () {
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("0") })
          const sub = yield* attach(broker)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, resync("1"))
          yield* Queue.offer(events, insert("2"))
          const received = yield* sub.take(2)
          assert.deepStrictEqual(tags(received), ["Snapshot", "Upsert"])
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("1"))
          assert.deepStrictEqual(yield* journal.getLastResync, Option.some(sid("1")))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("a catchup Resync snapshots active subscribers at response.lastSyncId", () =>
    run({
      catchup: CatchupClient.layerMemory({ events: [resync("3")], lastSyncId: sid("5"), epoch: Option.none() }),
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("0") })
          const sub = yield* attach(broker)
          const start = yield* Effect.forkScoped(broker.start)
          const received = yield* sub.take(1)
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
        body: ({ broker, events, journal }) =>
          Effect.gen(function* () {
            yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("0") })
            const sub = yield* attach(broker)
            yield* Effect.forkScoped(broker.start)
            yield* Queue.offer(events, insert("1"))
            assert.deepStrictEqual(tags(yield* sub.take(1)), ["Upsert"])
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
      // The only way to a pending mark is through the broker's own ack: attach and
      // let the subscriber apply the cold Snapshot at the seeded position.
      yield* journal.setLastIngestedSyncId(sid("7"))
      const seen = yield* Queue.unbounded<SyncSignal>()
      const attached = yield* Effect.forkChild(
        broker.attachSubscriber({
          modelName: Webhook,
          scope: Option.some("org-1"),
          schemaVersion: version,
          apply: (signal) => Queue.offer(seen, signal).pipe(Effect.asVoid),
        }),
      )
      const first = yield* Queue.take(seen)
      assert.strictEqual(first._tag === "Snapshot" && first.at, sid("7"))
      assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())
      yield* Fiber.interrupt(attached)
      yield* Scope.close(scope, Exit.void)
      assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.some(sid("7")))
    }))

  it.effect("acks batch durable writes while the next attach sees the pending mark immediately", () =>
    run({
      broker: { pendingLastAppliedFlushInterval: "100 millis" },
      body: ({ broker, events, journal }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          yield* journal.setLastIngestedSyncId(sid("1"))
          // Cold attach applies Snapshot(1); the broker's ack for it is pending, not durable.
          const first = yield* attach(broker)
          assert.deepStrictEqual(tags(yield* first.take(1)), ["Snapshot"])
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())
          // A second attach planned NOW sees the pending mark ⇒ decides Skip, not Snapshot.
          const second = yield* attach(broker)
          // The flush tick persists the batched mark.
          yield* TestClock.adjust("100 millis")
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.some(sid("1")))
          // Second's first signal is the live Upsert — proof it skipped on the pending mark.
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("2"))
          assert.deepStrictEqual(tags(yield* second.take(1)), ["Upsert"])
          // The ack for 2 is pending again — durable still at 1 until the next tick.
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.some(sid("1")))
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("the broker acks only after apply returns: a suspended apply holds the last-applied mark back", () =>
    run({
      broker: { pendingLastAppliedFlushInterval: "100 millis" },
      body: ({ broker, journal }) =>
        Effect.gen(function* () {
          const key = scopedKey({ entity: "Webhook", scope: "org-1" })
          const release = yield* Deferred.make<void>()
          yield* journal.setLastIngestedSyncId(sid("2"))
          const sub = yield* attach(broker, { gate: () => Deferred.await(release) })
          const signals = yield* sub.take(1)
          assert.strictEqual(signals[0]!._tag === "Snapshot" && signals[0]!.at, sid("2"))
          // apply is still suspended — no ack exists yet, so a flush tick writes nothing.
          yield* TestClock.adjust("100 millis")
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())
          // apply returns ⇒ the broker acks ⇒ the next flush persists the mark.
          yield* Deferred.succeed(release, undefined)
          yield* waitUntil(
            TestClock.adjust("100 millis").pipe(
              Effect.andThen(journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version })),
              Effect.map(Option.isSome),
            ),
          )
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.some(sid("2")))
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
          yield* journal.setLastIngestedSyncId(sid("10"))

          // …then the schema changed: the saved rows were dumped, so the remount must
          // NOT trust the old last-applied mark. It snapshots at the last-ingested syncId and keeps tailing.
          const sub = yield* attach(broker, { schemaVersion: newVersion })
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("11"))
          const received = yield* sub.take(2)
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
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("0") })
          const sub = yield* attach(broker)
          const start = yield* Effect.forkScoped(broker.start)
          assert.deepStrictEqual(tags(yield* sub.take(1)), ["Upsert"])
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
          yield* journal.setLastIngestedSyncId(sid("10"))
          const sub = yield* attach(broker)
          const start = yield* Effect.forkScoped(broker.start)
          assert.deepStrictEqual(tags(yield* sub.take(1)), ["Upsert"]) // #11, replayed/tail — no Snapshot
          assert.deepStrictEqual((yield* journal.read({ modelName: Webhook, since: sid("0") })).map((row) => row.syncId), [sid("10"), sid("11")])
          yield* Queue.shutdown(events)
          yield* Fiber.interrupt(start)
        }),
    }))

  it.effect("an epoch mismatch self-heals: mounted subscribers snapshot at the new position past the stale tail guard", () =>
    run({
      // The server timeline reset: its new head (3) is far below the client's durable state (500).
      catchup: CatchupClient.layerMemory({ events: [], lastSyncId: sid("3"), epoch: Option.some(Epoch.make("b")) }),
      body: ({ broker, journal, events }) =>
        Effect.gen(function* () {
          yield* journal.setEpoch(Epoch.make("a"))
          yield* journal.append([logged("500")])
          yield* journal.setCollectionLastAppliedSyncId({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), schemaVersion: version, at: sid("500") })
          yield* journal.setLastIngestedSyncId(sid("500"))

          // Mounted BEFORE the reset: decision Skip, tail head = 500 — the poisoned guard.
          const sub = yield* attach(broker)
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("4")) // new-epoch live event, "below" the old head
          const received = yield* sub.take(2)
          assert.deepStrictEqual(tags(received), ["Snapshot", "Upsert"])
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("3"))
          assert.strictEqual(received[1]!._tag === "Upsert" && received[1]!.syncId, sid("4"))

          // Local sync state was wiped and rebuilt under the new timeline.
          assert.deepStrictEqual(yield* journal.getEpoch, Option.some(Epoch.make("b")))
          yield* waitUntil(journal.getLastIngestedSyncId.pipe(Effect.map(Option.contains(sid("4")))))
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
          yield* journal.setLastIngestedSyncId(sid("500"))
          // An applied-but-unflushed old-epoch mark: attach, apply the Snapshot at 500 —
          // the broker's ack for it is pending, never yet durable. It must die with the
          // reset, not flush later and re-poison the wiped journal.
          const stale = yield* attach(broker)
          const staleSignals = yield* stale.take(1)
          assert.strictEqual(staleSignals[0]!._tag === "Snapshot" && staleSignals[0]!.at, sid("500"))
          yield* Fiber.interrupt(stale.fiber)

          const start = yield* Effect.forkScoped(broker.start)
          yield* waitUntil(journal.getEpoch.pipe(Effect.map(Option.contains(Epoch.make("b")))))
          yield* TestClock.adjust("100 millis") // the flush tick — pending must already be empty
          assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())

          // A mount after the reset finds no last-applied mark ⇒ Snapshot at the new position.
          const fresh = yield* attach(broker)
          const received = yield* fresh.take(1)
          assert.strictEqual(received[0]!._tag === "Snapshot" && received[0]!.at, sid("3"))
          yield* Fiber.interrupt(start)
        }),
    }))
})

// ── ensureSubset — the loadBy* engine ──

const subset = { indexKey: "templateId", keyValue: "t1" }

/** Members-for-everything hydrate stub with a call counter and a fixed stamp. */
const countingHydrate = (args: {
  readonly calls: Ref.Ref<number>
  readonly stamp: string
  readonly rows?: ReadonlyArray<unknown>
  readonly epoch?: Epoch
  readonly forbidden?: boolean
}): Layer.Layer<HydrateClient> =>
  HydrateClient.layerMemory((request) =>
    Ref.update(args.calls, (n) => n + 1).pipe(
      Effect.as({
        results: request.requests.map((r) =>
          args.forbidden === true
            ? HydrateBatchResult.cases.Forbidden.make({ request: r })
            : HydrateBatchResult.cases.Members.make({ request: r, rows: args.rows ?? [] }),
        ),
        lastSyncId: sid(args.stamp),
        epoch: Option.fromNullishOr(args.epoch),
      }),
    ),
  )

/** Run one ensure, recording every replaceSlice and apply the broker drives. */
const ensureRecorded = (broker: SyncBrokerShape) =>
  Effect.gen(function* () {
    const slices = yield* Ref.make<ReadonlyArray<{ rows: ReadonlyArray<unknown>; at: SyncId }>>([])
    const applied = yield* Ref.make<ReadonlyArray<SyncSignal>>([])
    const ensure = broker.ensureSubset({
      modelName: Webhook,
      scope: Option.none(),
      schemaVersion: version,
      subset,
      replaceSlice: (rows, at) => Ref.update(slices, (all) => [...all, { rows, at }]),
      apply: (signal) => Ref.update(applied, (all) => [...all, signal]),
    })
    return { ensure, slices: Ref.get(slices), applied: Ref.get(applied) }
  })

describe("SyncBroker.ensureSubset", () => {
  it.effect("a fresh subset takes the Snapshot tier: one fetch, the slice lands with the server stamp, the mark holds", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      yield* run({
        hydrate: countingHydrate({ calls, stamp: "5", rows: [{ id: "v1" }, { id: "v2" }] }),
        body: ({ broker }) =>
          Effect.gen(function* () {
            const first = yield* ensureRecorded(broker)
            yield* first.ensure
            assert.deepStrictEqual(yield* first.slices, [{ rows: [{ id: "v1" }, { id: "v2" }], at: sid("5") }])
            assert.deepStrictEqual(yield* first.applied, []) // empty journal ⇒ nothing to replay
            assert.deepStrictEqual(yield* broker.coveredSubsets({ modelName: Webhook, scope: Option.none(), schemaVersion: version }), [
              { subset, at: sid("5") },
            ])

            // Second ensure: the mark is current ⇒ Skip — no fetch, no slice.
            const second = yield* ensureRecorded(broker)
            yield* second.ensure
            assert.deepStrictEqual(yield* second.slices, [])
          }),
      })
      assert.strictEqual(yield* Ref.get(calls), 1)
    }))

  it.effect("a journal-covered gap takes the Replay tier — local slice, zero fetches", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      yield* run({
        hydrate: countingHydrate({ calls, stamp: "2" }),
        body: ({ broker, journal }) =>
          Effect.gen(function* () {
            const first = yield* ensureRecorded(broker)
            yield* first.ensure // Snapshot tier, mark at 2
            yield* journal.append([logged("3"), logged("4")])
            yield* journal.setLastIngestedSyncId(sid("4"))

            const replay = yield* ensureRecorded(broker)
            yield* replay.ensure
            assert.deepStrictEqual(yield* replay.slices, []) // no fetch: journal covers the gap
            assert.deepStrictEqual(
              (yield* replay.applied).map((s) => (s._tag === "Upsert" ? s.syncId : s._tag)),
              [sid("3"), sid("4")],
            )

            const third = yield* ensureRecorded(broker)
            yield* third.ensure // mark advanced to 4 ⇒ Skip
            assert.deepStrictEqual(yield* third.applied, [])
          }),
      })
      assert.strictEqual(yield* Ref.get(calls), 1)
    }))

  it.effect("events landing between the head-read and the response are re-applied from the journal after the slice", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      yield* run({
        // Stamp 2: the server read its head before event 3 was appended.
        hydrate: countingHydrate({ calls, stamp: "2", rows: [{ id: "v1" }] }),
        body: ({ broker, journal }) =>
          Effect.gen(function* () {
            yield* journal.append([logged("3")])
            yield* journal.setLastIngestedSyncId(sid("3"))
            const first = yield* ensureRecorded(broker)
            yield* first.ensure
            assert.deepStrictEqual(yield* first.slices, [{ rows: [{ id: "v1" }], at: sid("2") }])
            // The journal event above the stamp replays on top of the slice — the
            // idempotence story: worst case it re-applies an already-reflected row.
            assert.deepStrictEqual(
              (yield* first.applied).map((s) => (s._tag === "Upsert" ? s.syncId : s._tag)),
              [sid("3")],
            )
            assert.deepStrictEqual(yield* broker.coveredSubsets({ modelName: Webhook, scope: Option.none(), schemaVersion: version }), [
              { subset, at: sid("3") },
            ])
          }),
      })
    }))

  it.effect("Forbidden fails the ensure typed, writes no mark, and the next ensure asks the server again", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      yield* run({
        hydrate: countingHydrate({ calls, stamp: "5", forbidden: true }),
        body: ({ broker }) =>
          Effect.gen(function* () {
            const first = yield* ensureRecorded(broker)
            const error = yield* Effect.flip(first.ensure)
            assert.strictEqual(error._tag, "SubsetForbidden")
            assert.deepStrictEqual(
              yield* broker.coveredSubsets({ modelName: Webhook, scope: Option.none(), schemaVersion: version }),
              [],
            )
            const second = yield* ensureRecorded(broker)
            yield* Effect.flip(second.ensure) // retried — not remembered as denied
          }),
      })
      assert.strictEqual(yield* Ref.get(calls), 2)
    }))

  it.effect("a pruned gap forces the Snapshot tier — the journal can no longer replay the subset forward", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      yield* run({
        hydrate: countingHydrate({ calls, stamp: "1" }),
        body: ({ broker, journal }) =>
          Effect.gen(function* () {
            const first = yield* ensureRecorded(broker)
            yield* first.ensure // mark at 1
            yield* broker.coveredSubsets({ modelName: Webhook, scope: Option.none(), schemaVersion: version }) // flush the pending mark so prune sees it
            // Two newer events; caps of one force pruning to eat event 2 — boundary 2 > mark 1.
            yield* journal.append([logged("2"), logged("3")])
            yield* journal.setLastIngestedSyncId(sid("3"))
            yield* journal.prune({ maxEventsPerModel: 1, maxEventsTotal: 100 })
            const second = yield* ensureRecorded(broker)
            yield* second.ensure
            assert.strictEqual((yield* second.slices).length, 1) // refetched, not replayed
          }),
      })
      assert.strictEqual(yield* Ref.get(calls), 2)
    }))

  it.effect("a resync newer than the mark forces the Snapshot tier", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      yield* run({
        hydrate: countingHydrate({ calls, stamp: "2" }),
        body: ({ broker, journal }) =>
          Effect.gen(function* () {
            const first = yield* ensureRecorded(broker)
            yield* first.ensure // mark at 2
            yield* journal.setLastResync(sid("5"))
            yield* journal.setLastIngestedSyncId(sid("5"))
            const second = yield* ensureRecorded(broker)
            yield* second.ensure
            assert.strictEqual((yield* second.slices).length, 1)
          }),
      })
      assert.strictEqual(yield* Ref.get(calls), 2)
    }))

  it.effect("an epoch mismatch on the response fails the ensure and writes no mark — ingest owns the reset", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      yield* run({
        hydrate: countingHydrate({ calls, stamp: "5", epoch: Epoch.make("B") }),
        body: ({ broker, journal }) =>
          Effect.gen(function* () {
            yield* journal.setEpoch(Epoch.make("A"))
            const first = yield* ensureRecorded(broker)
            const error = yield* Effect.flip(first.ensure)
            assert.strictEqual(error._tag, "HydrateFailed")
            assert.isTrue(error._tag === "HydrateFailed" && error.reason.includes("epoch"))
            assert.deepStrictEqual(
              yield* broker.coveredSubsets({ modelName: Webhook, scope: Option.none(), schemaVersion: version }),
              [],
            )
          }),
      })
    }))

  it.effect("apply's returned subset keys advance their coverage marks through each acked signal", () =>
    run({
      body: ({ broker, events }) =>
        Effect.gen(function* () {
          yield* Effect.forkScoped(
            broker.attachSubscriber({
              modelName: Webhook,
              scope: Option.none(),
              schemaVersion: version,
              // A partial drain reporting one covered subset for every signal.
              apply: () => Effect.succeed([subset]),
            }),
          )
          const start = yield* Effect.forkScoped(broker.start)
          yield* Queue.offer(events, insert("7"))
          yield* waitUntil(
            broker
              .coveredSubsets({ modelName: Webhook, scope: Option.none(), schemaVersion: version })
              .pipe(Effect.map((marks) => marks.some((m) => m.at === sid("7")))),
          )
          yield* Fiber.interrupt(start)
        }),
    }))
})
