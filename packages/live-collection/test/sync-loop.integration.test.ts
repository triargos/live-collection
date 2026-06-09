import { Duration, Effect, Exit, Fiber, Layer, Option, Queue, Ref, Schema, Scope } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  type CatchupResponse,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncGroup,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CollectionRegistry, type CollectionRegistryShape, makeRegistry } from "../src/registry/collection-registry.js"
import { defineCollection, type ScopedHandle } from "../src/registry/define-collection.js"
import { scopedKey } from "../src/registry/collection-key.js"
import { LastSyncIdStore } from "../src/client/last-sync-id-store.js"
import { CatchupClient } from "../src/client/catchup-client.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { EventLogStore, type LoggedEvent } from "../src/client/event-log-store.js"
import { syncLoop, type SyncLoopOptions } from "../src/client/sync-loop.js"
import type { LiveRuntime } from "../src/runtime/live-runtime.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

// The whole read path end-to-end: a memory transport/catchup/cursor feeding the REAL registry,
// factory, persistence, and the syncLoop's dispatch/snapshot. Nothing below the transport is faked.
const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String })
type Webhook = typeof Webhook.Type
const k = (s: string) => ModelId.make(s)
const sid = (s: string) => SyncId.make(s)
const grp = SyncGroup.make("organization:org-1")

const insertEnv = (id: string, orgId = "org-1"): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId: sid(id.replace(/\D/g, "") || "1"),
  modelName: ModelName.make("Webhook"),
  modelId: k(id),
  syncGroups: [grp],
  createdAt: new Date(0),
  data: { id, orgId },
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

/** Poll an in-memory condition until it holds (the loop applies events on another fiber). */
const waitUntil = (cond: () => boolean): Effect.Effect<void> => {
  const attempt: Effect.Effect<void> = Effect.suspend(() =>
    cond() ? Effect.void : Effect.sleep(Duration.millis(5)).pipe(Effect.zipRight(attempt)),
  )
  return attempt.pipe(
    Effect.timeoutFail({ duration: Duration.seconds(2), onTimeout: () => new Error("condition not met") }),
    Effect.orDie,
  )
}

/** Poll an Effect condition until it returns true (for state behind a service, e.g. the cursor). */
const waitUntilE = (cond: Effect.Effect<boolean>): Effect.Effect<void> => {
  const attempt: Effect.Effect<void> = Effect.flatMap(cond, (ok) =>
    ok ? Effect.void : Effect.sleep(Duration.millis(5)).pipe(Effect.zipRight(attempt)),
  )
  return attempt.pipe(
    Effect.timeoutFail({ duration: Duration.seconds(2), onTimeout: () => new Error("condition not met") }),
    Effect.orDie,
  )
}

interface Ctx {
  readonly webhookCollection: ScopedHandle<Webhook>
  readonly store: LastSyncIdStore["Type"]
  readonly log: EventLogStore["Type"]
  readonly registry: CollectionRegistryShape
  readonly listCalls: () => number
  readonly queue: Queue.Queue<HydratedSyncEventEnvelope>
  readonly fiber: Fiber.RuntimeFiber<void>
}

/**
 * Build the full env (real registry/factory/persistence + memory transport/catchup/cursor, registry
 * shared between handles and the loop), premount the given scopes, fork `syncLoop`, then run `body`.
 */
const run = (args: {
  readonly catchup: CatchupResponse
  readonly listRows?: ReadonlyArray<Webhook>
  readonly onResync?: Effect.Effect<void>
  readonly premount?: ReadonlyArray<string>
  readonly loopOptions?: SyncLoopOptions
  readonly body: (ctx: Ctx) => Effect.Effect<void>
}): Effect.Effect<void> =>
  Effect.gen(function* () {
    let listCalls = 0
    const scope = yield* Scope.make()
    const registry = yield* Scope.extend(makeRegistry, scope)
    const runtime = { registry, persistence: makeNodeSqlitePersistence() } as unknown as LiveRuntime
    const webhookCollection = defineCollection({
      runtime,
      entity: "Webhook",
      schema: Webhook,
      getKey: (w) => k(w.id),
      scopeOf: (w) => w.orgId,
      listFn: () => Effect.sync(() => (listCalls += 1)).pipe(Effect.as(args.listRows ?? [])),
    })
    const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
    const memory = Layer.mergeAll(
      LastSyncIdStore.layerMemory,
      CatchupClient.layerMemory(args.catchup),
      SyncTransport.layerMemory(queue),
      EventLogStore.layerMemory,
      Layer.succeed(CollectionRegistry, registry),
    )

    yield* Effect.gen(function* () {
      const store = yield* LastSyncIdStore
      const log = yield* EventLogStore
      // Premount (and preload) the scopes the snapshot/dispatch should reach, BEFORE the loop runs.
      yield* Effect.forEach(
        args.premount ?? [],
        (org) => Effect.promise(() => webhookCollection(org).preload()),
        { discard: true },
      )
      const fiber = yield* Effect.forkScoped(
        syncLoop({ Webhook: webhookCollection }, args.onResync ?? Effect.void, args.loopOptions),
      )
      yield* args.body({ webhookCollection, store, log, registry, listCalls: () => listCalls, queue, fiber })
    }).pipe(Effect.scoped, Effect.provide(memory))

    yield* Scope.close(scope, Exit.void)
  })

describe("syncLoop — read path end-to-end", () => {
  it.live("cold start: a catchup Resync snapshots mounted collections and seeds the cursor", () =>
    run({
      catchup: { events: [resyncAllEnv("5")], lastSyncId: sid("5") },
      listRows: [{ id: "w1", orgId: "org-1" }, { id: "w2", orgId: "org-1" }],
      premount: ["org-1"],
      body: ({ webhookCollection, store }) =>
        Effect.gen(function* () {
          const coll = webhookCollection("org-1")
          yield* waitUntil(() => coll.has(k("w1")) && coll.has(k("w2")))
          assert.isTrue(coll.has(k("w1")))
          assert.deepStrictEqual(yield* store.get, Option.some(sid("5")))
        }),
    }))

  it.live("warm start: catchup deltas are dispatched and the cursor advances", () =>
    run({
      catchup: { events: [insertEnv("w1"), insertEnv("w2")], lastSyncId: sid("9") },
      premount: ["org-1"],
      body: ({ webhookCollection, store }) =>
        Effect.gen(function* () {
          const coll = webhookCollection("org-1")
          yield* waitUntil(() => coll.has(k("w1")) && coll.has(k("w2")))
          assert.deepStrictEqual(yield* store.get, Option.some(sid("9")))
        }),
    }))

  it.live("live: an Insert then a Delete pushed on the stream land in the collection", () =>
    run({
      catchup: { events: [], lastSyncId: sid("1") },
      premount: ["org-1"],
      body: ({ webhookCollection, queue }) =>
        Effect.gen(function* () {
          const coll = webhookCollection("org-1")
          yield* Queue.offer(queue, insertEnv("w1"))
          yield* waitUntil(() => coll.has(k("w1")))
          assert.isTrue(coll.has(k("w1")))
          yield* Queue.offer(queue, deleteEnv("w1", "7"))
          yield* waitUntil(() => !coll.has(k("w1")))
          assert.isFalse(coll.has(k("w1")))
        }),
    }))

  it.live("live: a Resync clears the cursor and fires onResync (loop stops)", () =>
    Effect.gen(function* () {
      const fired = yield* Ref.make(false)
      yield* run({
        catchup: { events: [], lastSyncId: sid("3") },
        onResync: Ref.set(fired, true),
        body: ({ store, queue, fiber }) =>
          Effect.gen(function* () {
            yield* waitUntil(() => true) // let the cycle reach the tail
            yield* Queue.offer(queue, resyncAllEnv("8"))
            yield* Fiber.join(fiber) // start completes once the live resync stops the tail
            assert.isTrue(yield* Ref.get(fired))
            assert.isTrue(Option.isNone(yield* store.get))
          }),
      })
    }))

  it.live("snapshot reconcile: a row absent from the snapshot is removed (delete-absent)", () =>
    run({
      catchup: { events: [resyncAllEnv("5")], lastSyncId: sid("5") },
      listRows: [{ id: "w1", orgId: "org-1" }],
      premount: ["org-1"],
      body: ({ webhookCollection }) =>
        Effect.gen(function* () {
          const coll = webhookCollection("org-1")
          yield* coll.utils.writeSynced({ id: "stale", orgId: "org-1" }) // pre-existing, omitted by snapshot
          yield* waitUntil(() => coll.has(k("w1")) && !coll.has(k("stale")))
          assert.isTrue(coll.has(k("w1")))
          assert.isFalse(coll.has(k("stale")))
        }),
    }))

  it.live("dispatch: an event for an unmounted scope is ignored without error", () =>
    run({
      catchup: { events: [], lastSyncId: sid("1") },
      premount: ["org-1"],
      body: ({ webhookCollection, queue }) =>
        Effect.gen(function* () {
          const coll = webhookCollection("org-1")
          yield* Queue.offer(queue, insertEnv("w9", "org-2")) // org-2 not mounted
          yield* Queue.offer(queue, insertEnv("w1", "org-1")) // a later org-1 event proves the loop kept running
          yield* waitUntil(() => coll.has(k("w1")))
          assert.isFalse(coll.has(k("w9")))
        }),
    }))

  it.live("dispatch: an unknown model is skipped", () =>
    run({
      catchup: { events: [], lastSyncId: sid("1") },
      premount: ["org-1"],
      body: ({ webhookCollection, queue }) =>
        Effect.gen(function* () {
          const coll = webhookCollection("org-1")
          const ghost: HydratedSyncEventEnvelope = {
            _tag: "Insert",
            syncId: sid("1"),
            modelName: ModelName.make("Ghost"), // no map entry ⇒ skipped
            modelId: k("ghost"),
            syncGroups: [grp],
            createdAt: new Date(0),
            data: { id: "ghost", orgId: "org-1" },
          }
          yield* Queue.offer(queue, ghost)
          yield* Queue.offer(queue, insertEnv("w1", "org-1"))
          yield* waitUntil(() => coll.has(k("w1")))
          assert.isFalse(coll.has(k("ghost")))
        }),
    }))

  it.live("a scope mounted after its events streamed past converges via REPLAY, not listFn", () =>
    run({
      catchup: { events: [], lastSyncId: sid("0") }, // cursor seeds at "0"
      body: ({ webhookCollection, log, store, queue, listCalls }) =>
        Effect.gen(function* () {
          // org-2 was caught up to "0" in a prior session, then unmounted (its base watermark survives).
          const org2 = scopedKey({ entity: "Webhook", scope: "org-2" })
          yield* log.setBaseWatermark({ key: org2, at: sid("0") })

          // An event for org-2 streams past while it is NOT mounted: the loop logs it but drops the apply.
          yield* Queue.offer(queue, insertEnv("w1", "org-2")) // syncId "1"
          yield* waitUntilE(store.get.pipe(Effect.map(Option.contains(sid("1")))))

          // Now org-2 mounts — it should heal by replaying the logged event, NOT by calling listFn.
          const coll = webhookCollection("org-2")
          yield* Effect.promise(() => coll.preload())
          yield* waitUntil(() => coll.has(k("w1")))
          assert.isTrue(coll.has(k("w1")))
          assert.strictEqual(listCalls(), 0) // converged by replay alone — no bootstrap
        }),
    }))

  it.live("a resync that passed while a scope was unmounted forces BOOTSTRAP, not replay (D9)", () =>
    run({
      catchup: { events: [resyncAllEnv("5")], lastSyncId: sid("5") }, // the loop records lastResyncAt = "5"
      listRows: [{ id: "w1", orgId: "org-2" }], // what bootstrap (listFn) returns for org-2
      body: ({ webhookCollection, log, listCalls }) =>
        Effect.gen(function* () {
          // Prior session: org-2 was complete to "1". A resync ("5") then passed while it was unmounted.
          const org2 = scopedKey({ entity: "Webhook", scope: "org-2" })
          yield* log.setBaseWatermark({ key: org2, at: sid("1") })
          yield* waitUntilE(log.getLastResync.pipe(Effect.map(Option.contains(sid("5")))))

          // org-2 mounts now: base "1" < resync "5" ⇒ the local log can't be trusted ⇒ bootstrap.
          // (Without the resync, base "1" < cursor "5" with an empty log would REPLAY ⇒ stay empty.)
          const coll = webhookCollection("org-2")
          yield* Effect.promise(() => coll.preload())
          yield* waitUntil(() => coll.has(k("w1")))
          assert.isTrue(coll.has(k("w1")))
          assert.strictEqual(listCalls(), 1) // healed by listFn, not by replay
        }),
    }))

  it.live("the loop prunes the log as events arrive, advancing the model floor", () =>
    run({
      catchup: { events: [], lastSyncId: sid("0") },
      premount: ["org-1"],
      loopOptions: { prune: { perModel: 1, total: 100, everyEvents: 1 } }, // keep only the newest per model
      body: ({ log, queue }) =>
        Effect.gen(function* () {
          yield* Queue.offer(queue, insertEnv("w1", "org-1")) // syncId "1"
          yield* Queue.offer(queue, insertEnv("w2", "org-1")) // syncId "2" ⇒ "1" pruned
          // floor advances to the highest deleted syncId once the loop prunes past it.
          yield* waitUntilE(log.floor(ModelName.make("Webhook")).pipe(Effect.map(Option.contains(sid("1")))))
          assert.deepStrictEqual(yield* log.floor(ModelName.make("Webhook")), Option.some(sid("1")))
        }),
    }))

  it.live("a scope whose base is below the model floor BOOTSTRAPs (the log can't cover the gap)", () =>
    run({
      catchup: { events: [], lastSyncId: sid("5") },
      listRows: [{ id: "w1", orgId: "org-2" }],
      body: ({ webhookCollection, log, store, listCalls }) =>
        Effect.gen(function* () {
          const org2 = scopedKey({ entity: "Webhook", scope: "org-2" })
          const logged = (syncId: string, id: string): LoggedEvent => ({
            syncId: sid(syncId),
            modelName: ModelName.make("Webhook"),
            scope: Option.some("org-2"),
            tag: "Insert",
            modelId: k(id),
            data: Option.some({ id, orgId: "org-2" }),
          })
          // org-2's base is "1", but events 3 & 4 streamed and pruning kept only the newest ⇒ floor "3" > base "1".
          yield* log.append([logged("3", "old3"), logged("4", "old4")])
          yield* log.setBaseWatermark({ key: org2, at: sid("1") })
          yield* log.prune({ perModel: 1, total: 100 }) // keeps syncId "4", deletes "3" ⇒ floor("Webhook") = "3"
          yield* waitUntilE(store.get.pipe(Effect.map(Option.contains(sid("5"))))) // cursor must reflect catchup first

          const coll = webhookCollection("org-2")
          yield* Effect.promise(() => coll.preload())
          yield* waitUntil(() => coll.has(k("w1")))
          assert.isTrue(coll.has(k("w1"))) // healed by listFn (current truth), NOT a gap-ridden replay
          assert.isFalse(coll.has(k("old4"))) // the surviving logged event was NOT replayed in
          assert.strictEqual(listCalls(), 1)
        }),
    }))

  it.live("dispatch: a Delete fans out across mounted scopes, idempotently", () =>
    run({
      catchup: { events: [], lastSyncId: sid("1") },
      premount: ["org-1", "org-2"],
      body: ({ webhookCollection, queue }) =>
        Effect.gen(function* () {
          const org1 = webhookCollection("org-1")
          const org2 = webhookCollection("org-2")
          yield* Queue.offer(queue, insertEnv("w1", "org-1"))
          yield* Queue.offer(queue, insertEnv("w2", "org-2"))
          yield* waitUntil(() => org1.has(k("w1")) && org2.has(k("w2")))
          yield* Queue.offer(queue, deleteEnv("w1", "7")) // delete has no scope ⇒ fanned out
          yield* waitUntil(() => !org1.has(k("w1")))
          assert.isFalse(org1.has(k("w1"))) // owner removed it
          assert.isTrue(org2.has(k("w2"))) // sibling untouched (no-op there)
        }),
    }))
})
