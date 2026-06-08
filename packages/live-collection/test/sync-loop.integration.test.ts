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
import { CollectionRegistry, makeRegistry } from "../src/registry/collection-registry.js"
import { defineCollection, type ScopedHandle } from "../src/registry/define-collection.js"
import { LastSyncIdStore } from "../src/client/last-sync-id-store.js"
import { CatchupClient } from "../src/client/catchup-client.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { syncLoop } from "../src/client/sync-loop.js"
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

interface Ctx {
  readonly webhookCollection: ScopedHandle<Webhook>
  readonly store: LastSyncIdStore["Type"]
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
  readonly body: (ctx: Ctx) => Effect.Effect<void>
}): Effect.Effect<void> =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const registry = yield* Scope.extend(makeRegistry, scope)
    const runtime = { registry, persistence: makeNodeSqlitePersistence() } as unknown as LiveRuntime
    const webhookCollection = defineCollection({
      runtime,
      entity: "Webhook",
      schema: Webhook,
      getKey: (w) => k(w.id),
      scopeOf: (w) => w.orgId,
      listFn: () => Effect.succeed(args.listRows ?? []),
    })
    const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
    const memory = Layer.mergeAll(
      LastSyncIdStore.layerMemory,
      CatchupClient.layerMemory(args.catchup),
      SyncTransport.layerMemory(queue),
      Layer.succeed(CollectionRegistry, registry),
    )

    yield* Effect.gen(function* () {
      const store = yield* LastSyncIdStore
      // Premount (and preload) the scopes the snapshot/dispatch should reach, BEFORE the loop runs.
      yield* Effect.forEach(
        args.premount ?? [],
        (org) => Effect.promise(() => webhookCollection(org).preload()),
        { discard: true },
      )
      const fiber = yield* Effect.forkScoped(syncLoop({ Webhook: webhookCollection }, args.onResync ?? Effect.void))
      yield* args.body({ webhookCollection, store, queue, fiber })
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
