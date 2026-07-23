import { DateTime, Duration, Effect, Fiber, Layer, Option, Queue, Ref, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  deriveGroup,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CatchupClient } from "../src/client/catchup-client.js"
import { SyncJournal } from "../src/client/sync-journal.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { defineCollection } from "../src/define-collection.js"
import { makeLiveRuntime } from "../src/runtime/live-runtime.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String })
type Webhook = typeof Webhook.Type
const modelName = ModelName.make("Webhook")
const group = deriveGroup(["organization", "org-1"])
const sid = (value: string) => SyncId.make(value)
const key = (value: string) => ModelId.make(value)
const epoch = DateTime.makeUnsafe(0).pipe(DateTime.toDateUtc)

const event = (syncId: string, data: unknown, id = `w-${syncId}`): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId: sid(syncId),
  modelName,
  modelId: key(id),
  syncGroups: [group],
  createdAt: epoch,
  data,
})

const waitUntil = (condition: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() =>
    condition() ? Effect.void : Effect.sleep(Duration.millis(5)).pipe(Effect.andThen(waitUntil(condition))),
  ).pipe(
    Effect.timeoutOrElse({ duration: Duration.seconds(2), orElse: () => Effect.die("condition not met") }),
  )

const waitUntilEffect = (condition: Effect.Effect<boolean>): Effect.Effect<void> =>
  condition.pipe(
    Effect.flatMap((ready) =>
      ready ? Effect.void : Effect.sleep(Duration.millis(5)).pipe(Effect.andThen(waitUntilEffect(condition))),
    ),
    Effect.timeoutOrElse({ duration: Duration.seconds(2), orElse: () => Effect.die("condition not met") }),
  )

const withRuntime = <A>(
  use: (args: {
    readonly runtime: ReturnType<typeof makeLiveRuntime>
    readonly events: Queue.Queue<HydratedSyncEventEnvelope>
  }) => Effect.Effect<A>,
): Effect.Effect<A> =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
    const sync = Layer.mergeAll(
      CatchupClient.layerMemory({ events: [], lastSyncId: sid("0"), epoch: Option.none() }),
      SyncTransport.layerMemory(events),
      SyncJournal.layerMemory,
    )
    const runtime = makeLiveRuntime({ persistence: makeNodeSqlitePersistence(), sync })
    const fiber = runtime.forkSync()
    return yield* use({ runtime, events }).pipe(
      Effect.ensuring(
        Fiber.interrupt(fiber).pipe(
          Effect.andThen(Effect.sync(() => runtime.dispose())),
          Effect.asVoid,
        ),
      ),
    )
  })

describe("defineCollection broker drain", () => {
  it.live("snapshots, scope-filters, skips malformed rows, applies live rows, and fans deletes", () =>
    withRuntime(({ runtime, events }) =>
      Effect.gen(function* () {
        const webhooks = defineCollection({
          runtime,
          entity: "Webhook",
          schema: Webhook,
          getKey: (row) => key(row.id),
          scopeOf: (row) => row.orgId,
          listFn: (scope) => Effect.succeed([{ id: "seed", orgId: scope }]),
        })
        const collection = webhooks("org-1")
        yield* Effect.promise(() => collection.preload())
        yield* waitUntil(() => collection.has(key("seed")))

        yield* Queue.offer(events, event("1", { id: "other", orgId: "org-2" }, "other"))
        yield* Queue.offer(events, event("2", { id: "mine", orgId: "org-1" }, "mine"))
        yield* waitUntil(() => collection.has(key("mine")))
        assert.isFalse(collection.has(key("other")))

        yield* Queue.offer(events, event("3", { id: 123, orgId: false }, "bad"))
        yield* Queue.offer(events, event("4", { id: "after-bad", orgId: "org-1" }, "after-bad"))
        yield* waitUntil(() => collection.has(key("after-bad")))
        assert.isFalse(collection.has(key("bad")))

        yield* Queue.offer(events, {
          _tag: "Delete",
          syncId: sid("5"),
          modelName,
          modelId: key("mine"),
          syncGroups: [group],
          createdAt: epoch,
        })
        yield* waitUntil(() => !collection.has(key("mine")))
      }),
    ))

  it.live("decodes non-JSON-native fields through the canonical JSON codec — Date arrives as an ISO string", () =>
    withRuntime(({ runtime, events }) =>
      Effect.gen(function* () {
        // Schema.Date's plain encoded form is still a Date instance; on the wire it is
        // an ISO string (canonical JSON). The drain must decode that string, not skip
        // the event as undecodable.
        const Stamped = Schema.Struct({ id: Schema.String, orgId: Schema.String, createdAt: Schema.Date })
        const stamps = defineCollection({
          runtime,
          entity: "Webhook",
          schema: Stamped,
          getKey: (row) => key(row.id),
          scopeOf: (row) => row.orgId,
          listFn: (scope) => Effect.succeed([{ id: "seed", orgId: scope, createdAt: new Date(0) }]),
        })
        const collection = stamps("org-1")
        yield* Effect.promise(() => collection.preload())
        yield* waitUntil(() => collection.has(key("seed")))

        yield* Queue.offer(
          events,
          event("1", { id: "dated", orgId: "org-1", createdAt: "2026-07-23T12:12:08.434Z" }, "dated"),
        )
        yield* waitUntil(() => collection.has(key("dated")))
        const row = collection.get(key("dated"))!
        assert.instanceOf(row.createdAt, Date)
        assert.strictEqual(row.createdAt.toISOString(), "2026-07-23T12:12:08.434Z")
      }),
    ))

  it.live("disposeScope interrupts a drain blocked in listFn", () =>
    withRuntime(({ runtime }) =>
      Effect.gen(function* () {
        const entered = yield* Ref.make(false)
        const webhooks = defineCollection({
          runtime,
          entity: "Webhook",
          schema: Webhook,
          getKey: (row) => key(row.id),
          scopeOf: (row) => row.orgId,
          listFn: () => Ref.set(entered, true).pipe(Effect.andThen(Effect.never)),
        })
        webhooks("org-1")
        yield* waitUntilEffect(Ref.get(entered))
        yield* runtime.registry.disposeScope("org-1").pipe(
          Effect.timeoutOrElse({
            duration: Duration.seconds(1),
            orElse: () => Effect.die("drain was not interrupted"),
          }),
        )
      }),
    ))
})
