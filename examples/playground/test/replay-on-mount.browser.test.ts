import { Duration, Effect, Fiber, Layer, Option, Queue } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  CatchupClient,
  defineCollection,
  EventLogStore,
  LastSyncIdStore,
  makeLiveRuntime,
  type ScopedHandle,
  scopedKey,
  SyncTransport,
} from "@triargos/live-collection"
import {
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncGroup,
  SyncId,
} from "@triargos/live-collection-protocol"
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"
import { Webhook, webhookKey } from "../src/live/schema.js"

const LAB = "replay-lab"
const MODEL = ModelName.make("Webhook")
const GROUP = SyncGroup.make("playground")
const labKey = scopedKey<unknown>({ entity: "Webhook", scope: LAB })
const rowCount = (collection: { readonly keys: () => Iterable<unknown> }): number => Array.from(collection.keys()).length

const waitUntil = (condition: () => boolean, label: string): Effect.Effect<void> => {
  const attempt: Effect.Effect<void> = Effect.suspend(() =>
    condition() ? Effect.void : Effect.sleep(Duration.millis(10)).pipe(Effect.andThen(attempt)),
  )
  return attempt.pipe(
    Effect.timeoutOrElse({ duration: Duration.seconds(8), orElse: () => Effect.fail(new Error(`timeout: ${label}`) ) }),
    Effect.orDie,
  )
}

interface Ctx {
  readonly webhooks: ScopedHandle<Webhook>
  readonly listCalls: () => number
  readonly queue: Queue.Queue<HydratedSyncEventEnvelope>
  readonly disposeScope: () => Effect.Effect<void>
}

const run = (body: (context: Ctx) => Effect.Effect<void>): Effect.Effect<void> =>
  Effect.gen(function* () {
    let listCalls = 0
    const databaseName = `eventlog-${crypto.randomUUID()}`
    const database = yield* Effect.promise(() =>
      openBrowserWASQLiteOPFSDatabase({ databaseName: `replay-${crypto.randomUUID()}.sqlite` }),
    )
    const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
    const sync = Layer.mergeAll(
      LastSyncIdStore.layerMemory,
      CatchupClient.layerMemory({ events: [], lastSyncId: SyncId.make("0"), epoch: Option.none() }),
      SyncTransport.layerMemory(queue),
      EventLogStore.layer({ databaseName }),
    )
    const runtime = makeLiveRuntime({
      persistence: createBrowserWASQLitePersistence({ database }),
      sync,
    })
    const webhooks = defineCollection({
      runtime,
      entity: "Webhook",
      schema: Webhook,
      getKey: webhookKey,
      scopeOf: (row) => row.orgId,
      listFn: () => Effect.sync(() => (listCalls += 1)).pipe(Effect.as([] as ReadonlyArray<Webhook>)),
    })
    const fiber = runtime.forkSync()
    yield* body({
      webhooks,
      listCalls: () => listCalls,
      queue,
      disposeScope: () => runtime.registry.dispose(labKey),
    }).pipe(
      Effect.ensuring(
        Fiber.interrupt(fiber).pipe(
          Effect.andThen(Effect.sync(() => runtime.dispose())),
          Effect.asVoid,
        ),
      ),
    )
  })

describe("replay-on-mount (OPFS + IndexedDB, browser)", () => {
  it.live("a scope mounted after its events streamed past heals from the local log — no further listFn", () =>
    run(({ webhooks, listCalls, queue, disposeScope }) =>
      Effect.gen(function* () {
        let sequence = 0
        const seed = (): Effect.Effect<void> => {
          sequence += 1
          const row: Webhook = {
            id: crypto.randomUUID(),
            orgId: LAB,
            url: `https://lab.example/hook-${sequence}`,
          }
          return Queue.offer(queue, {
            _tag: "Insert",
            syncId: SyncId.make(String(sequence)),
            modelName: MODEL,
            modelId: ModelId.make(row.id),
            syncGroups: [GROUP],
            createdAt: new Date(0),
            data: row,
          }).pipe(Effect.asVoid)
        }

        const first = webhooks(LAB)
        yield* Effect.promise(() => first.preload())
        yield* waitUntil(() => listCalls() === 1, "cold snapshot")
        yield* Effect.sleep(Duration.millis(100)) // let the persistence wrapper finish its ready transition before cleanup
        yield* disposeScope()
        yield* seed()
        yield* seed()
        yield* seed()

        const second = webhooks(LAB)
        yield* Effect.promise(() => second.preload())
        yield* waitUntil(() => rowCount(second) >= 3, "remount replay")
        yield* Effect.sleep(Duration.millis(100)) // do not tear down while persisted startup is still marking ready
        assert.strictEqual(rowCount(second), 3)
        assert.strictEqual(listCalls(), 1)
      }),
    ))
})
