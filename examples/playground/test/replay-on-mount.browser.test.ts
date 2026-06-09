import { Duration, Effect, Exit, Fiber, Layer, Option, Queue, Scope } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  CatchupClient,
  CollectionRegistry,
  type CollectionRegistryShape,
  defineCollection,
  EventLogStore,
  LastSyncIdStore,
  type LiveRuntime,
  makeRegistry,
  type ScopedHandle,
  scopedKey,
  syncLoop,
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

// End-to-end proof of replay-on-mount over **real OPFS + IndexedDB**, driving the actual registry,
// factory, persistence, and `syncLoop` (node can run neither store). It scripts the playground demo —
// mount, unmount, seed events while unmounted, mount again — and asserts the discriminating outcome: the
// scope heals to N rows from the durable log WITHOUT a further `listFn`. A regression to "bootstrap on
// every mount" would both call `listFn` again and surface 0 rows (the seeds live only on the SSE tail,
// never in the list source). Mirrors the node integration test's harness, swapping in OPFS + the IDB layer.

const LAB = "replay-lab"
const MODEL = ModelName.make("Webhook")
const GROUP = SyncGroup.make("playground")
type Counted = { readonly keys: () => Iterable<unknown> }
const labKey = scopedKey<Counted>({ entity: "Webhook", scope: LAB })
const rowCount = (c: Counted): number => Array.from(c.keys()).length

const waitUntil = (cond: () => boolean, label: string): Effect.Effect<void> => {
  const attempt: Effect.Effect<void> = Effect.suspend(() =>
    cond() ? Effect.void : Effect.sleep(Duration.millis(10)).pipe(Effect.zipRight(attempt)),
  )
  return attempt.pipe(
    Effect.timeoutFail({ duration: Duration.seconds(8), onTimeout: () => new Error(`timeout: ${label}`) }),
    Effect.orDie,
  )
}
const waitUntilE = (cond: Effect.Effect<boolean>, label: string): Effect.Effect<void> => {
  const attempt: Effect.Effect<void> = Effect.flatMap(cond, (ok) =>
    ok ? Effect.void : Effect.sleep(Duration.millis(10)).pipe(Effect.zipRight(attempt)),
  )
  return attempt.pipe(
    Effect.timeoutFail({ duration: Duration.seconds(8), onTimeout: () => new Error(`timeout: ${label}`) }),
    Effect.orDie,
  )
}

interface Ctx {
  readonly webhooks: ScopedHandle<Webhook>
  readonly log: EventLogStore["Type"]
  readonly registry: CollectionRegistryShape
  readonly listCalls: () => number
  readonly queue: Queue.Queue<HydratedSyncEventEnvelope>
  readonly fiber: Fiber.RuntimeFiber<void>
}

/** Real registry/factory/OPFS + memory transport/catchup/cursor + the durable IDB EventLog, shared with the loop. */
const run = (body: (ctx: Ctx) => Effect.Effect<void>): Effect.Effect<void> =>
  Effect.gen(function* () {
    let listCalls = 0
    const databaseName = `eventlog-${crypto.randomUUID()}`
    const database = yield* Effect.promise(() =>
      openBrowserWASQLiteOPFSDatabase({ databaseName: `replay-${crypto.randomUUID()}.sqlite` }),
    )
    const persistence = createBrowserWASQLitePersistence({ database })

    const scope = yield* Scope.make()
    const registry = yield* Scope.extend(makeRegistry, scope)
    const runtime = { registry, persistence } as unknown as LiveRuntime
    const webhooks = defineCollection({
      runtime,
      entity: "Webhook",
      schema: Webhook,
      getKey: webhookKey,
      scopeOf: (w) => w.orgId,
      // listFn returns nothing — the seeded events reach a collection ONLY via the live/replay path.
      listFn: () => Effect.sync(() => (listCalls += 1)).pipe(Effect.as([] as ReadonlyArray<Webhook>)),
    })
    const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()

    const layers = Layer.mergeAll(
      LastSyncIdStore.layerMemory,
      CatchupClient.layerMemory({ events: [], lastSyncId: SyncId.make("0") }),
      SyncTransport.layerMemory(queue),
      EventLogStore.layer({ databaseName }),
      Layer.succeed(CollectionRegistry, registry),
    )

    yield* Effect.gen(function* () {
      const log = yield* EventLogStore
      const fiber = yield* Effect.forkScoped(syncLoop([webhooks], Effect.void))
      yield* body({ webhooks, log, registry, listCalls: () => listCalls, queue, fiber })
    }).pipe(Effect.scoped, Effect.provide(layers))

    yield* Scope.close(scope, Exit.void)
  })

describe("replay-on-mount (OPFS + IndexedDB, browser)", () => {
  it.live("a scope mounted after its events streamed past heals from the local log — no further listFn", () =>
    run(({ webhooks, log, registry, listCalls, queue }) =>
      Effect.gen(function* () {
        let seq = 0
        const seed = (): Effect.Effect<void> => {
          seq += 1
          const w: Webhook = { id: crypto.randomUUID(), orgId: LAB, url: `https://lab.example/hook-${seq}` }
          return Queue.offer(queue, {
            _tag: "Insert",
            syncId: SyncId.make(String(seq)),
            modelName: MODEL,
            modelId: ModelId.make(w.id),
            syncGroups: [GROUP],
            createdAt: new Date(0),
            data: w,
          }).pipe(Effect.asVoid)
        }

        // 1. Mount cold and let onMount settle — its base watermark gets recorded (bootstrap or catchup-skip).
        const first = webhooks(LAB)
        yield* Effect.promise(() => first.preload())
        yield* waitUntilE(log.getBaseWatermark(labKey).pipe(Effect.map(Option.isSome)), "first mount settles")
        const networkBefore = listCalls() // 0 (catchup-skip) or 1 (bootstrap) — either is a valid base

        // 2. Unmount it — the loop keeps logging events for the scope even with nothing mounted.
        yield* registry.dispose(labKey)

        // 3. Push three "remote" inserts WHILE UNMOUNTED; wait until they're durably in the local log.
        yield* seed()
        yield* seed()
        yield* seed()
        yield* waitUntilE(
          log.read({ modelName: MODEL, scope: Option.some(LAB), since: SyncId.make("0") }).pipe(
            Effect.map((rows) => rows.length >= 3),
          ),
          "seeds logged while unmounted",
        )

        // 4. Mount again → it must fill from the durable log (Replay), NOT refetch (Bootstrap).
        const second = webhooks(LAB)
        yield* Effect.promise(() => second.preload())
        yield* waitUntil(() => rowCount(second) >= 3, "remount replays the 3 logged events")

        assert.strictEqual(rowCount(second), 3) // healed from the durable IndexedDB log
        assert.strictEqual(listCalls(), networkBefore) // and with NO further listFn ⇒ replay, not bootstrap
      }),
    ))
})
