import { Duration, Effect, Fiber } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelId } from "@triargos/live-collection-protocol"
import { defineCollection, makeLiveRuntime } from "@triargos/live-collection"
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"
import { makeFakeBackend } from "./fake-backend.js"
import { WebhookApi } from "../src/live/shared-backend.js"
import { Webhook, webhookKey } from "../src/live/schema.js"

// The A.10 write path end-to-end in a REAL browser over REAL OPFS, against the fake backend (with fake
// delays). Proves, in one realistic flow, the things node can't: an optimistic insert confirmed by the
// library's reconcile, the SSE self-echo staying idempotent (client-minted id), a *remote* insert delivered
// over the live tail (loop alive + dispatch by scope), and all of it durably persisted across a reload.
const k = (s: string) => ModelId.make(s)
const count = (coll: { keys: () => Iterable<ModelId> }) => Array.from(coll.keys()).length

const waitUntil = (cond: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() =>
    cond() ? Effect.void : Effect.sleep(Duration.millis(10)).pipe(Effect.zipRight(waitUntil(cond))),
  ).pipe(
    Effect.timeoutFail({ duration: Duration.seconds(5), onTimeout: () => new Error("condition not met") }),
    Effect.orDie,
  )

const webhookCollection = (runtime: ReturnType<typeof makeLiveRuntime>, services: ReturnType<typeof makeFakeBackend>["services"]) =>
  defineCollection({
    runtime,
    services,
    entity: "Webhook",
    schema: Webhook,
    getKey: webhookKey,
    scopeOf: (w) => w.orgId,
    listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)),
    // Handler only calls the server and returns the confirmed row; the library reconciles (Model B).
    onInsert: ({ transaction }) =>
      Effect.flatMap(WebhookApi, (api) => api.create(transaction.mutations[0]!.modified)),
  })

describe("write path over OPFS (browser)", () => {
  it.live("optimistic insert confirms + idempotent echo + remote insert, all persisted across reload", () =>
    Effect.gen(function* () {
      const database = yield* Effect.promise(() =>
        openBrowserWASQLiteOPFSDatabase({ databaseName: `wp-${crypto.randomUUID()}.sqlite` }),
      )
      const persistence = createBrowserWASQLitePersistence({ database })

      // ── phase 1: writes with the live loop running ──
      const fake = makeFakeBackend()
      const runtime = makeLiveRuntime({ persistence, loop: fake.loop, onResync: Effect.void })
      const webhooks = webhookCollection(runtime, fake.services)
      const fiber = runtime.forkLoop([webhooks])
      const coll = webhooks("org-1")
      yield* Effect.promise(() => coll.preload())

      // optimistic insert with a CLIENT-minted id → handler returns the row, library reconciles it
      coll.insert({ id: "wh-1", orgId: "org-1", url: "https://example.com/1" })
      yield* waitUntil(() => coll.has(k("wh-1")))
      yield* Effect.sleep(Duration.millis(200)) // let the SSE self-echo flow through the loop
      assert.isTrue(coll.has(k("wh-1")))
      assert.strictEqual(count(coll), 1) // self-echo was idempotent — no duplicate row

      // a REMOTE insert (another client) arrives only over the SSE tail → loop dispatches it by scope
      yield* Effect.promise(() =>
        fake.services.runPromise(
          Effect.flatMap(WebhookApi, (api) => api.create({ id: "wh-2", orgId: "org-1", url: "https://example.com/2" })),
        ),
      )
      yield* waitUntil(() => coll.has(k("wh-2")))
      assert.strictEqual(count(coll), 2)

      yield* Fiber.interrupt(fiber)
      runtime.dispose()

      // ── phase 2: reload — fresh runtime over the SAME OPFS database, no loop ──
      const fake2 = makeFakeBackend()
      const runtime2 = makeLiveRuntime({ persistence, loop: fake2.loop, onResync: Effect.void })
      const webhooks2 = webhookCollection(runtime2, fake2.services)
      const coll2 = webhooks2("org-1")
      yield* Effect.promise(() => coll2.preload())

      yield* waitUntil(() => coll2.has(k("wh-1")) && coll2.has(k("wh-2")))
      assert.isTrue(coll2.has(k("wh-1"))) // confirmed optimistic mutation persisted
      assert.isTrue(coll2.has(k("wh-2"))) // remotely-synced row persisted
      runtime2.dispose()
    }))
})
