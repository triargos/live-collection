import { Context, Duration, Effect, Exit, Layer, ManagedRuntime, Option, Schema, Scope } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelId } from "@triargos/live-collection-protocol"
import { makeRegistry } from "../src/registry/collection-registry.js"
import { defineCollection } from "../src/registry/define-collection.js"
import type { LiveRuntime } from "../src/runtime/live-runtime.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

// The A.10 optimistic write path over the REAL composition (registry + persistence + native mutation
// handlers). The collection's `onInsert` is an Effect run on the app-provided `services` runtime; it
// reconciles by calling `collection.utils.writeSynced(confirmed)` (Model B) BEFORE resolving, so the
// synced store holds the row when TanStack drops the completed optimistic transaction — no flicker.
const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String, url: Schema.String })
type Webhook = typeof Webhook.Type
const k = (s: string) => ModelId.make(s)

// A fake server API as an Effect service — the thing `services` discharges (the app's `R`).
interface FakeApiShape {
  readonly createWebhook: (w: Webhook) => Effect.Effect<Webhook>
  readonly deleteWebhook: (id: ModelId) => Effect.Effect<void>
  readonly list: Effect.Effect<ReadonlyArray<Webhook>>
}
class FakeApi extends Context.Tag("FakeApi")<FakeApi, FakeApiShape>() {}

const fakeApiLayer = (log: Array<string>, rows: ReadonlyArray<Webhook> = []): Layer.Layer<FakeApi> =>
  Layer.succeed(FakeApi, {
    createWebhook: (w) => Effect.sync(() => (log.push(`create:${w.id}`), w)), // echoes back (keeps the client id)
    deleteWebhook: (id) => Effect.sync(() => void log.push(`delete:${id}`)),
    list: Effect.succeed(rows),
  })

const waitUntil = (cond: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() => (cond() ? Effect.void : Effect.sleep(Duration.millis(5)).pipe(Effect.zipRight(waitUntil(cond))))).pipe(
    Effect.timeoutFail({ duration: Duration.seconds(2), onTimeout: () => new Error("condition not met") }),
    Effect.orDie,
  )

/** Real registry + node-sqlite persistence + a `services` runtime over {@link fakeApiLayer}. */
const setup = (rows: ReadonlyArray<Webhook> = []) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const registry = yield* Scope.extend(makeRegistry, scope)
    const runtime = { registry, persistence: makeNodeSqlitePersistence() } as unknown as LiveRuntime
    const log: Array<string> = []
    const services = ManagedRuntime.make(fakeApiLayer(log, rows))
    const teardown = Effect.promise(() => services.dispose()).pipe(Effect.zipRight(Scope.close(scope, Exit.void)))
    return { runtime, services, log, teardown }
  })

describe("write path — optimistic mutations", () => {
  it.live("an optimistic insert: onInsert runs on `services` and writeSynced confirms the row", () =>
    Effect.gen(function* () {
      const { runtime, services, log, teardown } = yield* setup()
      const webhooks = defineCollection({
        runtime,
        services,
        entity: "Webhook",
        schema: Webhook,
        getKey: (w) => k(w.id),
        scopeOf: (w) => w.orgId,
        listFn: () => Effect.succeed<ReadonlyArray<Webhook>>([]),
        onInsert: ({ transaction, collection }) =>
          Effect.gen(function* () {
            const api = yield* FakeApi
            const created = yield* api.createWebhook(transaction.mutations[0]!.modified)
            yield* collection.utils.writeSynced(created)
          }),
      })

      const coll = webhooks("org-1")
      yield* Effect.promise(() => coll.preload())
      coll.insert({ id: "w1", orgId: "org-1", url: "https://example.com/hook" })

      yield* waitUntil(() => coll.has(k("w1")))
      assert.isTrue(coll.has(k("w1"))) // confirmed via the synced-store reconcile
      assert.deepStrictEqual(log, ["create:w1"]) // the handler ran on the services runtime
      yield* teardown
    }))

  it.live("a failed handler rolls the optimistic insert back", () =>
    Effect.gen(function* () {
      const { runtime, services, teardown } = yield* setup()
      const webhooks = defineCollection({
        runtime,
        services,
        entity: "Webhook",
        schema: Webhook,
        getKey: (w) => k(w.id),
        scopeOf: (w) => w.orgId,
        listFn: () => Effect.succeed<ReadonlyArray<Webhook>>([]),
        onInsert: () => Effect.fail("server rejected"), // models a server failure
      })

      const coll = webhooks("org-1")
      yield* Effect.promise(() => coll.preload())
      coll.insert({ id: "w2", orgId: "org-1", url: "https://example.com/hook" })
      assert.isTrue(coll.has(k("w2"))) // optimistic row visible immediately

      yield* waitUntil(() => !coll.has(k("w2")))
      assert.isFalse(coll.has(k("w2"))) // rolled back once the handler rejected
      yield* teardown
    }))

  it.live("onDelete: deleteSynced removes the row durably", () =>
    Effect.gen(function* () {
      const { runtime, services, log, teardown } = yield* setup()
      const webhooks = defineCollection({
        runtime,
        services,
        entity: "Webhook",
        schema: Webhook,
        getKey: (w) => k(w.id),
        scopeOf: (w) => w.orgId,
        listFn: () => Effect.succeed<ReadonlyArray<Webhook>>([]),
        onInsert: ({ transaction, collection }) =>
          Effect.gen(function* () {
            const api = yield* FakeApi
            yield* collection.utils.writeSynced(yield* api.createWebhook(transaction.mutations[0]!.modified))
          }),
        onDelete: ({ transaction, collection }) =>
          Effect.gen(function* () {
            const api = yield* FakeApi
            const id = transaction.mutations[0]!.key
            yield* api.deleteWebhook(id)
            yield* collection.utils.deleteSynced(id)
          }),
      })

      const coll = webhooks("org-1")
      yield* Effect.promise(() => coll.preload())
      coll.insert({ id: "w3", orgId: "org-1", url: "https://example.com/hook" })
      yield* waitUntil(() => coll.has(k("w3")))

      coll.delete(k("w3"))
      yield* waitUntil(() => !coll.has(k("w3")))
      assert.isFalse(coll.has(k("w3")))
      assert.deepStrictEqual(log, ["create:w3", "delete:w3"])
      yield* teardown
    }))

  it.effect("listFn carries R: the `services` runtime discharges it for the snapshot", () =>
    Effect.gen(function* () {
      const log: Array<string> = []
      const services = ManagedRuntime.make(fakeApiLayer(log, [{ id: "s1", orgId: "org-1", url: "u" }]))
      const webhooks = defineCollection({
        runtime: {} as unknown as LiveRuntime, // _meta.listFn never touches the runtime
        services,
        entity: "Webhook",
        schema: Webhook,
        getKey: (w) => k(w.id),
        scopeOf: (w) => w.orgId,
        listFn: () =>
          Effect.gen(function* () {
            const api = yield* FakeApi // requires R = FakeApi
            return yield* api.list
          }),
      })

      // _meta.listFn is already bridged to R = never (services context provided at define time).
      const out = yield* webhooks._meta.listFn(Option.some("org-1"))
      assert.deepStrictEqual(out, [{ id: "s1", orgId: "org-1", url: "u" }])
      yield* Effect.promise(() => services.dispose())
    }))
})
