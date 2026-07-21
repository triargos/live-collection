import { Context, Duration, Effect, Exit, Layer, ManagedRuntime, Option, Schema, Scope } from "effect"
import { assert, describe, it } from "@effect/vitest"
import type { PersistedCollectionPersistence } from "@tanstack/db-sqlite-persistence-core"
import { ModelId } from "@triargos/live-collection-protocol"
import { makeRegistry } from "../src/registry/collection-registry.js"
import { defineCollection, type ScopedHandle } from "../src/define-collection.js"
import type { LiveRuntime } from "../src/runtime/live-runtime.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

// The A.10 optimistic write path over the REAL composition (registry + persistence + native mutation
// handlers). The app's `onInsert`/`onUpdate` is an Effect run on the `services` runtime that calls the
// server and **returns the confirmed row** — it never touches `collection.utils`. The LIBRARY reconciles
// that row into the synced baseline (Model B) before resolving. `onDelete` returns `void`; the library
// removes by key. The synced baseline is durable (persists to sqlite); a bare optimistic overlay is not —
// so "reload over the same persistence and the row is still there" is the discriminating proof that the
// library reconciled it for us (mirrors the durability pattern in live-collection-options.test.ts).
const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String, url: Schema.String })
type Webhook = typeof Webhook.Type
const k = (s: string) => ModelId.make(s)
type Services = ManagedRuntime.ManagedRuntime<FakeApi, never>

// A fake server API as an Effect service — the thing `services` discharges (the app's `R`).
interface FakeApiShape {
  readonly createWebhook: (w: Webhook) => Effect.Effect<Webhook>
  readonly deleteWebhook: (id: ModelId) => Effect.Effect<void>
  readonly list: Effect.Effect<ReadonlyArray<Webhook>>
}
class FakeApi extends Context.Service<FakeApi, FakeApiShape>()("FakeApi") {}

const fakeApiLayer = (log: Array<string>, rows: ReadonlyArray<Webhook> = []): Layer.Layer<FakeApi> =>
  Layer.succeed(FakeApi, {
    createWebhook: (w) => Effect.sync(() => (log.push(`create:${w.id}`), w)), // echoes back (keeps the client id)
    deleteWebhook: (id) => Effect.sync(() => void log.push(`delete:${id}`)),
    list: Effect.succeed(rows),
  })

const waitUntil = (cond: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() => (cond() ? Effect.void : Effect.sleep(Duration.millis(5)).pipe(Effect.andThen(waitUntil(cond))))).pipe(
    Effect.timeoutOrElse({ duration: Duration.seconds(2), orElse: () => Effect.die("condition not met") }),
  )

/** Shared, reload-surviving persistence + a `services` runtime over {@link fakeApiLayer}. */
const setup = (rows: ReadonlyArray<Webhook> = []) =>
  Effect.gen(function* () {
    const persistence = makeNodeSqlitePersistence()
    const log: Array<string> = []
    const services = ManagedRuntime.make(fakeApiLayer(log, rows))
    const teardown = Effect.promise(() => services.dispose())
    return { persistence, log, services, teardown }
  })

/** The collection under test: handlers call the server and return the confirmed row (insert) / void (delete). */
const inertRuntime = (
  registry: LiveRuntime["registry"],
  persistence: PersistedCollectionPersistence,
): LiveRuntime => ({
  registry,
  persistence,
  forkDrain: () => Effect.runFork(Effect.never),
  forkSync: () => Effect.runFork(Effect.never),
  dispose: () => {},
})

const makeWebhooks = (runtime: LiveRuntime, services: Services): ScopedHandle<Webhook> =>
  defineCollection({
    runtime,
    services,
    entity: "Webhook",
    schema: Webhook,
    getKey: (w) => k(w.id),
    scopeOf: (w) => w.orgId,
    listFn: () => Effect.succeed<ReadonlyArray<Webhook>>([]),
    // App handlers: call the server, return the confirmed row / void. They NEVER touch collection.utils.
    onInsert: ({ transaction }) =>
      Effect.flatMap(FakeApi, (api) => api.createWebhook(transaction.mutations[0]!.modified)),
    onDelete: ({ transaction }) =>
      Effect.flatMap(FakeApi, (api) => api.deleteWebhook(transaction.mutations[0]!.key)),
  })

/**
 * One mount/unmount cycle (a "reload"): a **fresh registry** over the shared `persistence` in its own
 * scope, build the handle, run `use`, then dispose the scope. The registry caches by `(entity, scope)`,
 * so a fresh registry is what forces a real re-hydration from persistence.
 */
const onReload = <A>(persistence: PersistedCollectionPersistence, services: Services, use: (webhooks: ScopedHandle<Webhook>) => Effect.Effect<A>): Effect.Effect<A> =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const registry = yield* Scope.provide(makeRegistry, scope)
    const runtime = inertRuntime(registry, persistence)
    const result = yield* use(makeWebhooks(runtime, services))
    yield* Scope.close(scope, Exit.void)
    return result
  })

/**
 * Remount a fresh registry over the same persistence and read `has(key)`, polling until it equals `want`.
 * Synced writes persist fire-and-forget (the alpha exposes no durability handle), so poll rather than
 * assert on the first reload. Only a LIBRARY-reconciled (synced) row survives a reload — a bare optimistic
 * overlay does not — so this is the discriminating assertion for "did the library writeSynced for us?".
 */
const reloadUntilHas = (persistence: PersistedCollectionPersistence, services: Services, key: ModelId, want: boolean): Effect.Effect<boolean> => {
  const attempt = (): Effect.Effect<boolean> =>
    onReload(persistence, services, (webhooks) =>
      Effect.gen(function* () {
        const coll = webhooks("org-1")
        yield* Effect.promise(() => coll.preload())
        yield* Effect.sleep(Duration.millis(10)) // let hydration settle
        return coll.has(key)
      }),
    ).pipe(Effect.flatMap((has) => (has === want ? Effect.succeed(has) : Effect.sleep(Duration.millis(5)).pipe(Effect.andThen(attempt())))))
  return attempt().pipe(
    Effect.timeoutOrElse({
      duration: Duration.seconds(2),
      orElse: () => Effect.die(`reload has(${key}) never settled to ${want}`),
    }),
  )
}

describe("write path — optimistic mutations", () => {
  it.live("onInsert returns the confirmed row; the library reconciles it into the durable synced baseline", () =>
    Effect.gen(function* () {
      const { persistence, log, services, teardown } = yield* setup()

      // Write through the handler — the app handler only calls the server and returns the row.
      yield* onReload(persistence, services, (webhooks) =>
        Effect.gen(function* () {
          const coll = webhooks("org-1")
          yield* Effect.promise(() => coll.preload())
          const tx = coll.insert({ id: "w1", orgId: "org-1", url: "https://example.com/hook" })
          yield* Effect.promise(() => tx.isPersisted.promise)
          yield* Effect.sleep(Duration.millis(20))
        }),
      )
      assert.deepStrictEqual(log, ["create:w1"]) // handler ran on the services runtime

      // Reload: only a library-reconciled (synced) row survives. A bare optimistic write would be gone.
      assert.isTrue(yield* reloadUntilHas(persistence, services, k("w1"), true))
      yield* teardown
    }))

  it.live("a failed handler rolls the optimistic insert back", () =>
    Effect.gen(function* () {
      const { persistence, services, teardown } = yield* setup()
      const scope = yield* Scope.make()
      const registry = yield* Scope.provide(makeRegistry, scope)
      const runtime = inertRuntime(registry, persistence)
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
      yield* Scope.close(scope, Exit.void)
      yield* teardown
    }))

  it.live("onDelete returns void; the library removes the row from the durable synced baseline by key", () =>
    Effect.gen(function* () {
      const { persistence, log, services, teardown } = yield* setup()

      yield* onReload(persistence, services, (webhooks) =>
        Effect.gen(function* () {
          const coll = webhooks("org-1")
          yield* Effect.promise(() => coll.preload())
          const tx = coll.insert({ id: "w3", orgId: "org-1", url: "https://example.com/hook" })
          yield* Effect.promise(() => tx.isPersisted.promise)
          yield* Effect.sleep(Duration.millis(20))
        }),
      )
      assert.isTrue(yield* reloadUntilHas(persistence, services, k("w3"), true)) // reconciled + durable

      yield* onReload(persistence, services, (webhooks) =>
        Effect.gen(function* () {
          const coll = webhooks("org-1")
          yield* Effect.promise(() => coll.preload())
          yield* waitUntil(() => coll.has(k("w3"))) // hydrated from the durable baseline
          const tx = coll.delete(k("w3"))
          yield* Effect.promise(() => tx.isPersisted.promise)
          yield* Effect.sleep(Duration.millis(20))
        }),
      )
      assert.isFalse(yield* reloadUntilHas(persistence, services, k("w3"), false)) // library removed it durably
      assert.deepStrictEqual(log, ["create:w3", "delete:w3"])
      yield* teardown
    }))

  it.live("a batched insert (two mutations in one tx) dies loudly instead of silently dropping rows", () =>
    Effect.gen(function* () {
      const { persistence, log, services, teardown } = yield* setup()
      const scope = yield* Scope.make()
      const registry = yield* Scope.provide(makeRegistry, scope)
      const runtime = inertRuntime(registry, persistence)
      const webhooks = makeWebhooks(runtime, services)

      const coll = webhooks("org-1")
      yield* Effect.promise(() => coll.preload())
      const tx = coll.insert([
        { id: "b1", orgId: "org-1", url: "https://example.com/1" },
        { id: "b2", orgId: "org-1", url: "https://example.com/2" },
      ])

      // The library reconciles exactly mutations[0]'s confirmed row, so a batch would silently lose
      // rows 2..n when the optimistic tx drops. The guard must reject the whole transaction…
      const exit = yield* Effect.exit(Effect.tryPromise(() => tx.isPersisted.promise))
      assert.isTrue(Exit.isFailure(exit))
      // …BEFORE the handler runs (no server call with an unreconcilable batch)…
      assert.deepStrictEqual(log, [])
      // …and TanStack rolls the optimistic rows back.
      yield* waitUntil(() => !coll.has(k("b1")) && !coll.has(k("b2")))
      yield* Scope.close(scope, Exit.void)
      yield* teardown
    }))

  it.live("an async-constructing services layer works: handlers run ON the runtime, not a frozen capture", () =>
    Effect.gen(function* () {
      const persistence = makeNodeSqlitePersistence()
      const log: Array<string> = []
      // One async step in layer construction (a config fetch, a token load, an IDB open…). The
      // ManagedRuntime builds this lazily on first run; defineCollection must not force it
      // synchronously at define time.
      const services: Services = ManagedRuntime.make(
        Layer.effect(
          FakeApi,
          Effect.promise(() =>
            Promise.resolve<FakeApiShape>({
              createWebhook: (w) => Effect.sync(() => (log.push(`create:${w.id}`), w)),
              deleteWebhook: (id) => Effect.sync(() => void log.push(`delete:${id}`)),
              list: Effect.succeed([{ id: "s1", orgId: "org-1", url: "u" }]),
            }),
          ),
        ),
      )
      const scope = yield* Scope.make()
      const registry = yield* Scope.provide(makeRegistry, scope)
      const runtime = inertRuntime(registry, persistence)
      const webhooks = defineCollection({
        runtime,
        services,
        entity: "Webhook",
        schema: Webhook,
        getKey: (w) => k(w.id),
        scopeOf: (w) => w.orgId,
        listFn: () => Effect.flatMap(FakeApi, (api) => api.list),
        onInsert: ({ transaction }) =>
          Effect.flatMap(FakeApi, (api) => api.createWebhook(transaction.mutations[0]!.modified)),
      })

      const coll = webhooks("org-1")
      yield* Effect.promise(() => coll.preload())
      const tx = coll.insert({ id: "w9", orgId: "org-1", url: "https://example.com/hook" })
      yield* Effect.promise(() => tx.isPersisted.promise)
      assert.isTrue(coll.has(k("w9"))) // confirmed + reconciled through the lazily-built runtime
      assert.deepStrictEqual(log, ["create:w9"])
      // the listFn bridge reaches the same runtime, still as an Effect (loop-side seam)
      const rows = yield* webhooks._meta.listFn(Option.some("org-1"))
      assert.deepStrictEqual(rows, [{ id: "s1", orgId: "org-1", url: "u" }])
      yield* Scope.close(scope, Exit.void)
      yield* Effect.promise(() => services.dispose())
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
