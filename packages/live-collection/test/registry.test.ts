import { Context, Effect, Exit, Layer, Option, Scope } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { CollectionRegistry } from "../src/registry/collection-registry.js"
import { globalKey, scopedKey } from "../src/registry/collection-key.js"

// A fake collection: a fresh object per build (so reference identity proves caching), a
// counter to prove `make` ran once, and a finalizer that records its teardown into `log`.
const tracked = (name: string, log: Array<string>) => {
  let creations = 0
  const make = Effect.gen(function* () {
    creations += 1
    yield* Effect.addFinalizer(() => Effect.sync(() => log.push(name)))
    return { name }
  })
  return { make, creations: () => creations }
}

describe("CollectionRegistry", () => {
  it.scoped("getOrCreate builds once and returns the canonical instance", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const t = tracked("user", [])
      const key = globalKey<{ name: string }>("user")

      const first = yield* registry.getOrCreate({ key, make: t.make })
      const second = yield* registry.getOrCreate({ key, make: t.make })

      assert.strictEqual(first, second) // same object, not just equal
      assert.strictEqual(t.creations(), 1) // make ran only on the miss
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.scoped("getById is None before create, Some after, None after dispose", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const key = scopedKey<{ name: string }>({ entity: "webhook", scope: "org-1" })

      assert.isTrue(Option.isNone(yield* registry.getById(key)))
      const created = yield* registry.getOrCreate({ key, make: tracked("webhook", []).make })
      assert.deepStrictEqual(yield* registry.getById(key), Option.some(created))
      yield* registry.dispose(key)
      assert.isTrue(Option.isNone(yield* registry.getById(key)))
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.scoped("dispose tears down one collection exactly once and leaves siblings", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const log: Array<string> = []
      const a = scopedKey<{ name: string }>({ entity: "webhook", scope: "org-1" })
      const b = scopedKey<{ name: string }>({ entity: "webhook", scope: "org-2" })
      yield* registry.getOrCreate({ key: a, make: tracked("a", log).make })
      const keptB = yield* registry.getOrCreate({ key: b, make: tracked("b", log).make })

      yield* registry.dispose(a)
      assert.deepStrictEqual(log, ["a"]) // a's finalizer ran
      assert.deepStrictEqual(yield* registry.getById(b), Option.some(keptB)) // sibling intact

      yield* registry.dispose(a) // already evicted ⇒ no-op, finalizer not run again
      assert.deepStrictEqual(log, ["a"])
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.scoped("disposeScope tears down a scope and leaves globals and other scopes", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const log: Array<string> = []
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-1" }), make: tracked("wh1", log).make })
      yield* registry.getOrCreate({ key: scopedKey({ entity: "member", scope: "org-1" }), make: tracked("m1", log).make })
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-2" }), make: tracked("wh2", log).make })
      yield* registry.getOrCreate({ key: globalKey("user"), make: tracked("user", log).make })

      yield* registry.disposeScope("org-1")

      assert.deepStrictEqual([...log].sort(), ["m1", "wh1"]) // both org-1 collections only
      assert.isTrue(Option.isSome(yield* registry.getById(scopedKey({ entity: "webhook", scope: "org-2" }))))
      assert.isTrue(Option.isSome(yield* registry.getById(globalKey("user"))))
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.scoped("getByEntity returns every mounted collection for an entity across scopes", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const wh1 = yield* registry.getOrCreate({ key: scopedKey({ entity: "Webhook", scope: "org-1" }), make: tracked("wh1", []).make })
      const wh2 = yield* registry.getOrCreate({ key: scopedKey({ entity: "Webhook", scope: "org-2" }), make: tracked("wh2", []).make })
      yield* registry.getOrCreate({ key: globalKey("User"), make: tracked("user", []).make })

      const webhooks = yield* registry.getByEntity("Webhook")
      assert.deepStrictEqual(webhooks.map((e) => e.collection), [wh1, wh2]) // both Webhook scopes, User excluded
      assert.deepStrictEqual(
        webhooks.map((e) => Option.getOrNull(e.key.scope)),
        ["org-1", "org-2"], // each instance paired with its scope
      )

      assert.deepStrictEqual([...(yield* registry.getByEntity("Nope"))], []) // none mounted ⇒ empty
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.scoped("disposeAllScoped tears down every scoped collection and leaves globals", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const log: Array<string> = []
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-1" }), make: tracked("wh1", log).make })
      yield* registry.getOrCreate({ key: scopedKey({ entity: "member", scope: "org-2" }), make: tracked("m2", log).make })
      const keptUser = yield* registry.getOrCreate({ key: globalKey("user"), make: tracked("user", log).make })

      yield* registry.disposeAllScoped()

      assert.deepStrictEqual([...log].sort(), ["m2", "wh1"]) // both scoped, regardless of which scope
      assert.deepStrictEqual(yield* registry.getById(globalKey("user")), Option.some(keptUser)) // global intact
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.scoped("disposeAll tears down every collection, globals included", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const log: Array<string> = []
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-1" }), make: tracked("wh1", log).make })
      yield* registry.getOrCreate({ key: globalKey("user"), make: tracked("user", log).make })

      yield* registry.disposeAll()

      assert.deepStrictEqual([...log].sort(), ["user", "wh1"]) // everything torn down
      assert.isTrue(Option.isNone(yield* registry.getById(globalKey("user"))))
      assert.isTrue(Option.isNone(yield* registry.getById(scopedKey({ entity: "webhook", scope: "org-1" }))))
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.effect("releasing the layer disposes every surviving collection (Scope backstop)", () =>
    Effect.gen(function* () {
      const log: Array<string> = []
      const scope = yield* Scope.make()
      const context = yield* Layer.build(CollectionRegistry.layer).pipe(Scope.extend(scope))
      const registry = Context.get(context, CollectionRegistry)

      yield* registry.getOrCreate({ key: globalKey("user"), make: tracked("user", log).make })
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-1" }), make: tracked("wh1", log).make })
      assert.deepStrictEqual(log, []) // nothing disposed yet

      yield* Scope.close(scope, Exit.void)
      assert.deepStrictEqual([...log].sort(), ["user", "wh1"]) // all survivors torn down
    }))
})
