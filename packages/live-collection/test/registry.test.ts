import { Context, Effect, Exit, Layer, Scope } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { CollectionRegistry } from "../src/registry/collection-registry.js"
import { globalKey, scopedKey } from "../src/core/collection-key.js"

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
  it.effect("getOrCreate builds once and returns the canonical instance", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const t = tracked("user", [])
      const key = globalKey<{ name: string }>("user")
      const first = yield* registry.getOrCreate({ key, make: t.make })
      const second = yield* registry.getOrCreate({ key, make: t.make })
      assert.strictEqual(first, second)
      assert.strictEqual(t.creations(), 1)
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.effect("dispose tears down one collection exactly once and leaves siblings alive", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const log: Array<string> = []
      const a = scopedKey<{ name: string }>({ entity: "webhook", scope: "org-1" })
      const b = scopedKey<{ name: string }>({ entity: "webhook", scope: "org-2" })
      yield* registry.getOrCreate({ key: a, make: tracked("a", log).make })
      yield* registry.getOrCreate({ key: b, make: tracked("b", log).make })
      yield* registry.dispose(a)
      yield* registry.dispose(a)
      assert.deepStrictEqual(log, ["a"])
      yield* registry.disposeAll
      assert.deepStrictEqual(log, ["a", "b"])
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.effect("disposeScope tears down that scope and leaves globals and other scopes", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const log: Array<string> = []
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-1" }), make: tracked("wh1", log).make })
      yield* registry.getOrCreate({ key: scopedKey({ entity: "member", scope: "org-1" }), make: tracked("m1", log).make })
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-2" }), make: tracked("wh2", log).make })
      yield* registry.getOrCreate({ key: globalKey("user"), make: tracked("user", log).make })
      yield* registry.disposeScope("org-1")
      assert.deepStrictEqual([...log].sort(), ["m1", "wh1"])
      yield* registry.disposeAll
      assert.deepStrictEqual([...log].sort(), ["m1", "user", "wh1", "wh2"])
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.effect("disposeAllScoped tears down every scoped collection and leaves globals", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const log: Array<string> = []
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-1" }), make: tracked("wh1", log).make })
      yield* registry.getOrCreate({ key: scopedKey({ entity: "member", scope: "org-2" }), make: tracked("m2", log).make })
      yield* registry.getOrCreate({ key: globalKey("user"), make: tracked("user", log).make })
      yield* registry.disposeAllScoped
      assert.deepStrictEqual([...log].sort(), ["m2", "wh1"])
      yield* registry.disposeAll
      assert.deepStrictEqual([...log].sort(), ["m2", "user", "wh1"])
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.effect("disposeAll tears down every collection, globals included", () =>
    Effect.gen(function* () {
      const registry = yield* CollectionRegistry
      const log: Array<string> = []
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-1" }), make: tracked("wh1", log).make })
      yield* registry.getOrCreate({ key: globalKey("user"), make: tracked("user", log).make })
      yield* registry.disposeAll
      assert.deepStrictEqual([...log].sort(), ["user", "wh1"])
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.effect("releasing the layer disposes every surviving collection", () =>
    Effect.gen(function* () {
      const log: Array<string> = []
      const scope = yield* Scope.make()
      const context = yield* Layer.build(CollectionRegistry.layer).pipe(Scope.provide(scope))
      const registry = Context.get(context, CollectionRegistry)
      yield* registry.getOrCreate({ key: globalKey("user"), make: tracked("user", log).make })
      yield* registry.getOrCreate({ key: scopedKey({ entity: "webhook", scope: "org-1" }), make: tracked("wh1", log).make })
      assert.deepStrictEqual(log, [])
      yield* Scope.close(scope, Exit.void)
      assert.deepStrictEqual([...log].sort(), ["user", "wh1"])
    }))
})
