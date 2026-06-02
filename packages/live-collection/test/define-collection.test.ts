import { Effect, Option } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { CollectionRegistry } from "../src/registry/collection-registry.js"
import { globalKey, scopedKey } from "../src/registry/collection-key.js"
import { defineCollection } from "../src/registry/define-collection.js"

// A fake collection whose `make` requires `Scope` (via addFinalizer), so these tests also
// exercise the Scope-discharge: the ref surfaces only `CollectionRegistry`, never `Scope`.
const tracked = (name: string) => {
  let creations = 0
  const make = Effect.gen(function* () {
    creations += 1
    yield* Effect.addFinalizer(() => Effect.void)
    return { name }
  })
  return { make, creations: () => creations }
}

describe("defineCollection / MountRef", () => {
  it("derives a scoped key from scopeOf", () => {
    const webhook = defineCollection({
      entity: "webhook",
      scopeOf: (orgId: string) => orgId,
      make: () => tracked("webhook").make,
    })
    assert.deepStrictEqual(
      webhook("org-1").key,
      scopedKey({ entity: "webhook", scope: "org-1" }),
    )
  })

  it("derives a global key when scopeOf is omitted", () => {
    const user = defineCollection({
      entity: "user",
      make: () => tracked("user").make,
    })
    assert.deepStrictEqual(user().key, globalKey("user"))
  })

  it.scoped("yielding a ref mounts via the registry and returns the canonical instance", () =>
    Effect.gen(function* () {
      const t = tracked("webhook")
      const webhook = defineCollection({
        entity: "webhook",
        scopeOf: (orgId: string) => orgId,
        make: () => t.make,
      })

      // Two *distinct* refs for the same key — proves identity-by-key, not by object.
      const first = yield* webhook("org-1")
      const second = yield* webhook("org-1")

      assert.strictEqual(first, second) // one canonical instance, via the registry cache
      assert.strictEqual(t.creations(), 1) // built once
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.scoped("distinct scopes mount distinct instances", () =>
    Effect.gen(function* () {
      const webhook = defineCollection({
        entity: "webhook",
        scopeOf: (orgId: string) => orgId,
        make: () => tracked("webhook").make,
      })

      const a = yield* webhook("org-1")
      const b = yield* webhook("org-2")

      assert.notStrictEqual(a, b)
    }).pipe(Effect.provide(CollectionRegistry.layer)))

  it.scoped("the ref key locates the mounted instance via registry.getById", () =>
    Effect.gen(function* () { 
      const registry = yield* CollectionRegistry
      const webhook = defineCollection({
        entity: "webhook",
        scopeOf: (orgId: string) => orgId,
        make: () => tracked("webhook").make,
      })
      const ref = webhook("org-1")

      assert.isTrue(Option.isNone(yield* registry.getById(ref.key))) // not mounted yet
      const created = yield* ref
      assert.deepStrictEqual(yield* registry.getById(ref.key), Option.some(created))
    }).pipe(Effect.provide(CollectionRegistry.layer)))
})
