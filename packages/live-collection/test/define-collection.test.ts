import { Effect, Option, Schema, type Scope } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelId } from "@triargos/live-collection-protocol"
import type { CollectionRegistryShape } from "../src/registry/collection-registry.js"
import { type CollectionKey, scopedKey, serializeKey } from "../src/registry/collection-key.js"
import { defineCollection } from "../src/registry/define-collection.js"
import type { LiveRuntime } from "../src/runtime/live-runtime.js"

const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String })
type Webhook = typeof Webhook.Type
const k = (s: string) => ModelId.make(s)

// A fake registry: records the keys defineCollection mounts under and caches a stub per key, WITHOUT
// running `make`. This isolates defineCollection's own logic — key derivation (global vs scoped) and
// the registry handoff — from TanStack's `createCollection` (covered by the gate + integration tests).
const fakeRuntime = () => {
  const built = new Map<string, unknown>()
  const keys: Array<CollectionKey<unknown>> = []
  const registry: CollectionRegistryShape = {
    getOrCreate: <A, R>(args: {
      readonly key: CollectionKey<A>
      readonly make: Effect.Effect<A, never, R>
    }): Effect.Effect<A, never, Exclude<R, Scope.Scope>> =>
      Effect.sync(() => {
        const id = serializeKey(args.key)
        keys.push(args.key)
        if (!built.has(id)) built.set(id, { __stub: id })
        return built.get(id) as A
      }),
    dispose: () => Effect.void,
    disposeScope: () => Effect.void,
    disposeAllScoped: () => Effect.void,
    disposeAll: () => Effect.void,
  }
  return { runtime: { registry } as unknown as LiveRuntime, keys }
}

describe("defineCollection — runtime-bound handle", () => {
  it("a scoped handle returns the SAME instance per scope and distinct across scopes", () => {
    const { runtime, keys } = fakeRuntime()
    const webhook = defineCollection({
      runtime,
      entity: "Webhook",
      schema: Webhook,
      getKey: (w) => k(w.id),
      scopeOf: (w) => w.orgId,
      listFn: () => Effect.succeed([]),
    })

    assert.strictEqual(webhook("org-1"), webhook("org-1")) // registry-cached: one instance per scope
    assert.notStrictEqual(webhook("org-1"), webhook("org-2")) // distinct scope ⇒ distinct instance
    assert.deepStrictEqual(keys[0], scopedKey({ entity: "Webhook", scope: "org-1" })) // derives the scoped key
  })

  it("a global handle mounts under the global key (one instance app-wide)", () => {
    const { runtime, keys } = fakeRuntime()
    const user = defineCollection({
      runtime,
      entity: "User",
      schema: Webhook,
      getKey: (w) => k(w.id),
      listFn: Effect.succeed([]),
    })
    assert.strictEqual(user(), user())
    assert.deepStrictEqual(keys[0], { entity: "User", scope: Option.none() }) // global key, no scope
  })

  it("_meta carries entity, scopeOf and the snapshot listFn the loop reads", () => {
    const { runtime } = fakeRuntime()
    const scoped = defineCollection({
      runtime,
      entity: "Webhook",
      schema: Webhook,
      getKey: (w) => k(w.id),
      scopeOf: (w) => w.orgId,
      listFn: (orgId) => Effect.succeed([{ id: `seed-${orgId}`, orgId }]),
    })
    const global = defineCollection({
      runtime,
      entity: "User",
      schema: Webhook,
      getKey: (w) => k(w.id),
      listFn: Effect.succeed([{ id: "u1", orgId: "g" }]),
    })

    // scoped: scopeOf is Some and reads the scope off an entity; listFn routes the scope through.
    assert.isTrue(Option.isSome(scoped._meta.scopeOf))
    assert.strictEqual(Option.getOrThrow(scoped._meta.scopeOf)({ id: "x", orgId: "org-7" }), "org-7")
    assert.deepStrictEqual(Effect.runSync(scoped._meta.listFn(Option.some("org-7"))), [
      { id: "seed-org-7", orgId: "org-7" },
    ])

    // global: scopeOf is None; listFn ignores the (absent) scope.
    assert.isTrue(Option.isNone(global._meta.scopeOf))
    assert.deepStrictEqual(Effect.runSync(global._meta.listFn(Option.none())), [{ id: "u1", orgId: "g" }])
  })
})
