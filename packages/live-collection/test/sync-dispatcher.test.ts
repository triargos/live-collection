import { Effect, Layer, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelId, ModelName, SyncGroup, SyncId } from "@triargos/live-collection-protocol"
import { CollectionRegistry } from "../src/registry/collection-registry.js"
import { defineCollection } from "../src/registry/define-collection.js"
import {
  type EntityEvent,
  SyncDispatcher,
  dispatchEntry,
} from "../src/dispatch/sync-dispatcher.js"

// A model whose entity names its own home via `orgId` (so routing never reads syncGroups).
const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String })
type Webhook = typeof Webhook.Type

// A fake collection: it records synced writes/deletes so a test can read what landed.
const makeCollection = () => {
  const rows = new Map<string, Webhook>()
  return {
    rows,
    writeSynced: (e: Webhook) => Effect.sync(() => void rows.set(e.id, e)),
    deleteSynced: (id: ModelId) => Effect.sync(() => void rows.delete(String(id))),
  }
}

const webhookCollection = defineCollection({
  entity: "Webhook",
  scopeOf: (orgId: string) => orgId,
  make: () => Effect.succeed(makeCollection()),
})

const DispatcherLive = SyncDispatcher.fromEntries({
  Webhook: dispatchEntry({
    schema: Webhook,
    collection: webhookCollection,
    scopeOf: (w) => w.orgId,
  }),
})

const env = Layer.mergeAll(CollectionRegistry.layer, DispatcherLive)

const insert = (data: Webhook): EntityEvent => ({
  _tag: "Insert",
  syncId: SyncId.make("1"),
  modelName: ModelName.make("Webhook"),
  modelId: ModelId.make(data.id),
  syncGroups: [SyncGroup.make(`organization:${data.orgId}`)],
  createdAt: new Date(0),
  data,
})

const remove = (args: { readonly id: string; readonly orgId: string }): EntityEvent => ({
  _tag: "Delete",
  syncId: SyncId.make("2"),
  modelName: ModelName.make("Webhook"),
  modelId: ModelId.make(args.id),
  syncGroups: [SyncGroup.make(`organization:${args.orgId}`)],
  createdAt: new Date(0),
})

describe("SyncDispatcher", () => {
  it.scoped("routes an Insert to the owning collection by the entity's own scope", () =>
    Effect.gen(function* () {
      const dispatcher = yield* SyncDispatcher
      const coll = yield* webhookCollection("org-1") // mount org-1

      yield* dispatcher.dispatch(insert({ id: "wh_1", orgId: "org-1" }))

      assert.deepStrictEqual([...coll.rows.entries()], [["wh_1", { id: "wh_1", orgId: "org-1" }]])
    }).pipe(Effect.provide(env)))

  it.scoped("ignores an event whose collection isn't mounted", () =>
    Effect.gen(function* () {
      const dispatcher = yield* SyncDispatcher
      const coll = yield* webhookCollection("org-1")

      yield* dispatcher.dispatch(insert({ id: "wh_9", orgId: "org-2" })) // org-2 not mounted

      assert.deepStrictEqual([...coll.rows.entries()], []) // nothing landed, no error
    }).pipe(Effect.provide(env)))

  it.scoped("skips an unknown model", () =>
    Effect.gen(function* () {
      const dispatcher = yield* SyncDispatcher
      const coll = yield* webhookCollection("org-1")

      yield* dispatcher.dispatch({
        ...insert({ id: "wh_1", orgId: "org-1" }),
        modelName: ModelName.make("Ghost"),
      })

      assert.deepStrictEqual([...coll.rows.entries()], []) // no entry for Ghost ⇒ skipped
    }).pipe(Effect.provide(env)))

  it.scoped("fans a Delete out across the model's collections, idempotently", () =>
    Effect.gen(function* () {
      const dispatcher = yield* SyncDispatcher
      const org1 = yield* webhookCollection("org-1")
      const org2 = yield* webhookCollection("org-2")
      yield* dispatcher.dispatch(insert({ id: "wh_1", orgId: "org-1" }))
      yield* dispatcher.dispatch(insert({ id: "wh_2", orgId: "org-2" }))

      yield* dispatcher.dispatch(remove({ id: "wh_1", orgId: "org-1" }))

      assert.deepStrictEqual([...org1.rows.keys()], []) // owner removed it
      assert.deepStrictEqual([...org2.rows.keys()], ["wh_2"]) // sibling untouched (no-op there)
    }).pipe(Effect.provide(env)))
})
