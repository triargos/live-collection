import { Effect, Layer, Option, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelId, ModelName, SyncGroup, SyncId } from "@triargos/live-collection-protocol"
import { CollectionRegistry } from "../src/registry/collection-registry.js"
import { defineCollection } from "../src/registry/define-collection.js"
import { effectCollectionOptions } from "../src/persistence/effect-collection.js"
import { PersistenceBase } from "../src/persistence/persistence-base.js"
import { SyncDispatcher, dispatchEntry, type EntityEvent } from "../src/dispatch/sync-dispatcher.js"
import { makeNodeSqliteDriver } from "./node-sqlite-driver.js"

// The real seam under test: an event handed to the dispatcher must land in a collection built by
// the production factory. The dispatcher resolves the mounted instance via the registry and writes
// through its `.utils` synced-write path — nothing here fakes the collection.
const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String })
type Webhook = typeof Webhook.Type
const k = (s: string) => ModelId.make(s)

// `make` receives the `collectionId` defineCollection derived from the key (DEC-A3) — the app never
// hand-builds it — and threads it straight into the factory.
const webhookCollection = defineCollection({
  entity: "Webhook",
  scopeOf: (orgId: string) => orgId,
  make: ({ collectionId }) =>
    effectCollectionOptions({ collectionId, schema: Webhook, getKey: (w: Webhook) => k(w.id) }),
})

const DispatcherLive = SyncDispatcher.fromEntries({
  Webhook: dispatchEntry({
    schema: Webhook,
    collection: webhookCollection,
    scopeOf: (w: Webhook) => w.orgId,
  }),
})

const env = Layer.mergeAll(
  CollectionRegistry.layer,
  DispatcherLive,
  PersistenceBase.layerSqliteDriver(makeNodeSqliteDriver()),
)

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

describe("SyncDispatcher → effectCollectionOptions (end-to-end)", () => {
  it.live("an Insert lands in the real factory collection via the synced-write path", () =>
    Effect.gen(function* () {
      const dispatcher = yield* SyncDispatcher
      const coll = yield* webhookCollection("org-1")
      yield* Effect.promise(() => coll.preload()) // session captured + hydration done

      yield* dispatcher.dispatch(insert({ id: "wh_1", orgId: "org-1" }))

      assert.isTrue(coll.has(k("wh_1")))
      const row = coll.get(k("wh_1"))
      assert.strictEqual(row?.id, "wh_1")
      assert.strictEqual(row?.orgId, "org-1")
    }).pipe(Effect.scoped, Effect.provide(env)))

  it.live("a Delete fans out and removes the row from the owning factory collection", () =>
    Effect.gen(function* () {
      const dispatcher = yield* SyncDispatcher
      const coll = yield* webhookCollection("org-1")
      yield* Effect.promise(() => coll.preload())

      yield* dispatcher.dispatch(insert({ id: "wh_1", orgId: "org-1" }))
      assert.isTrue(coll.has(k("wh_1")))

      yield* dispatcher.dispatch(remove({ id: "wh_1", orgId: "org-1" }))
      assert.isFalse(coll.has(k("wh_1")))
    }).pipe(Effect.scoped, Effect.provide(env)))

  it.live("an event for an unmounted scope is ignored without error", () =>
    Effect.gen(function* () {
      const dispatcher = yield* SyncDispatcher
      const coll = yield* webhookCollection("org-1")
      yield* Effect.promise(() => coll.preload())

      yield* dispatcher.dispatch(insert({ id: "wh_9", orgId: "org-2" })) // org-2 not mounted

      assert.isFalse(coll.has(k("wh_9")))
      assert.isTrue(Option.isNone(Option.fromNullable(coll.get(k("wh_9")))))
    }).pipe(Effect.scoped, Effect.provide(env)))
})
