import { Effect, Layer } from "effect"
import {
  defineCollection,
  EventLogStore,
  type LiveRuntime,
  makeLiveRuntime,
  reloadWindow,
  type ScopedHandle,
  type SyncMap,
} from "@triargos/live-collection"
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"
import { DebugBus } from "../debug/debug-bus.js"
import { type BackendControls, makeSharedBackend, WebhookApi } from "./shared-backend.js"
import { getTabSession } from "./session.js"
import { Webhook, webhookKey } from "./schema.js"

/**
 * The assembled playground: a {@link LiveRuntime} over **real, per-tab OPFS persistence** plus the
 * cross-tab {@link makeSharedBackend} (shared localStorage log + BroadcastChannel) serving both the read
 * path (catchup/SSE/listFn) and the `WebhookApi` the optimistic handlers call. The UI uses the **native**
 * collection — `coll.insert` / `coll.delete` — and the full write path runs: optimistic row → handler
 * calls the fake server → `writeSynced` confirm → SSE echo back through the loop (idempotent) → persisted
 * to OPFS. Reload to hydrate from OPFS; open a second tab to watch writes sync across tabs.
 *
 * Everything observable hangs off here for the debug panel: the {@link DebugBus} traffic log, the backend
 * {@link BackendControls} (failure injection, resync, server reset), the registry, and the {@link SyncMap}.
 */
export interface Playground {
  readonly runtime: LiveRuntime
  readonly syncMap: SyncMap
  readonly webhooks: ScopedHandle<Webhook>
  readonly bus: DebugBus
  readonly controls: BackendControls
  readonly tabId: string
}

export const createPlayground = async (): Promise<Playground> => {
  const { tabId, dbName } = getTabSession()
  const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: dbName })
  const persistence = createBrowserWASQLitePersistence({ database })

  const bus = new DebugBus()
  const backend = makeSharedBackend({ bus, tabId })
  // EventLogStore (replay-on-mount) — in-memory for now; the durable IndexedDB adapter lands with the browser proof.
  const loop = Layer.merge(backend.loop, EventLogStore.layerMemory)
  const runtime = makeLiveRuntime({ persistence, loop, onResync: reloadWindow })

  const webhooks = defineCollection({
    runtime,
    services: backend.services,
    entity: "Webhook",
    schema: Webhook,
    getKey: webhookKey,
    scopeOf: (w) => w.orgId,
    listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)),
    onInsert: ({ transaction, collection }) =>
      Effect.gen(function* () {
        const api = yield* WebhookApi
        const created = yield* api.create(transaction.mutations[0]!.modified)
        yield* collection.utils.writeSynced(created) // Model B: confirm before resolving
      }),
    onDelete: ({ transaction, collection }) =>
      Effect.gen(function* () {
        const api = yield* WebhookApi
        const id = transaction.mutations[0]!.key
        yield* api.remove(id)
        yield* collection.utils.deleteSynced(id)
      }),
  })

  return { runtime, syncMap: { Webhook: webhooks }, webhooks, bus, controls: backend.controls, tabId }
}
