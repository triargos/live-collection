import { Effect, Layer } from "effect"
import {
  defineCollection,
  EventLogStore,
  type LiveRuntime,
  makeLiveRuntime,
  reloadWindow,
  type ScopedHandle,
  type SyncModels,
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
 * {@link BackendControls} (failure injection, resync, server reset), the registry, and the {@link SyncModels}.
 */
export interface Playground {
  readonly runtime: LiveRuntime
  readonly models: SyncModels
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
  // EventLogStore (replay-on-mount) — durable IndexedDB, per-tab so two tabs are independent clients (one
  // origin-shared default DB would clobber each other's log/watermarks). Survives reload; powers replay.
  const loop = Layer.merge(backend.loop, EventLogStore.layer({ databaseName: `${dbName}-eventlog` }))
  const runtime = makeLiveRuntime({ persistence, loop, onResync: reloadWindow })

  const webhooks = defineCollection({
    runtime,
    services: backend.services,
    entity: "Webhook",
    schema: Webhook,
    getKey: webhookKey,
    scopeOf: (w) => w.orgId,
    listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)),
    // Handlers only call the server and return the confirmed row (insert) / void (delete); the library
    // reconciles into the synced baseline before resolving (Model B). The app never touches utils.
    onInsert: ({ transaction }) =>
      Effect.flatMap(WebhookApi, (api) => api.create(transaction.mutations[0]!.modified)),
    onDelete: ({ transaction }) =>
      Effect.flatMap(WebhookApi, (api) => api.remove(transaction.mutations[0]!.key)),
  })

  return { runtime, models: [webhooks], webhooks, bus, controls: backend.controls, tabId }
}
