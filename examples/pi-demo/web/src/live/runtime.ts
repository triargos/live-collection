import type { HttpClient } from "effect/http"
import {
  CatchupClient,
  HydrateClient,
  SyncJournal,
  type LiveRuntime,
  makeLiveRuntime,
  SyncTransport,
} from "@triargos/live-collection"
import {
  BrowserCollectionCoordinator,
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"
import { Layer } from "effect"

export const createRuntime = async (
  httpClient: Layer.Layer<HttpClient.HttpClient>,
): Promise<LiveRuntime> => {
  const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "pi-demo" })
  // Tabs share one OPFS database; the coordinator elects one writer tab (Web Locks) and fans
  // commits out to the others (BroadcastChannel).
  const persistence = createBrowserWASQLitePersistence({
    database,
    coordinator: new BrowserCollectionCoordinator({ dbName: "pi-demo" }),
  })
  const sync = Layer.mergeAll(
    SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" }),
    CatchupClient.layer({ url: "/api/catchup" }),
    HydrateClient.layer({ url: "/api/sync/batch" }), // partial-index subset fetches
    SyncJournal.layer({ databaseName: "pi-demo-eventlog" }),
  ).pipe(Layer.provide(httpClient))

  return makeLiveRuntime({ persistence, sync })
}
