import type { HttpClient } from "effect/unstable/http"
import {
  CatchupClient,
  SyncJournal,
  LastSyncIdStore,
  type LiveRuntime,
  makeLiveRuntime,
  SyncTransport,
} from "@triargos/live-collection"
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"
import { Layer } from "effect"

export const createRuntime = async (
  httpClient: Layer.Layer<HttpClient.HttpClient>,
): Promise<LiveRuntime> => {
  const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "pi-demo" })
  const persistence = createBrowserWASQLitePersistence({ database })
  const sync = Layer.mergeAll(
    SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" }),
    CatchupClient.layer({ url: "/api/catchup" }),
    LastSyncIdStore.layer,
    SyncJournal.layer({ databaseName: "pi-demo-eventlog" }),
  ).pipe(Layer.provide(httpClient))

  return makeLiveRuntime({ persistence, sync })
}
