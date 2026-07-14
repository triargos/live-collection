import type { HttpClient } from "@effect/platform"
import {
  CatchupClient,
  EventLogStore,
  LastSyncIdStore,
  type LiveRuntime,
  makeLiveRuntime,
  reloadWindow,
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
  const loop = Layer.mergeAll(
    SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" }),
    CatchupClient.layer({ url: "/api/catchup" }),
    LastSyncIdStore.layer,
    EventLogStore.layer({ databaseName: "pi-demo-eventlog" }),
  ).pipe(Layer.provide(httpClient))

  return makeLiveRuntime({ persistence, loop, onResync: reloadWindow })
}
