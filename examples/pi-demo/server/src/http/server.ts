import { createServer } from "node:http"
import path from "node:path"
import { Config, Effect, FileSystem, Layer } from "effect"
import { HttpRouter, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { NodeHttpServer, NodeServices } from "@effect/platform-node"
import { DemoApi } from "@pi-demo/shared"
import {
  SyncDispatcher,
  SyncEventBus,
  SyncEventStore,
  SyncFeed,
} from "@triargos/live-collection-server"
import { ProjectRepo } from "../repo/project-repo.js"
import { TodoRepo } from "../repo/todo-repo.js"
import { RegistryLayer } from "../sync/registry.js"
import { ProjectsApiLive, SyncApiLive, TodosApiLive } from "./api-live.js"
import { SessionAuthLive } from "./session-auth.js"
import { SseRoute } from "./sse.js"

const StorageServices = Layer.mergeAll(
  ProjectRepo.layerMemory,
  TodoRepo.layerMemory,
  SyncEventStore.layerMemory,
  SyncEventBus.layerMemory,
)

export const BackendServices = Layer.merge(
  StorageServices,
  Layer.merge(SyncDispatcher.layer, SyncFeed.layer).pipe(
    Layer.provide(RegistryLayer),
    Layer.provide(StorageServices),
  ),
)

const ApiHandlers = Layer.mergeAll(ProjectsApiLive, TodosApiLive, SyncApiLive)
const ApiRoute = HttpApiBuilder.layer(DemoApi).pipe(
  Layer.provide(ApiHandlers),
  Layer.provide(SessionAuthLive),
)

const StaticRoute = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const configured = yield* Config.string("STATIC_DIR").pipe(
      Config.withDefault("../web/dist"),
    )
    const staticDir = path.resolve(process.cwd(), configured)
    if (!(yield* fs.exists(staticDir))) {
      yield* Effect.logInfo(`Static directory not found at ${staticDir}; serving API only`)
      return Layer.empty
    }

    const indexPath = path.join(staticDir, "index.html")
    return HttpRouter.add("GET", "*", (request) =>
      Effect.gen(function* () {
        const pathname = new URL(request.url, "http://localhost").pathname
        if (pathname === "/api" || pathname.startsWith("/api/")) {
          return HttpServerResponse.empty({ status: 404 })
        }
        const requested = path.resolve(staticDir, `.${pathname}`)
        const safe = requested === staticDir || requested.startsWith(`${staticDir}${path.sep}`)
        const requestedExists = pathname !== "/" && safe && (yield* fs.exists(requested))
        const responsePath = requestedExists ? requested : indexPath
        const response = yield* HttpServerResponse.file(responsePath)
        return path.extname(responsePath) === ".js"
          ? HttpServerResponse.setHeader(response, "content-type", "text/javascript")
          : response
      }).pipe(Effect.orDie),
    )
  }),
)

const Routes = Layer.mergeAll(ApiRoute, SseRoute, StaticRoute)

const serve = (config: { readonly port: number }) =>
  HttpRouter.serve(Routes).pipe(
    Layer.provide(NodeHttpServer.layer(createServer, { port: config.port })),
    Layer.provide(NodeServices.layer),
  )

/** Unseeded server that also exposes its service graph, so tests drive real seams. */
export const makeTestServerLayer = (config: { readonly port: number }) =>
  serve(config).pipe(Layer.provideMerge(BackendServices))

export const makeServerLayer = (config: { readonly port: number }) =>
  serve(config).pipe(Layer.provide(BackendServices))
