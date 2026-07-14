import { createServer } from "node:http"
import path from "node:path"
import {
  FileSystem,
  HttpLayerRouter,
  HttpServerResponse,
} from "@effect/platform"
import { NodeContext, NodeHttpServer } from "@effect/platform-node"
import { Config, Effect, Layer } from "effect"
import { DemoApi } from "@pi-demo/shared"
import { ProjectRepo } from "../repo/project-repo.js"
import { TodoRepo } from "../repo/todo-repo.js"
import { SyncEventBus } from "../sync/sync-event-bus.js"
import { SyncDispatcher } from "../sync/sync-dispatcher.js"
import { SyncEventStore } from "../sync/sync-event-store.js"
import { ProjectsApiLive, SyncApiLive, TodosApiLive } from "./api-live.js"
import { SessionAuthLive } from "./session-auth.js"
import { SseRoute } from "./sse.js"

const StorageServices = Layer.mergeAll(
  ProjectRepo.layerMemory,
  TodoRepo.layerMemory,
  SyncEventStore.layerMemory,
  SyncEventBus.layer,
)

export const BackendServices = Layer.merge(
  StorageServices,
  SyncDispatcher.layer.pipe(Layer.provide(StorageServices)),
)

const ApiHandlers = Layer.mergeAll(ProjectsApiLive, TodosApiLive, SyncApiLive)
const ApiRoute = HttpLayerRouter.addHttpApi(DemoApi).pipe(
  Layer.provide(ApiHandlers),
  Layer.provide(SessionAuthLive),
)

const StaticRoute = Layer.unwrapEffect(
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
    return HttpLayerRouter.add("GET", "*", (request) =>
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
  HttpLayerRouter.serve(Routes).pipe(
    Layer.provide(NodeHttpServer.layer(createServer, { port: config.port })),
    Layer.provide(NodeContext.layer),
  )

/** Unseeded server that also exposes its service graph, so tests drive real seams. */
export const makeTestServerLayer = (config: { readonly port: number }) =>
  serve(config).pipe(Layer.provideMerge(BackendServices))

export const makeServerLayer = (config: { readonly port: number }) =>
  serve(config).pipe(Layer.provide(BackendServices))
