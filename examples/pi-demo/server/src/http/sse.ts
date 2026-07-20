import { HttpRouter, HttpServerResponse } from "effect/unstable/http"
import { Effect, Result, type Scope, Stream } from "effect"
import { sessionGroup } from "@pi-demo/shared"
import { SyncFeed } from "@triargos/live-collection-server"
import { sessionCodeFromRequest } from "./session-auth.js"

export const SseRoute = HttpRouter.add("GET", "/api/sync", (request) =>
  Effect.gen(function* () {
    const decoded = yield* Effect.result(sessionCodeFromRequest(request))
    if (Result.isFailure(decoded)) return HttpServerResponse.empty({ status: 401 })

    const feed = yield* SyncFeed
    // The bus subscription lives in the request scope: it is released when the
    // client disconnects and the response scope closes.
    const requestScope = yield* Effect.context<Scope.Scope>()
    return HttpServerResponse.stream(
      feed
        .streamEvents({ syncGroups: [sessionGroup(decoded.success)] })
        .pipe(Stream.provideContext(requestScope), Stream.encodeText),
      {
        contentType: "text/event-stream",
        headers: {
          "cache-control": "no-cache",
          connection: "keep-alive"
        }
      }
    )
  })
)
