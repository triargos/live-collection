import { HttpRouter, HttpServerResponse } from "effect/unstable/http"
import { Effect, Result, Stream } from "effect"
import { sessionGroup } from "@pi-demo/shared"
import { SyncFeed } from "@triargos/live-collection-server"
import { sessionCodeFromRequest } from "./session-auth.js"

export const SseRoute = HttpRouter.add("GET", "/api/sync", (request) =>
  Effect.gen(function* () {
    const decoded = yield* Effect.result(sessionCodeFromRequest(request))
    if (Result.isFailure(decoded)) return HttpServerResponse.empty({ status: 401 })

    const feed = yield* SyncFeed
    return HttpServerResponse.stream(
      feed.streamEvents({ syncGroups: [sessionGroup(decoded.success)] }).pipe(Stream.encodeText),
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
