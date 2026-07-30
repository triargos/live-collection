import { HttpLayerRouter, HttpServerResponse } from "@effect/platform"
import { Effect, Either, Stream } from "effect"
import { sessionGroup } from "@pi-demo/shared"
import { SyncFeed } from "@triargos/live-collection-server"
import { sessionCodeFromRequest } from "./session-auth.js"

export const SseRoute = HttpLayerRouter.add("GET", "/api/sync", (request) =>
  Effect.gen(function* () {
    const decoded = yield* Effect.either(sessionCodeFromRequest(request))
    if (Either.isLeft(decoded)) return HttpServerResponse.empty({ status: 401 })

    const feed = yield* SyncFeed
    return HttpServerResponse.stream(
      feed.streamEvents({ syncGroups: [sessionGroup(decoded.right)] }).pipe(Stream.encodeText),
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
