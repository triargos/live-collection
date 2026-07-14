import { HttpLayerRouter, HttpServerResponse } from "@effect/platform"
import { Effect, Either, Schema, Stream } from "effect"
import { sessionGroup } from "@pi-demo/shared"
import {
  HydratedSyncEventEnvelope,
  intersects,
  UserId,
} from "@triargos/live-collection-protocol"
import { ProjectRepo } from "../repo/project-repo.js"
import { TodoRepo } from "../repo/todo-repo.js"
import { SyncEventBus } from "../sync/sync-event-bus.js"
import { hydrateEvents } from "../sync/hydration.js"
import { sessionCodeFromRequest } from "./session-auth.js"

const encodeEnvelope = Schema.encode(
  Schema.parseJson(HydratedSyncEventEnvelope),
)

export const SseRoute = HttpLayerRouter.add("GET", "/api/sync", (request) =>
  Effect.gen(function* () {
    const decoded = yield* Effect.either(sessionCodeFromRequest(request))
    if (Either.isLeft(decoded)) return HttpServerResponse.empty({ status: 401 })

    const session = decoded.right
    const bus = yield* SyncEventBus
    const projects = yield* ProjectRepo
    const todos = yield* TodoRepo
    const allowed = [sessionGroup(session)]
    const queue = yield* bus.subscribe

    const events = Stream.fromQueue(queue).pipe(
      Stream.filter((event) => intersects(event.syncGroups, allowed)),
      Stream.mapEffect((event) =>
        hydrateEvents({
          events: [event],
          ctx: { userId: UserId.make(session), syncGroups: allowed },
        }).pipe(
          Effect.provideService(ProjectRepo, projects),
          Effect.provideService(TodoRepo, todos),
        ),
      ),
      Stream.mapConcat((hydrated) => hydrated),
      Stream.mapEffect((event) =>
        encodeEnvelope(event).pipe(Effect.map((json) => `data: ${json}\n\n`)),
      ),
    )
    const keepAlive = Stream.tick("15 seconds").pipe(
      Stream.map(() => ":ka\n\n"),
    )

    return HttpServerResponse.stream(
      Stream.merge(events, keepAlive).pipe(Stream.encodeText),
      {
        contentType: "text/event-stream",
        headers: {
          "cache-control": "no-cache",
          connection: "keep-alive",
        },
      },
    )
  }),
)
