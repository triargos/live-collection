import { HttpRouter, HttpServerResponse } from "effect/unstable/http"
import { Effect, Result, Schema, Stream } from "effect"
import { sessionGroup } from "@pi-demo/shared"
import {
  HydratedSyncEventEnvelope,
  intersects,
} from "@triargos/live-collection-protocol"
import { ProjectRepo } from "../repo/project-repo.js"
import { TodoRepo } from "../repo/todo-repo.js"
import { SyncEventBus } from "../sync/sync-event-bus.js"
import { hydrateEvents } from "../sync/hydration.js"
import { sessionCodeFromRequest } from "./session-auth.js"

const encodeEnvelope = Schema.encodeEffect(
  Schema.fromJsonString(HydratedSyncEventEnvelope),
)

export const SseRoute = HttpRouter.add("GET", "/api/sync", (request) =>
  Effect.gen(function* () {
    const decoded = yield* Effect.result(sessionCodeFromRequest(request))
    if (Result.isFailure(decoded)) return HttpServerResponse.empty({ status: 401 })

    const session = decoded.success
    const bus = yield* SyncEventBus
    const projects = yield* ProjectRepo
    const todos = yield* TodoRepo
    const allowed = [sessionGroup(session)]
    const subscription = yield* bus.subscribe

    const events = Stream.fromSubscription(subscription).pipe(
      Stream.filter((event) => intersects(event.syncGroups, allowed)),
      Stream.mapEffect((event) =>
        hydrateEvents({
          events: [event],
          syncGroups: allowed,
        }).pipe(
          Effect.provideService(ProjectRepo, projects),
          Effect.provideService(TodoRepo, todos),
        ),
      ),
      Stream.flatMap(Stream.fromIterable),
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
