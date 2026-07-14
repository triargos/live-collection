import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiMiddleware, HttpApiSchema } from "@effect/platform"
import { Context, Schema } from "effect"
import { CatchupResponse, SyncId } from "@triargos/live-collection-protocol"
import { Project, ProjectId, SessionCode, Todo, TodoId } from "./domain.js"

/**
 * The demo's HTTP contract. The server implements it with `HttpApiBuilder`; the web app
 * derives a typed client with `HttpApiClient.make(DemoApi)`.
 *
 * The live SSE stream (`GET /api/sync`) is deliberately NOT part of this api — it's a
 * long-lived `text/event-stream`, served by a raw route mounted next to the api. Its
 * payload contract is `HydratedSyncEventEnvelope` from the protocol package, one event
 * per SSE `data:` frame.
 *
 * Writes are upserts: `onInsert` and `onUpdate` handlers both send the full row and get
 * the server-confirmed row back (the library reconciles it into the synced baseline).
 * The server decides Insert-vs-Update by prior existence when it dispatches sync events.
 *
 * Every request carries the session in an `x-session-code` header — the client sets it
 * once on a wrapped `HttpClient` (which also covers the SSE connect, since the transport
 * is fetch-based), and {@link SessionAuth} decodes it into {@link CurrentSession} on the
 * server. Rows are only visible to — and mutable by — requests holding their session code.
 */

/** The session header is missing or not a valid code. */
export class UnauthorizedError extends Schema.TaggedError<UnauthorizedError>()(
  "UnauthorizedError",
  { reason: Schema.String },
  HttpApiSchema.annotations({ status: 401 }),
) {}

/** The authenticated session of the current request — provided by {@link SessionAuth}. */
export class CurrentSession extends Context.Tag("CurrentSession")<CurrentSession, SessionCode>() {}

/**
 * Decodes `x-session-code` into {@link CurrentSession}. The contract kit only declares
 * the seam; the server implements the layer.
 */
export class SessionAuth extends HttpApiMiddleware.Tag<SessionAuth>()("SessionAuth", {
  failure: UnauthorizedError,
  provides: CurrentSession,
}) {}

export class ProjectNotFound extends Schema.TaggedError<ProjectNotFound>()(
  "ProjectNotFound",
  { id: ProjectId },
  HttpApiSchema.annotations({ status: 404 }),
) {}

export class TodoNotFound extends Schema.TaggedError<TodoNotFound>()(
  "TodoNotFound",
  { id: TodoId },
  HttpApiSchema.annotations({ status: 404 }),
) {}

export const projectsGroup = HttpApiGroup.make("projects")
  .middleware(SessionAuth)
  .add(HttpApiEndpoint.get("list", "/projects").addSuccess(Schema.Array(Project)))
  .add(HttpApiEndpoint.post("upsert", "/projects").setPayload(Project).addSuccess(Project))
  .add(
    HttpApiEndpoint.del("remove", "/projects/:id")
      .setPath(Schema.Struct({ id: ProjectId }))
      .addSuccess(Schema.Void)
      .addError(ProjectNotFound),
  )

export const todosGroup = HttpApiGroup.make("todos")
  .middleware(SessionAuth)
  .add(HttpApiEndpoint.get("list", "/todos").addSuccess(Schema.Array(Todo)))
  .add(HttpApiEndpoint.post("upsert", "/todos").setPayload(Todo).addSuccess(Todo))
  .add(
    HttpApiEndpoint.del("remove", "/todos/:id")
      .setPath(Schema.Struct({ id: TodoId }))
      .addSuccess(Schema.Void)
      .addError(TodoNotFound),
  )

export const syncApiGroup = HttpApiGroup.make("sync")
  .middleware(SessionAuth)
  .add(
    HttpApiEndpoint.get("catchup", "/catchup")
      .setUrlParams(Schema.Struct({ from: SyncId }))
      .addSuccess(CatchupResponse),
  )

export class DemoApi extends HttpApi.make("pi-demo")
  .add(projectsGroup)
  .add(todosGroup)
  .add(syncApiGroup)
  .prefix("/api") {}
