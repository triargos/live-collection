import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiMiddleware, HttpApiSchema } from "effect/unstable/httpapi"
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
export class UnauthorizedError extends Schema.TaggedErrorClass<UnauthorizedError>()(
  "UnauthorizedError",
  { reason: Schema.String },
) {}
const UnauthorizedErrorResponse = UnauthorizedError.pipe(HttpApiSchema.status(401))

/** The authenticated session of the current request — provided by {@link SessionAuth}. */
export class CurrentSession extends Context.Service<CurrentSession, SessionCode>()("CurrentSession") {}

/**
 * Decodes `x-session-code` into {@link CurrentSession}. The contract kit only declares
 * the seam; the server implements the layer.
 */
export class SessionAuth extends HttpApiMiddleware.Service<
  SessionAuth,
  { provides: CurrentSession }
>()("SessionAuth", {
  error: UnauthorizedErrorResponse,
}) {}

export class ProjectNotFound extends Schema.TaggedErrorClass<ProjectNotFound>()(
  "ProjectNotFound",
  { id: ProjectId },
) {}
const ProjectNotFoundResponse = ProjectNotFound.pipe(HttpApiSchema.status(404))

export class TodoNotFound extends Schema.TaggedErrorClass<TodoNotFound>()(
  "TodoNotFound",
  { id: TodoId },
) {}
const TodoNotFoundResponse = TodoNotFound.pipe(HttpApiSchema.status(404))

export const projectsGroup = HttpApiGroup.make("projects")
  .add(HttpApiEndpoint.get("list", "/projects", { success: Schema.Array(Project) }))
  .add(HttpApiEndpoint.post("upsert", "/projects", {
    payload: Project,
    success: Project,
    error: UnauthorizedErrorResponse,
  }))
  .add(HttpApiEndpoint.delete("remove", "/projects/:id", {
    params: { id: ProjectId },
    error: ProjectNotFoundResponse,
  }))
  .middleware(SessionAuth)

export const todosGroup = HttpApiGroup.make("todos")
  .add(HttpApiEndpoint.get("list", "/todos", { success: Schema.Array(Todo) }))
  .add(HttpApiEndpoint.post("upsert", "/todos", {
    payload: Todo,
    success: Todo,
    error: UnauthorizedErrorResponse,
  }))
  .add(HttpApiEndpoint.delete("remove", "/todos/:id", {
    params: { id: TodoId },
    error: TodoNotFoundResponse,
  }))
  .middleware(SessionAuth)

export const syncApiGroup = HttpApiGroup.make("sync")
  .add(HttpApiEndpoint.get("catchup", "/catchup", {
    query: { from: SyncId },
    success: CatchupResponse,
  }))
  .middleware(SessionAuth)

export class DemoApi extends HttpApi.make("pi-demo")
  .add(projectsGroup)
  .add(todosGroup)
  .add(syncApiGroup)
  .prefix("/api") {}
