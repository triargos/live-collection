import { HttpApiBuilder } from "effect/unstable/httpapi"
import { Effect, Option } from "effect"
import {
  CurrentSession,
  DemoApi,
  ProjectNotFound,
  projectKey,
  PROJECT_MODEL,
  sessionGroup,
  TodoNotFound,
  todoKey,
  TODO_MODEL,
  UnauthorizedError,
} from "@pi-demo/shared"
import {
  intersects,
  PendingDelete,
  PendingInsert,
  PendingUpdate,
  squash,
  UserId,
} from "@triargos/live-collection-protocol"
import { ProjectRepo } from "../repo/project-repo.js"
import { TodoRepo } from "../repo/todo-repo.js"
import { SyncDispatcher } from "../sync/sync-dispatcher.js"
import { SyncEventStore } from "../sync/sync-event-store.js"
import { hydrateEvents } from "../sync/hydration.js"

const dispatchRow = (args: {
  readonly kind: "Insert" | "Update"
  readonly modelName: typeof PROJECT_MODEL | typeof TODO_MODEL
  readonly modelId: ReturnType<typeof projectKey> | ReturnType<typeof todoKey>
  readonly group: ReturnType<typeof sessionGroup>
}) =>
  Effect.flatMap(SyncDispatcher, (dispatcher) =>
    dispatcher.dispatch(
      args.kind === "Insert"
        ? PendingInsert.make({
            modelName: args.modelName,
            modelId: args.modelId,
            syncGroups: [args.group],
          })
        : PendingUpdate.make({
            modelName: args.modelName,
            modelId: args.modelId,
            syncGroups: [args.group],
          }),
    ),
  )

const rejectWrongSession = (matches: boolean) =>
  matches
    ? Effect.void
    : new UnauthorizedError({ reason: "row does not belong to this session" })

export const ProjectsApiLive = HttpApiBuilder.group(DemoApi, "projects", (handlers) =>
  handlers
    .handle("list", () =>
      Effect.gen(function* () {
        const session = yield* CurrentSession
        const repo = yield* ProjectRepo
        return yield* repo.list(session)
      }),
    )
    .handle("upsert", ({ payload }) =>
      Effect.gen(function* () {
        const session = yield* CurrentSession
        yield* rejectWrongSession(payload.sessionId === session)
        const repo = yield* ProjectRepo
        const existing = yield* repo.find(payload.id)
        yield* Option.match(existing, {
          onNone: () => Effect.void,
          onSome: (row) => rejectWrongSession(row.sessionId === session),
        })
        const saved = yield* repo.upsert(payload)
        yield* dispatchRow({
          kind: saved.kind,
          modelName: PROJECT_MODEL,
          modelId: projectKey(saved.row),
          group: sessionGroup(session),
        })
        return saved.row
      }),
    )
    .handle("remove", ({ params }) =>
      Effect.gen(function* () {
        const session = yield* CurrentSession
        const projects = yield* ProjectRepo
        const todos = yield* TodoRepo
        const dispatcher = yield* SyncDispatcher
        const existing = yield* projects.find(params.id)
        const owned = Option.filter(existing, (row) => row.sessionId === session)
        if (Option.isNone(owned)) return yield* new ProjectNotFound({ id: params.id })

        const removedTodos = yield* todos.removeByProject({ projectId: params.id, session })
        const removedProject = yield* projects.remove(params.id)
        const group = sessionGroup(session)
        for (const todo of removedTodos) {
          yield* dispatcher.dispatch(PendingDelete.make({
            modelName: TODO_MODEL,
            modelId: todoKey(todo),
            syncGroups: [group],
          }))
        }
        yield* dispatcher.dispatch(PendingDelete.make({
          modelName: PROJECT_MODEL,
          modelId: projectKey(removedProject),
          syncGroups: [group],
        }))
      }),
    ),
)

export const TodosApiLive = HttpApiBuilder.group(DemoApi, "todos", (handlers) =>
  handlers
    .handle("list", () =>
      Effect.gen(function* () {
        const session = yield* CurrentSession
        const repo = yield* TodoRepo
        return yield* repo.list(session)
      }),
    )
    .handle("upsert", ({ payload }) =>
      Effect.gen(function* () {
        const session = yield* CurrentSession
        yield* rejectWrongSession(payload.sessionId === session)
        const repo = yield* TodoRepo
        const projects = yield* ProjectRepo
        const project = yield* projects.find(payload.projectId)
        yield* Option.match(project, {
          onNone: () => new UnauthorizedError({ reason: "project does not belong to this session" }),
          onSome: (row) => rejectWrongSession(row.sessionId === session),
        })
        const existing = yield* repo.find(payload.id)
        yield* Option.match(existing, {
          onNone: () => Effect.void,
          onSome: (row) => rejectWrongSession(row.sessionId === session),
        })
        const saved = yield* repo.upsert(payload)
        yield* dispatchRow({
          kind: saved.kind,
          modelName: TODO_MODEL,
          modelId: todoKey(saved.row),
          group: sessionGroup(session),
        })
        return saved.row
      }),
    )
    .handle("remove", ({ params }) =>
      Effect.gen(function* () {
        const session = yield* CurrentSession
        const repo = yield* TodoRepo
        const dispatcher = yield* SyncDispatcher
        const existing = yield* repo.find(params.id)
        const owned = Option.filter(existing, (row) => row.sessionId === session)
        if (Option.isNone(owned)) return yield* new TodoNotFound({ id: params.id })
        const removed = yield* repo.remove(params.id)
        yield* dispatcher.dispatch(PendingDelete.make({
          modelName: TODO_MODEL,
          modelId: todoKey(removed),
          syncGroups: [sessionGroup(session)],
        }))
      }),
    ),
)

export const SyncApiLive = HttpApiBuilder.group(DemoApi, "sync", (handlers) =>
  handlers.handle("catchup", ({ query }) =>
    Effect.gen(function* () {
      const session = yield* CurrentSession
      const store = yield* SyncEventStore
      const allowed = [sessionGroup(session)]
      const all = yield* store.since(query.from)
      const visible = all.filter((event) => intersects(event.syncGroups, allowed))
      const context = { userId: UserId.make(session), syncGroups: allowed }
      const events = yield* hydrateEvents({ events: squash(visible), ctx: context })
      const lastSyncId = yield* store.currentSyncId
      return { events, lastSyncId }
    }),
  ),
)
