import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Layer, Schema } from "effect"
import {
  CatchupResponse,
  deriveGroup,
  ModelId,
  ModelName,
  PendingSyncEvent,
} from "@triargos/live-collection-protocol"
import {
  type Project,
  Project as ProjectSchema,
  ProjectId,
  type Todo,
  TodoId,
  SessionCode,
} from "@pi-demo/shared"
import { makeTestServerLayer } from "../src/http/server.js"
import { ProjectRepo } from "../src/repo/project-repo.js"
import { TodoRepo } from "../src/repo/todo-repo.js"
import { SyncDispatcher, SyncEventStore } from "@triargos/live-collection-server"

const port = 34671
const base = `http://127.0.0.1:${port}/api`
const session = SessionCode.make("ABC234")
const otherSession = SessionCode.make("XYZ789")
const request = (path: string, init?: RequestInit, code = session) =>
  Effect.tryPromise(() => fetch(`${base}${path}`, {
    ...init,
    headers: { ...init?.headers, "x-session-code": code },
  })).pipe(Effect.orDie)
const jsonRequest = (path: string, body: unknown) =>
  request(path, {
    method: "POST",
    headers: { "content-type": "application/json", "x-session-code": session },
    body: JSON.stringify(body),
  })

const project: Project = {
  id: ProjectId.make("catchup-project"),
  sessionId: session,
  name: "Initial",
  color: "#123456",
  createdAt: "2026-01-01T00:00:00.000Z",
}
const todo: Todo = {
  id: TodoId.make("catchup-todo"),
  sessionId: session,
  projectId: project.id,
  title: "Transient",
  completed: false,
  createdAt: project.createdAt,
}

describe("GET /api/catchup", () => {
  it.effect("filters, squashes, hydrates, and converges to repository state", () =>
    Effect.gen(function* () {
      const services = yield* Layer.build(makeTestServerLayer({ port }))
      const dispatcher = Context.get(services, SyncDispatcher)
      const store = Context.get(services, SyncEventStore)
      const projects = Context.get(services, ProjectRepo)
      const todos = Context.get(services, TodoRepo)

      assert.strictEqual((yield* jsonRequest("/projects", project)).status, 200)
      assert.strictEqual((yield* jsonRequest("/projects", { ...project, name: "Confirmed" })).status, 200)
      assert.strictEqual((yield* jsonRequest("/todos", todo)).status, 200)
      assert.strictEqual((yield* request(`/todos/${todo.id}`, { method: "DELETE" })).status, 204)

      yield* dispatcher.dispatch(PendingSyncEvent.cases.Insert.make({
        modelName: ModelName.make("Foreign"),
        modelId: ModelId.make("hidden"),
        syncGroups: [deriveGroup(["other"])] as const,
      }))

      const otherResponse = yield* request("/catchup?from=0", undefined, otherSession)
      const otherBody = yield* Effect.tryPromise(() => otherResponse.json()).pipe(Effect.orDie)
      const otherCatchup = yield* Schema.decodeUnknownEffect(CatchupResponse)(otherBody)
      assert.strictEqual(otherCatchup.events.length, 0)

      const response = yield* request("/catchup?from=0")
      const unknownBody = yield* Effect.tryPromise(() => response.json()).pipe(Effect.orDie)
      const caughtUp = yield* Schema.decodeUnknownEffect(CatchupResponse)(unknownBody)
      const cursor = yield* store.getLatestSyncId

      assert.strictEqual(caughtUp.lastSyncId, cursor)
      assert.strictEqual(caughtUp.events.length, 1)
      const event = caughtUp.events[0]!
      assert.strictEqual(event._tag, "Insert")
      if (event._tag === "Insert") {
        assert.strictEqual(event.modelName, "Project")
        const decoded = yield* Schema.decodeUnknownEffect(ProjectSchema)(event.data)
        assert.strictEqual(decoded.name, "Confirmed")
      }

      const projectRows = yield* projects.list(session)
      const todoRows = yield* todos.list(session)
      assert.strictEqual(projectRows.length, 1)
      assert.strictEqual(projectRows[0]!.name, "Confirmed")
      assert.strictEqual(todoRows.length, 0)
      assert.deepStrictEqual(
        caughtUp.events.flatMap((item) =>
          item._tag === "Insert" || item._tag === "Update" ? [String(item.modelId)] : [],
        ),
        projectRows.map((item) => String(item.id)),
      )
    }).pipe(Effect.scoped))
})
