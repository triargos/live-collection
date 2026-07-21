import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http"
import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Fiber, Layer, Schema, Stream } from "effect"
import {
  type Project,
  ProjectId,
  Todo,
  type Todo as TodoRow,
  TodoId,
  SessionCode,
} from "@pi-demo/shared"
import { SyncTransport } from "@triargos/live-collection"
import { makeTestServerLayer } from "../src/http/server.js"
import { testServerUrl } from "./support/test-url.js"

const session = SessionCode.make("ABC234")
const jsonRequest = (base: string, path: string, body: unknown) =>
  Effect.tryPromise(() => fetch(`${base}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-session-code": session },
    body: JSON.stringify(body),
  })).pipe(Effect.orDie)
const remove = (base: string, path: string) =>
  Effect.tryPromise(() => fetch(`${base}${path}`, {
    method: "DELETE",
    headers: { "x-session-code": session },
  })).pipe(Effect.orDie)

const project: Project = {
  id: ProjectId.make("sse-project"),
  sessionId: session,
  name: "SSE project",
  color: "#abcdef",
  createdAt: "2026-01-01T00:00:00.000Z",
}
const todo = (id: string, title: string): TodoRow => ({
  id: TodoId.make(id),
  sessionId: session,
  projectId: project.id,
  title,
  completed: false,
  createdAt: project.createdAt,
})

const SessionHttpClient = Layer.effect(
  HttpClient.HttpClient,
  Effect.map(
    HttpClient.HttpClient,
    HttpClient.mapRequest(HttpClientRequest.setHeader("x-session-code", session)),
  ),
).pipe(Layer.provide(FetchHttpClient.layer))

describe("GET /api/sync", () => {
  it.effect("is decoded by the library client and streams cascade deletes in order", () =>
    Effect.gen(function* () {
      const services = yield* Layer.build(makeTestServerLayer({ port: 0 }))
      const base = `${testServerUrl(services)}/api`
      const transportContext = yield* Layer.build(
        SyncTransport.layer({
          url: `${base}/sync`,
          keepAlive: "45 seconds",
        }).pipe(Layer.provide(SessionHttpClient)),
      )
      const transport = Context.get(transportContext, SyncTransport)
      const receivedFiber = yield* transport.connect.pipe(
        Stream.take(7),
        Stream.runCollect,
        Effect.forkChild,
      )
      yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 100)))

      assert.strictEqual((yield* jsonRequest(base, "/projects", project)).status, 200)
      const first = todo("sse-todo-1", "First")
      assert.strictEqual((yield* jsonRequest(base, "/todos", first)).status, 200)
      assert.strictEqual((yield* jsonRequest(base, "/todos", { ...first, title: "Updated" })).status, 200)
      assert.strictEqual((yield* remove(base, `/todos/${first.id}`)).status, 204)
      const cascaded = todo("sse-todo-2", "Cascaded")
      assert.strictEqual((yield* jsonRequest(base, "/todos", cascaded)).status, 200)
      assert.strictEqual((yield* remove(base, `/projects/${project.id}`)).status, 204)

      const events = yield* Fiber.join(receivedFiber)
      assert.deepStrictEqual(events.map((event) => event._tag), [
        "Insert", "Insert", "Update", "Delete", "Insert", "Delete", "Delete",
      ])
      assert.deepStrictEqual(events.slice(-2).map((event) =>
        event._tag === "Resync" ? "Resync" : String(event.modelName)), ["Todo", "Project"])

      const update = events[2]!
      assert.strictEqual(update._tag, "Update")
      if (update._tag === "Update") {
        const decoded = yield* Schema.decodeUnknownEffect(Todo)(update.data)
        assert.strictEqual(decoded.title, "Updated")
      }
    }).pipe(Effect.scoped))
})
