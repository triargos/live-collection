import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { HttpApiClient } from "effect/unstable/httpapi"
import { NodeHttpClient } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Context, Duration, Effect, Fiber, Layer, ManagedRuntime } from "effect"
import {
  CatchupClient,
  defineCollection,
  SyncJournal,
  makeLiveRuntime,
  SyncTransport,
} from "@triargos/live-collection"
import {
  DemoApi,
  type Project as ProjectRow,
  Project,
  ProjectId,
  projectKey,
  type Todo as TodoRow,
  Todo,
  TodoId,
  todoKey,
  randomSessionCode,
} from "@pi-demo/shared"
import { makeTestServerLayer } from "../src/http/server.js"
import { ProjectRepo } from "../src/repo/project-repo.js"
import { TodoRepo } from "../src/repo/todo-repo.js"
import { SyncId } from "@triargos/live-collection-protocol"
import { makeNodeSqlitePersistence } from "./support/sqlite-persistence.js"
import { testServerUrl } from "./support/test-url.js"

const session = randomSessionCode()
const addSessionHeader = HttpClient.mapRequest(
  HttpClientRequest.setHeader("x-session-code", session),
)
const makeApiFor = (baseUrl: string) =>
  HttpApiClient.make(DemoApi, {
    baseUrl,
    transformClient: addSessionHeader,
  })
type DemoClient = Effect.Success<ReturnType<typeof makeApiFor>>

const waitUntil = (condition: () => boolean): Effect.Effect<void> => {
  const attempt: Effect.Effect<void> = Effect.suspend(() =>
    condition() ? Effect.void : Effect.sleep(Duration.millis(20)).pipe(Effect.andThen(attempt)),
  )
  return attempt.pipe(
    Effect.timeoutOrElse({ duration: Duration.seconds(10), orElse: () => Effect.fail(new Error("condition not met within 10 seconds")) }),
    Effect.orDie,
  )
}

const waitUntilEffect = <E>(condition: Effect.Effect<boolean, E>): Effect.Effect<void> => {
  const checked = condition.pipe(Effect.orDie)
  const attempt: Effect.Effect<void> = Effect.flatMap(checked, (ready) =>
    ready ? Effect.void : Effect.sleep(Duration.millis(20)).pipe(Effect.andThen(attempt)),
  )
  return attempt.pipe(
    Effect.timeoutOrElse({ duration: Duration.seconds(10), orElse: () => Effect.fail(new Error("effectful condition not met within 10 seconds")) }),
    Effect.orDie,
  )
}

const project = (id: string, name: string): ProjectRow => ({
  id: ProjectId.make(id),
  sessionId: session,
  name,
  color: "#8b5cf6",
  createdAt: "2026-07-13T00:00:00.000Z",
})

const todo = (args: {
  readonly id: string
  readonly projectId: ProjectId
  readonly title: string
}): TodoRow => ({
  id: TodoId.make(args.id),
  sessionId: session,
  projectId: args.projectId,
  title: args.title,
  completed: false,
  createdAt: "2026-07-13T00:00:00.000Z",
})

describe("pi-demo client ↔ server", () => {
  it.live("converges across snapshots, SSE, optimistic writes, cascades, and reconnect catchup", () =>
    Effect.gen(function* () {
      const backend = yield* Layer.build(makeTestServerLayer({ port: 0 }))
      const baseUrl = testServerUrl(backend)
      const makeApi = makeApiFor(baseUrl)
      const withApi = <A, E>(f: (client: DemoClient) => Effect.Effect<A, E>) =>
        makeApi.pipe(Effect.flatMap(f))
      const projectRepo = Context.get(backend, ProjectRepo)
      const todoRepo = Context.get(backend, TodoRepo)
      const seedProjects = [project("e2e-seed-work", "Work"), project("e2e-seed-home", "Home")] as const
      const seedTodos = [
        todo({ id: "e2e-seed-1", projectId: seedProjects[0].id, title: "Ship the demo" }),
        todo({ id: "e2e-seed-2", projectId: seedProjects[0].id, title: "Test reconnect" }),
        todo({ id: "e2e-seed-3", projectId: seedProjects[0].id, title: "Open another client" }),
        todo({ id: "e2e-seed-4", projectId: seedProjects[1].id, title: "Buy coffee" }),
        todo({ id: "e2e-seed-5", projectId: seedProjects[1].id, title: "Water plants" }),
      ] as const
      yield* Effect.forEach(seedProjects, projectRepo.upsert, { discard: true })
      yield* Effect.forEach(seedTodos, todoRepo.upsert, { discard: true })

      const otherClient = yield* makeApi
      const cursorMarker = project("e2e-cursor-marker", "Cursor marker")
      yield* otherClient.projects.upsert({ payload: cursorMarker })
      yield* projectRepo.remove(cursorMarker.id)

      const journalContext = yield* Layer.build(SyncJournal.layerMemory)
      const journal = Context.get(journalContext, SyncJournal)
      yield* journal.setLastIngestedSyncId(SyncId.make("1"))

      const SessionHttpClient = Layer.effect(
        HttpClient.HttpClient,
        Effect.map(HttpClient.HttpClient, addSessionHeader),
      ).pipe(Layer.provide(NodeHttpClient.layerUndici))
      const services = ManagedRuntime.make(SessionHttpClient)
      const sync = Layer.mergeAll(
        SyncTransport.layer({ url: `${baseUrl}/api/sync`, keepAlive: "45 seconds" }),
        CatchupClient.layer({ url: `${baseUrl}/api/catchup` }),
        Layer.succeed(SyncJournal, journal),
      ).pipe(Layer.provide(SessionHttpClient))
      const runtime = makeLiveRuntime({
        persistence: makeNodeSqlitePersistence(),
        sync,
      })
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => runtime.dispose()).pipe(
          Effect.andThen(Effect.promise(() => services.dispose())),
          Effect.asVoid,
        ),
      )

      const projectsHandle = defineCollection({
        runtime,
        services,
        entity: "Project",
        schema: Project,
        getKey: projectKey,
        scopeOf: (row) => row.sessionId,
        listFn: (_session: string) => withApi((client) => client.projects.list()).pipe(Effect.orDie),
      })
      const todosHandle = defineCollection({
        runtime,
        services,
        entity: "Todo",
        schema: Todo,
        getKey: todoKey,
        scopeOf: (row) => row.sessionId,
        listFn: (_session: string) => withApi((client) => client.todos.list()).pipe(Effect.orDie),
        onInsert: ({ transaction }) =>
          withApi((client) => client.todos.upsert({ payload: transaction.mutations[0]!.modified })),
        onUpdate: ({ transaction }) =>
          withApi((client) => client.todos.upsert({ payload: transaction.mutations[0]!.modified })),
        onDelete: ({ transaction }) =>
          withApi((client) =>
            client.todos.remove({ params: { id: transaction.mutations[0]!.original.id } }),
          ),
      })
      const projects = projectsHandle(session)
      const todos = todosHandle(session)
      yield* Effect.promise(() => Promise.all([projects.preload(), todos.preload()]).then(() => undefined))

      let loopFiber = runtime.forkSync()

      yield* waitUntil(() => projects.size === 2 && todos.size === 5)
      assert.strictEqual(projects.size, 2)
      assert.strictEqual(todos.size, 5)
      const seedProject = Array.from(projects.values())[0]!

      const liveTodo = todo({ id: "e2e-live", projectId: seedProject.id, title: "Live insert" })
      yield* waitUntilEffect(
        otherClient.todos.upsert({ payload: liveTodo }).pipe(
          Effect.andThen(Effect.sync(() => todos.has(todoKey(liveTodo)))),
        ),
      )
      assert.strictEqual(todos.get(todoKey(liveTodo))?.title, "Live insert")

      const updatedLiveTodo = { ...liveTodo, title: "Live update" }
      yield* otherClient.todos.upsert({ payload: updatedLiveTodo })
      yield* waitUntil(() => todos.get(todoKey(liveTodo))?.title === "Live update")
      assert.strictEqual(todos.get(todoKey(liveTodo))?.title, "Live update")

      yield* otherClient.todos.remove({ params: { id: liveTodo.id } })
      yield* waitUntil(() => !todos.has(todoKey(liveTodo)))
      assert.isFalse(todos.has(todoKey(liveTodo)))

      const cascadeProject = project("e2e-cascade-project", "Cascade")
      const cascadeOne = todo({ id: "e2e-cascade-one", projectId: cascadeProject.id, title: "One" })
      const cascadeTwo = todo({ id: "e2e-cascade-two", projectId: cascadeProject.id, title: "Two" })
      yield* otherClient.projects.upsert({ payload: cascadeProject })
      yield* otherClient.todos.upsert({ payload: cascadeOne })
      yield* otherClient.todos.upsert({ payload: cascadeTwo })
      yield* waitUntil(() =>
        projects.has(projectKey(cascadeProject)) &&
        todos.has(todoKey(cascadeOne)) &&
        todos.has(todoKey(cascadeTwo)),
      )

      yield* otherClient.projects.remove({ params: { id: cascadeProject.id } })
      yield* waitUntil(() =>
        !projects.has(projectKey(cascadeProject)) &&
        !todos.has(todoKey(cascadeOne)) &&
        !todos.has(todoKey(cascadeTwo)),
      )
      assert.isFalse(projects.has(projectKey(cascadeProject)))
      assert.isFalse(todos.has(todoKey(cascadeOne)))
      assert.isFalse(todos.has(todoKey(cascadeTwo)))

      const optimistic = todo({
        id: "e2e-optimistic",
        projectId: seedProject.id,
        title: "Optimistically confirmed",
      })
      const optimisticTx = todos.insert(optimistic)
      assert.isTrue(todos.has(todoKey(optimistic)))
      yield* Effect.promise(() => optimisticTx.isPersisted.promise)
      yield* waitUntilEffect(
        otherClient.todos.list().pipe(
          Effect.map((rows) => rows.some((row) => row.id === optimistic.id)),
        ),
      )
      yield* waitUntil(() =>
        Array.from(todos.values()).filter((row) => row.id === optimistic.id).length === 1,
      )
      assert.strictEqual(
        Array.from(todos.values()).filter((row) => row.id === optimistic.id).length,
        1,
      )

      yield* Fiber.interrupt(loopFiber)
      const offline = todo({ id: "e2e-offline", projectId: seedProject.id, title: "Caught up" })
      const squashed = todo({ id: "e2e-squashed", projectId: seedProject.id, title: "Never visible" })
      yield* otherClient.todos.upsert({ payload: offline })
      yield* otherClient.todos.upsert({ payload: squashed })
      yield* otherClient.todos.remove({ params: { id: squashed.id } })
      assert.isFalse(todos.has(todoKey(offline)))
      assert.isFalse(todos.has(todoKey(squashed)))

      loopFiber = runtime.forkSync()
      yield* waitUntil(() => todos.has(todoKey(offline)))
      assert.strictEqual(todos.get(todoKey(offline))?.title, "Caught up")
      assert.isFalse(todos.has(todoKey(squashed)))

      const serverTodos = yield* otherClient.todos.list()
      assert.deepStrictEqual(
        Array.from(todos.values()).map((row) => row.id).sort(),
        serverTodos.map((row) => row.id).sort(),
      )
      yield* Fiber.interrupt(loopFiber)
    }).pipe(Effect.provide(NodeHttpClient.layerUndici), Effect.scoped),
  )
})
