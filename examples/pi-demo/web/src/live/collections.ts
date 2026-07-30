import { FetchHttpClient, HttpApiClient, HttpClient, HttpClientRequest } from "@effect/platform"
import {
  defineCollection,
  type LiveRuntime,
  type ScopedHandle,
} from "@triargos/live-collection"
import { DemoApi, Project, type SessionCode, Todo, projectKey, todoKey } from "@pi-demo/shared"
import { Effect, Layer, ManagedRuntime } from "effect"
import { createRuntime } from "./runtime.js"

const makeApi = HttpApiClient.make(DemoApi)
type DemoClient = Effect.Effect.Success<typeof makeApi>

const withApi = <A, E>(f: (client: DemoClient) => Effect.Effect<A, E>) =>
  makeApi.pipe(Effect.flatMap(f))

export interface AppBundle {
  readonly runtime: LiveRuntime
  readonly session: SessionCode
  readonly todosCollection: ScopedHandle<Todo>
  readonly projectsCollection: ScopedHandle<Project>
}

export const createApp = async (args: { readonly session: SessionCode }): Promise<AppBundle> => {
  const httpClient = Layer.effect(
    HttpClient.HttpClient,
    Effect.map(
      HttpClient.HttpClient,
      HttpClient.mapRequest(HttpClientRequest.setHeader("x-session-code", args.session)),
    ),
  ).pipe(Layer.provide(FetchHttpClient.layer))

  const runtime = await createRuntime(httpClient)
  const services = ManagedRuntime.make(httpClient)

  const todosCollection = defineCollection({
    runtime,
    services,
    entity: "Todo",
    schema: Todo,
    getKey: todoKey,
    scopeOf: (todo) => todo.sessionId,
    listFn: (_session) => withApi((client) => client.todos.list()).pipe(Effect.orDie),
    onInsert: ({ transaction }) =>
      withApi((client) => client.todos.upsert({ payload: transaction.mutations[0]!.modified })),
    onUpdate: ({ transaction }) =>
      withApi((client) => client.todos.upsert({ payload: transaction.mutations[0]!.modified })),
    onDelete: ({ transaction }) =>
      withApi((client) =>
        client.todos.remove({ path: { id: transaction.mutations[0]!.original.id } }),
      ),
  })

  const projectsCollection = defineCollection({
    runtime,
    services,
    entity: "Project",
    schema: Project,
    getKey: projectKey,
    scopeOf: (project) => project.sessionId,
    listFn: (_session) => withApi((client) => client.projects.list()).pipe(Effect.orDie),
    onInsert: ({ transaction }) =>
      withApi((client) => client.projects.upsert({ payload: transaction.mutations[0]!.modified })),
    onUpdate: ({ transaction }) =>
      withApi((client) => client.projects.upsert({ payload: transaction.mutations[0]!.modified })),
    onDelete: ({ transaction }) =>
      withApi((client) =>
        client.projects.remove({ path: { id: transaction.mutations[0]!.original.id } }),
      ),
  })

  return { runtime, session: args.session, todosCollection, projectsCollection }
}
