import { Context, Effect, Layer, Option, Ref } from "effect"
import {
  type ProjectId,
  type SessionCode,
  type Todo,
  TodoId,
  TodoNotFound,
} from "@pi-demo/shared"

export interface TodoRepoShape {
  readonly list: (session: SessionCode) => Effect.Effect<ReadonlyArray<Todo>>
  readonly find: (id: TodoId) => Effect.Effect<Option.Option<Todo>>
  readonly upsert: (
    row: Todo,
  ) => Effect.Effect<{ readonly row: Todo; readonly kind: "Insert" | "Update" }>
  readonly remove: (id: TodoId) => Effect.Effect<Todo, TodoNotFound>
  readonly removeByProject: (args: {
    readonly projectId: ProjectId
    readonly session: SessionCode
  }) => Effect.Effect<ReadonlyArray<Todo>>
}

const makeMemory: Effect.Effect<TodoRepoShape> = Effect.gen(function* () {
  const rows = yield* Ref.make<ReadonlyMap<TodoId, Todo>>(new Map())
  return {
    list: (session) =>
      Ref.get(rows).pipe(
        Effect.map((map) => Array.from(map.values()).filter((row) => row.sessionId === session)),
      ),
    find: (id) => Ref.get(rows).pipe(Effect.map((map) => Option.fromNullable(map.get(id)))),
    upsert: (row) =>
      Ref.modify(rows, (map) => {
        const kind = map.has(row.id) ? "Update" as const : "Insert" as const
        const next = new Map(map)
        next.set(row.id, row)
        return [{ row, kind }, next]
      }),
    remove: (id) =>
      Ref.modify(rows, (map) => {
        const found = map.get(id)
        if (found === undefined) return [Option.none<Todo>(), map]
        const next = new Map(map)
        next.delete(id)
        return [Option.some(found), next]
      }).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => new TodoNotFound({ id }),
            onSome: Effect.succeed,
          }),
        ),
      ),
    removeByProject: ({ projectId, session }) =>
      Ref.modify(rows, (map) => {
        const removed = Array.from(map.values()).filter(
          (todo) => todo.projectId === projectId && todo.sessionId === session,
        )
        if (removed.length === 0) return [removed, map]
        const next = new Map(map)
        for (const todo of removed) next.delete(todo.id)
        return [removed, next]
      }),
  }
})

export class TodoRepo extends Context.Tag("pi-demo/TodoRepo")<TodoRepo, TodoRepoShape>() {
  static readonly layerMemory: Layer.Layer<TodoRepo> = Layer.effect(TodoRepo, makeMemory)
}
