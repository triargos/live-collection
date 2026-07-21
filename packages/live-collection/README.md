# `@triargos/live-collection`

Local-first live collections for [Effect](https://effect.website) + [TanStack DB](https://tanstack.com/db).

A `LiveCollection<T>` is a **native** TanStack collection that persists locally (SQLite/OPFS), hydrates from disk on reload, and stays in sync with your backend over SSE + catchup. Define one collection per model, read it with `useLiveQuery`, and mutate it optimistically with `collection.insert/update/delete`. The library owns persistence, the durable sync cursor, catchup, and replay.

The backend is yours: the client speaks a small wire contract — one catchup endpoint and one SSE stream — defined in [`@triargos/live-collection-protocol`](https://www.npmjs.com/package/@triargos/live-collection-protocol). Implement it on any stack, or use the optional Effect kernel [`@triargos/live-collection-server`](https://www.npmjs.com/package/@triargos/live-collection-server).

## Install

```bash
pnpm add @triargos/live-collection @triargos/live-collection-protocol
pnpm add @triargos/live-collection-react   # React apps
```

## At a glance

```ts
const runtime = makeLiveRuntime({ persistence, sync })

const todosCollection = defineCollection({
  runtime,
  entity: "Todo",
  schema: Todo,
  getKey: (todo) => todo.id,
  scopeOf: (todo) => todo.projectId,
  listFn: (projectId) => api.todos.list(projectId),
  onInsert: ({ transaction }) => api.todos.create(transaction.mutations[0]!.modified),
})

const todos = todosCollection(projectId)
const { data } = useLiveQuery((q) => q.from({ todo: todos }))
todos.insert({ id: crypto.randomUUID(), projectId, title })
```

## Documentation

- [Getting started](https://github.com/triargos/live-collection/blob/main/docs/getting-started.md) — the step-by-step implementer guide.
- [Collections](https://github.com/triargos/live-collection/blob/main/docs/collections.md) — `defineCollection`, scoping, optimistic writes, lifecycle.
- [Persistence](https://github.com/triargos/live-collection/blob/main/docs/persistence.md) — local SQLite setup and behavior.
- [Architecture](https://github.com/triargos/live-collection/blob/main/docs/architecture.md) — how sync works underneath.
- [Backend contract](https://github.com/triargos/live-collection/blob/main/docs/backend.md) — what your server must provide.

## License

MIT
