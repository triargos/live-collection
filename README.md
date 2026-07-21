# live-collection

Local-first live collections for [Effect](https://effect.website) + [TanStack DB](https://tanstack.com/db).

A `LiveCollection<T>` is a **native** TanStack collection that persists locally (SQLite/OPFS), hydrates from disk on reload, and stays in sync with your backend over SSE + catchup. You define one collection per model, read it with `useLiveQuery`, and mutate it optimistically with `collection.insert/update/delete`. The library owns persistence, the durable sync cursor, catchup, and replay.

The backend is yours: the client speaks a small wire contract — one catchup endpoint and one SSE stream. Implement it on any stack, or use the optional Effect kernel package.

## Packages

| Package | What it is |
|---|---|
| [`@triargos/live-collection`](packages/live-collection) | The frontend engine: `defineCollection`, `makeLiveRuntime`, transport, persistence. |
| [`@triargos/live-collection-protocol`](packages/protocol) | The shared wire contract: schemas, sync groups, the squasher. Pure, no I/O. |
| [`@triargos/live-collection-react`](packages/react) | React bindings: `useLiveSync`. Reads use `@tanstack/react-db` directly. |
| [`@triargos/live-collection-server`](packages/server) | Optional backend kernel for Effect servers: the contract's invariants as code. |

## Install

```bash
pnpm add @triargos/live-collection @triargos/live-collection-protocol
pnpm add @triargos/live-collection-react   # React apps
```

## At a glance

```ts
// Once at startup: a runtime with local persistence + your sync endpoints.
const runtime = makeLiveRuntime({ persistence, sync })

// Once per model: a collection handle.
const todosCollection = defineCollection({
  runtime,
  entity: "Todo",
  schema: Todo,
  getKey: (todo) => todo.id,
  scopeOf: (todo) => todo.projectId,
  listFn: (projectId) => api.todos.list(projectId),
  onInsert: ({ transaction }) => api.todos.create(transaction.mutations[0]!.modified),
})
```

```tsx
// In React: start sync once, then read and write collections anywhere.
useLiveSync(runtime)

const todos = todosCollection(projectId)
const { data } = useLiveQuery((q) => q.from({ todo: todos }))
todos.insert({ id: crypto.randomUUID(), projectId, title })
```

The insert appears instantly, your handler confirms it with the server, the confirmed row lands in the synced store before the optimistic state resolves — no flicker — and the row survives a reload from local SQLite.

## Documentation

- **[Getting started](docs/getting-started.md)** — the step-by-step implementer guide, from backend endpoints to a rendered list.
- [Collections](docs/collections.md) — `defineCollection`, scoping, optimistic writes, lifecycle.
- [Persistence](docs/persistence.md) — local SQLite setup and behavior.
- [React](docs/react.md) — `useLiveSync` and reading with `useLiveQuery`.
- [Architecture](docs/architecture.md) — how sync works underneath: broker, journal, replay, resync.
- [Backend contract](docs/backend.md) — the two endpoints your server provides and the invariants they must satisfy.
- [Protocol reference](docs/protocol.md) — every wire schema, the sync-group grammar, the squasher.

A complete working example — Effect backend, React SPA, OPFS persistence, live cross-device sync — lives in [`examples/pi-demo`](examples/pi-demo).

## License

MIT
