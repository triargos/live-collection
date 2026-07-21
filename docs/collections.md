# Collections

`defineCollection` declares one synced model and returns its **handle**. Calling the handle mounts (or reuses) the collection and returns a native TanStack `LiveCollection<T>` — pass it to `useLiveQuery`, or call `.insert/.update/.delete` on it.

## Scoped and global

The presence of `scopeOf` selects the overload.

**Scoped** — one independent instance per scope, mounted with `handle(scope)`:

```ts
const todosCollection = defineCollection({
  runtime,
  entity: "Todo",
  schema: Todo,
  getKey: (todo) => todo.id,
  scopeOf: (todo) => todo.projectId,
  listFn: (projectId) => Effect.flatMap(TodoApi, (api) => api.list(projectId)),
})

const todos = todosCollection(projectId)
```

**Global** — one app-wide instance, mounted with `handle()`:

```ts
const settingsCollection = defineCollection({
  runtime,
  entity: "Setting",
  schema: Setting,
  getKey: (s) => s.id,
  listFn: Effect.flatMap(SettingsApi, (api) => api.list),
})

const settings = settingsCollection()
```

Scoping is how you keep the in-memory working set bounded: a per-project collection holds one project's rows, and `runtime.registry.disposeScope(projectId)` releases them when the user leaves. The scope is an opaque string — the library has no opinion about what it means.

Handles mount synchronously and are referentially stable: the same `(entity, scope)` returns the same instance until it is disposed, so calling a handle in render is cheap.

## Configuration

| Field | Required | What it does |
|---|---|---|
| `runtime` | yes | The shared `LiveRuntime` from `makeLiveRuntime`. |
| `entity` | yes | The wire model name. Must match the `modelName` your backend emits — it routes events, keys the registry, and names the persisted table. |
| `schema` | yes | Effect schema for the model. Decodes every incoming event; changing it rebuilds the local table on next start. |
| `getKey` | yes | Extracts the primary key (branded `ModelId`). |
| `scopeOf` | no | Scope extractor, e.g. `(todo) => todo.projectId`. Present ⇒ scoped. |
| `listFn` | yes | Fetches the full current server truth (for one scope, if scoped). Runs on cold start and resync only. |
| `onInsert` / `onUpdate` / `onDelete` | no | Optimistic write handlers — see below. |
| `services` | when needed | A `ManagedRuntime` providing the Effect services `listFn` and the handlers require. Type-required exactly when they require any. |

## Optimistic writes

Wiring `onInsert`/`onUpdate`/`onDelete` makes the collection writable. Writes apply to the UI instantly; the handler confirms them with the server:

```ts
const todosCollection = defineCollection({
  // ...read configuration as above,
  onInsert: ({ transaction }) =>
    Effect.flatMap(TodoApi, (api) => api.create(transaction.mutations[0]!.modified)),
  onUpdate: ({ transaction }) =>
    Effect.flatMap(TodoApi, (api) => api.update(transaction.mutations[0]!.modified)),
  onDelete: ({ transaction }) =>
    Effect.flatMap(TodoApi, (api) => api.remove(transaction.mutations[0]!.key)),
})
```

The contract in one line: **a handler makes the server call and returns the confirmed row** (insert/update) or `void` (delete). Everything else is the library's job.

What happens on `todos.insert(row)`:

1. The optimistic row is visible instantly.
2. `onInsert` runs as an Effect on your `services` runtime and returns the server-confirmed row.
3. The library writes that row into the **synced store** before the mutation resolves — so when TanStack DB drops the optimistic state, the confirmed row is already there. No flicker.
4. The server's SSE echo of the same row arrives later and is an idempotent re-write of the same key.
5. The synced row persists to local SQLite and survives reload.

If the handler **fails** — `Effect.fail` with a tagged error, never a thrown exception — the optimistic write is rolled back and the row disappears. The reconcile only runs on success.

Notes:

- Insert/update must return the confirmed row; without it there is nothing to reconcile. If your server returns nothing, return the row you sent (`Effect.as(modified)`).
- **Mint ids on the client** (`crypto.randomUUID()`). The server keeps them, so the self-echo lands on the same key and needs no suppression.
- **One mutation per transaction.** A batched transaction fails with `BatchedMutationsUnsupported` before any server call.
- Handlers are plain Effects with your app's `R` — no `runPromise` boilerplate. `services` discharges `R`.

## Reading

The handle returns a native collection, so reads are TanStack DB's own API:

```tsx
import { useLiveQuery } from "@tanstack/react-db"

const todos = todosCollection(projectId)
const { data } = useLiveQuery((q) => q.from({ todo: todos }), [projectId])
```

Joins, filters, and aggregations across collections all work — a live collection is just a collection.

## Lifecycle

Collections outlive components. The registry owns their lifetime:

```ts
Effect.runFork(runtime.registry.disposeScope(projectId)) // leaving a workspace — globals survive
Effect.runFork(runtime.registry.disposeAllScoped())      // reset all scoped collections
Effect.runFork(runtime.registry.disposeAll())            // logout
runtime.dispose()                                        // app teardown
```

Disposing interrupts the collection's sync drain and runs TanStack cleanup, but leaves its local table intact — the next mount hydrates from disk and replays missed events from the local journal (see [architecture](./architecture.md#replay-on-mount)).

Stopping sync (`useLiveSync` unmount) and disposing collections are independent: one stops the network feed, the other releases instances.

## See also

- [Getting started](./getting-started.md) — the full setup walkthrough.
- [Architecture](./architecture.md) — how events reach a collection's drain.
- [Persistence](./persistence.md) — where the rows live between sessions.
