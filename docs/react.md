# React

React integration is one hook. Collections are native TanStack DB collections, so reads use `@tanstack/react-db` directly.

## Start sync

```tsx
import { useLiveSync } from "@triargos/live-collection-react"

export function App() {
  useLiveSync(runtime)
  return <Routes />
}
```

`useLiveSync(runtime)` forks the sync loop on mount and interrupts it on unmount. Mount it **once**, near the app root. There is no list of models to register — collections subscribe themselves when their handles mount.

Stopping sync does not dispose collections: their lifetime belongs to `runtime.registry`, so remounting the root reuses the warm local store.

## Read a collection

```tsx
import { useLiveQuery } from "@tanstack/react-db"

function TodoList({ projectId }: { projectId: string }) {
  const todos = todosCollection(projectId)
  const { data } = useLiveQuery((q) => q.from({ todo: todos }), [projectId])

  return <ul>{data.map((todo) => <li key={todo.id}>{todo.title}</li>)}</ul>
}
```

Calling the handle during render is synchronous and cheap — the registry returns the same instance for the same `(entity, scope)`. Joins across collections, filters, and aggregations are plain `useLiveQuery` features.

Writes go straight through the collection: `todos.insert(...)`, `todos.update(...)`, `todos.delete(...)` — see [collections](./collections.md#optimistic-writes).

## Dispose on navigation

When the user leaves a workspace and you want its memory back:

```ts
Effect.runFork(runtime.registry.disposeScope(projectId))
```

The next visit remounts from local SQLite and replays missed events. Use `disposeAll()` on logout and `runtime.dispose()` on app teardown.
