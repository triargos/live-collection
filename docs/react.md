# React

The React package contains one lifecycle hook. Collections and reads stay native TanStack DB APIs.

## Start sync

```tsx
import { useLiveSync } from "@triargos/live-collection-react"

export function App() {
  useLiveSync(runtime)
  return <Routes />
}
```

`useLiveSync(runtime)` calls `runtime.forkSync()` on mount and interrupts that fiber on unmount. Mount it once near the app root.

Interrupting ingest does not dispose collections. Their lifetime belongs to `runtime.registry`, so remounting the React root can reuse warm persisted collections. Calling `forkSync` again while a previous ingest fiber is active interrupts the previous one.

There is no models argument. Every collection subscribes itself when its handle is mounted.

## Read a collection

`defineCollection` returns a native TanStack collection, so use `useLiveQuery` directly from `@tanstack/react-db`:

```tsx
import { useLiveQuery } from "@tanstack/react-db"

function WebhookList({ orgId }: { readonly orgId: string }) {
  const collection = webhooks(orgId)
  const { data } = useLiveQuery((query) =>
    query.from({ webhook: collection }),
  )

  return <ul>{data.map((row) => <li key={row.id}>{row.url}</li>)}</ul>
}
```

Calling `webhooks(orgId)` during render is synchronous and cheap. The registry returns the same object for the same `(entity, scope)` key.

## Lifecycle

Dispose workspace collections when the app no longer wants to retain them:

```ts
Effect.runFork(runtime.registry.disposeScope(orgId))
```

That closes each matching collection's child scope, interrupts its broker drain, and runs TanStack cleanup. Globals remain mounted. Use `disposeAllScoped` for a workspace reset, `disposeAll` for logout, and `runtime.dispose()` for app teardown.

## Runtime setup

```ts
const runtime = makeLiveRuntime({
  persistence,
  sync: Layer.mergeAll(
    SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" }),
    CatchupClient.layer({ url: "/api/catchup" }),
    SyncCursor.layer,
    SyncJournal.layer(),
  ).pipe(Layer.provide(FetchHttpClient.layer)),
})
```

Resync refetches active collections in place. There is no `onResync` or window-reload API.
