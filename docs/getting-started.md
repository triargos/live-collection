# Getting started

This guide takes you from nothing to a live, locally persisted, optimistically writable collection. Six steps:

1. [Provide the two backend endpoints](#1-provide-the-backend-endpoints)
2. [Build the runtime](#2-build-the-runtime)
3. [Define your collections](#3-define-your-collections)
4. [Start sync](#4-start-sync)
5. [Read and write](#5-read-and-write)
6. [Manage lifecycle](#6-manage-lifecycle)

Steps 2–6 are frontend code. Step 1 is your server — any stack works, and Effect backends can use the kernel package. The [`examples/pi-demo`](../examples/pi-demo) app implements every step end to end.

## 1. Provide the backend endpoints

The client needs two HTTP surfaces:

- **`GET /catchup?from=<syncId>`** — returns every event since the client's cursor, as JSON: `{ events, lastSyncId, epoch? }`. This is the source of truth; the client calls it on startup and after every reconnect.
- **`GET /sync`** — a long-lived SSE stream of new events, one JSON event per frame, plus periodic keepalive comments. Best-effort: a missed event here is healed by the next catchup.

Both deliver **hydrated** events — `Insert`/`Update` carry the entity's *current* data, `Delete` carries none — filtered to what the caller is allowed to see. Visibility is resolved server-side from the caller's auth on every request; the client never sends a group list.

On Effect, `@triargos/live-collection-server` reduces this to three call sites. You supply an event store and a model registry; the kernel handles compaction, hydration, group filtering, keepalives, and retention:

```ts
// Catchup route
const feed = yield* SyncFeed
return yield* feed.catchup({ fromSyncId: query.from, syncGroups: groupsFor(session) })

// SSE route — streamEvents emits ready-to-send frame strings
return HttpServerResponse.stream(
  feed.streamEvents({ syncGroups: groupsFor(session) }).pipe(Stream.encodeText),
  { contentType: "text/event-stream", headers: { "cache-control": "no-cache" } },
)

// After every authoritative write
const dispatcher = yield* SyncDispatcher
yield* dispatcher.dispatch(PendingSyncEvent.cases.Insert.make({ modelName, modelId, syncGroups }))
```

Building the backend by hand instead? Read the [backend contract](./backend.md) — it specifies both endpoints and the invariants the client relies on. Either way, decode and encode wire payloads with the schemas from `@triargos/live-collection-protocol`.

## 2. Build the runtime

The runtime is built once at startup. It needs two things: a **persistence value** (local SQLite) and a **sync layer** (transport + catchup + journal pointed at your endpoints).

```ts
import { CatchupClient, makeLiveRuntime, SyncJournal, SyncTransport } from "@triargos/live-collection"
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"
import { Layer } from "effect"
import { FetchHttpClient } from "effect/unstable/http"

const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "myapp" })
const persistence = createBrowserWASQLitePersistence({ database })

const sync = Layer.mergeAll(
  SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" }),
  CatchupClient.layer({ url: "/api/catchup" }),
  SyncJournal.layer({ databaseName: "myapp-eventlog" }),
).pipe(Layer.provide(FetchHttpClient.layer))

export const runtime = makeLiveRuntime({ persistence, sync })
```

What each piece is for:

- **`persistence`** — where collections store their rows. On reload, a collection hydrates from this database instead of re-fetching the server. See [persistence](./persistence.md) for setup details and the Vite configuration it needs.
- **`SyncTransport`** — the SSE connection. `keepAlive` is the silence window after which the client treats the connection as dead and reconnects; your server's keepalive interval must be shorter.
- **`CatchupClient`** — the catchup endpoint.
- **`SyncJournal`** — a durable local event log (IndexedDB) holding the sync cursor and recent events, so a collection that mounts later can replay what it missed without a network round trip.

If your requests need auth headers, provide a customized `HttpClient` layer instead of the bare `FetchHttpClient.layer` — see [`examples/pi-demo/web/src/live/collections.ts`](../examples/pi-demo/web/src/live/collections.ts) for a header-injecting example.

## 3. Define your collections

One `defineCollection` per synced model, at module level. It returns a **handle** — a function you call to get the native collection.

```ts
import { defineCollection } from "@triargos/live-collection"
import { Effect, ManagedRuntime } from "effect"

const services = ManagedRuntime.make(ApiClientLayer)

export const todosCollection = defineCollection({
  runtime,
  services,
  entity: "Todo",                          // the wire model name — must match what the backend emits
  schema: Todo,                            // Effect schema; decodes incoming events
  getKey: (todo) => todo.id,               // primary key, branded ModelId
  scopeOf: (todo) => todo.projectId,       // present ⇒ scoped: one instance per project
  listFn: (projectId) =>                   // full snapshot — used on cold start and resync
    Effect.flatMap(TodoApi, (api) => api.list(projectId)),
  onInsert: ({ transaction }) =>           // optimistic write: call the server, return the confirmed row
    Effect.flatMap(TodoApi, (api) => api.create(transaction.mutations[0]!.modified)),
  onUpdate: ({ transaction }) =>
    Effect.flatMap(TodoApi, (api) => api.update(transaction.mutations[0]!.modified)),
  onDelete: ({ transaction }) =>
    Effect.flatMap(TodoApi, (api) => api.remove(transaction.mutations[0]!.key)),
})
```

What's expected of each field:

- **`entity`** must equal the `modelName` your backend stamps on events — it routes events to this collection and names the persisted table.
- **`schema`** decodes every incoming event at the boundary. Changing it automatically rebuilds the local table on next start.
- **`scopeOf`** decides the overload. With it, the collection is **scoped**: `todosCollection(projectId)` mounts one independent instance per project, which keeps the in-memory working set bounded. Without it, the collection is **global**: `settingsCollection()`.
- **`listFn`** fetches current server truth for one scope. It runs only on cold start (no local data yet) and on resync — normal operation syncs by deltas.
- **`onInsert` / `onUpdate` / `onDelete`** are optional and make the collection writable. Each handler does exactly one thing: the server call. Insert/update **return the server-confirmed row**; the library reconciles it into the synced store for you. A failed handler rolls the optimistic write back.
- **`services`** is a `ManagedRuntime` providing whatever Effect services your `listFn` and handlers require. It's required exactly when they require something; omit it otherwise.

More detail in [collections](./collections.md).

## 4. Start sync

Start ingest once, near your app root. In React:

```tsx
import { useLiveSync } from "@triargos/live-collection-react"

export function App() {
  useLiveSync(runtime)
  return <Routes />
}
```

Outside React, call `runtime.forkSync()` directly. Either way this starts one app-wide loop: catchup from the stored cursor, then tail SSE; on disconnect, catch up again and resume. There is no collection list to register — collections subscribe themselves when mounted.

## 5. Read and write

Handles mount synchronously and are cheap to call in render — the same `(entity, scope)` pair always returns the same instance. Reads are plain TanStack DB:

```tsx
import { useLiveQuery } from "@tanstack/react-db"

function TodoList({ projectId }: { projectId: string }) {
  const todos = todosCollection(projectId)
  const { data } = useLiveQuery((q) => q.from({ todo: todos }), [projectId])

  return (
    <ul>
      {data.map((todo) => (
        <li key={todo.id}>{todo.title}</li>
      ))}
    </ul>
  )
}
```

Writes go through the collection and apply instantly:

```ts
todos.insert({ id: crypto.randomUUID(), projectId, title: "Ship it" })
todos.update(todoId, (draft) => { draft.done = true })
todos.delete(todoId)
```

Mint ids on the client (`crypto.randomUUID()`). The server keeps them, so when your own write echoes back down the SSE stream it lands on the same key and is an idempotent no-op — no echo suppression needed anywhere.

Live queries can join across collections, aggregate, and filter — anything `useLiveQuery` supports. See [react](./react.md).

## 6. Manage lifecycle

Collections outlive components. Unmounting a React tree — or stopping sync — does not dispose them; their lifetime belongs to the runtime's registry:

```ts
Effect.runFork(runtime.registry.disposeScope(projectId)) // leaving a workspace
Effect.runFork(runtime.registry.disposeAllScoped())      // switching accounts, keep globals
Effect.runFork(runtime.registry.disposeAll())            // logout
runtime.dispose()                                        // app teardown
```

A disposed collection is gone from memory but its local table remains; the next mount hydrates from disk and replays whatever it missed from the journal. See [architecture](./architecture.md) for how that replay works.

## Current limitations

- **Writes are not offline-durable.** The local database persists synced server truth; an optimistic write made while offline is rolled into memory only and does not survive a reload. Online writes are safe — failure rolls them back visibly.
- **One mutation per transaction.** Reconciliation handles exactly one mutation; batched transactions fail loudly before any server call.
- **Resync is global.** Any resync event currently refetches all active collections, regardless of its target.

## Where to next

- [Collections](./collections.md) — the full `defineCollection` reference, writes in depth, registry operations.
- [Backend contract](./backend.md) — everything your server must guarantee.
- [Architecture](./architecture.md) — the broker, the journal, replay-on-mount, pruning, and resync.
- [`examples/pi-demo`](../examples/pi-demo) — all of the above, running.
