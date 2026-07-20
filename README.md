# `@triargos/live-collection`

A **frontend-only**, Effect-native live-sync engine built on [TanStack DB](https://tanstack.com/db). The hero type is `LiveCollection<T>` — a *native* TanStack collection that persists locally (SQLite/OPFS), hydrates from disk on reload, and stays live against your backend over SSE + catchup.

**How you use it:** you define one collection per model, point it at your backend's read path (a `listFn`) and optionally its write path (optimistic `onInsert`/`onDelete`/`onUpdate` handlers), then start one shared broker near your app root. Each mounted collection subscribes itself. Reads are plain `useLiveQuery`; the engine owns persistence, catchup, the durable cursor, and replay-on-mount.

> **The backend is yours.** This library ships no server. It speaks a wire contract — the SSE stream, the `/catchup` request/response, and the squasher fold — defined in `@triargos/live-collection-protocol`. You implement the routes, auth, and permission resolution. See [`docs/backend.md`](docs/backend.md) and [`docs/protocol.md`](docs/protocol.md).

---

## The three packages

The dependency DAG is acyclic: `protocol → live-collection → react`.

| Package | What it is | You import it when… |
|---|---|---|
| **`@triargos/live-collection-protocol`** | The shared **contract kit** — pure, no I/O. `SyncEvent`/`HydratedSyncEvent` schemas, the sync-group grammar, the squasher fold, the `/catchup` request/response schemas, and the branded `ModelId`. The backend implements against it. | …you build the backend, or need the branded `ModelId` / wire schemas in app code. |
| **`@triargos/live-collection`** | The frontend engine. `defineCollection`, `makeLiveRuntime`, the SSE transport, catchup, persistence, the durable `SyncJournal`, and the `LiveCollection<T>` hero type. | …always, in the client app. |
| **`@triargos/live-collection-react`** | One React-specific piece: `useLiveSync`, which starts broker ingest on mount and interrupts it on unmount. Reads use `@tanstack/react-db` directly. | …your app is React. |

Core is framework-neutral; `defineCollection` returns a native TanStack collection, so non-React apps drive it through `@tanstack/db` directly.

---

## Install

```bash
pnpm add @triargos/live-collection @triargos/live-collection-protocol
pnpm add @triargos/live-collection-react              # React apps only
```

Peer/runtime deps the engine builds on (pinned deliberately — the persistence surface is alpha and shifts):

```bash
pnpm add effect @effect/platform
pnpm add @tanstack/db@0.6.7                           # pinned exactly; alpha
pnpm add @tanstack/db-sqlite-persistence-core@0.1.11  # persistedCollectionOptions lives here, NOT @tanstack/db
pnpm add @tanstack/browser-db-sqlite-persistence@0.1.11  # browser OPFS persistence
pnpm add @tanstack/react-db                           # React reads (useLiveQuery)
```

> **Verify versions against what you have installed.** `@tanstack/db` is pinned at `0.6.7` (alpha) and the persistence adapter at `0.1.11`; both surfaces move between releases. Bump them deliberately, not via caret.

---

## Quick start

A worked slice (drawn from [`examples/playground`](examples/playground/)). Two collections: one *scoped* (per `orgId`), one *global*.

**1. Build the runtime once at startup.** `persistence` is your app-owned value; `sync` provides transport, catchup, cursor, and durable event log services.

```ts
import { makeLiveRuntime, SyncJournal } from "@triargos/live-collection"
import { Layer } from "effect"
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"

const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "app" })
const persistence = createBrowserWASQLitePersistence({ database })

const runtime = makeLiveRuntime({
  persistence,
  sync: Layer.merge(myTransportLayer, SyncJournal.layer({ databaseName: "app-eventlog" })),
})
```

**2. Define one collection per model.** The model name is written once; the handle is runtime-bound and registry-backed. `scopeOf` present ⇒ scoped (`webhooks(orgId)`); absent ⇒ global (`projects()`).

```ts
import { defineCollection } from "@triargos/live-collection"
import { Effect } from "effect"

const webhooks = defineCollection({
  runtime,
  services,                          // ManagedRuntime that discharges your handlers' R (A.10)
  entity: "Webhook",                 // the wire model name
  schema: Webhook,
  getKey: (w) => ModelId.make(w.id), // boundary mapper: raw id → branded ModelId (the one place .make is allowed)
  scopeOf: (w) => w.orgId,           // one instance per orgId
  listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)), // cold/resync snapshot
  onInsert: ({ transaction }) => // call the server, RETURN the confirmed row — the library reconciles it
    Effect.flatMap(WebhookApi, (api) => api.create(transaction.mutations[0]!.modified)),
})
```

**3. Start sync once, then read with `useLiveQuery`.** `useLiveSync` starts broker ingest for the app's lifetime; collections subscribe when mounted.

```tsx
import { useLiveSync } from "@triargos/live-collection-react"
import { useLiveQuery } from "@tanstack/react-db"

function App() {
  useLiveSync(runtime) // mount once near the root
  return <Webhooks orgId="org-1" />
}

function Webhooks({ orgId }: { orgId: string }) {
  const coll = webhooks(orgId)                       // mounts/returns the per-scope native collection (cached)
  const { data } = useLiveQuery(() => coll, [orgId])
  return (
    <button onClick={() => coll.insert({ id: crypto.randomUUID(), orgId, url: "…" })}>
      {data?.length ?? 0} webhooks — add
    </button>
  )
}
```

That's the whole loop: `coll.insert` with a client-minted id shows instantly (optimistic) → your `onInsert` Effect calls the server and returns the confirmed row → the library `writeSynced`s it (Model B) → the SSE echo of the same row is an idempotent `writeSynced` → persisted to OPFS → survives reload (hydrate-from-disk, no full re-list).

---

## How it fits together

- **`makeLiveRuntime`** has a synchronous mount surface (`registry` + `persistence`) and asynchronous `forkSync`/collection-drain fibers. `useLiveSync` starts broker ingest.
- **The cursor** (`LastSyncId`) is a durable, global record of the newest syncId this client has ingested; it gates catchup — *not* the framework's `staleTime` (which resets on reload). Catchup deltas write through the **synced-store** path (`writeSynced`/`deleteSynced`), never the optimistic path.
- **Replay-on-mount** (`SyncJournal` + `SyncBroker`): a durable IndexedDB log lets a freshly mounted collection receive replay and live tail as one stream. An in-band `Snapshot` rebuilds an untrusted base.
- **The squasher** (in `protocol`) is a pure fold both ends rely on: a client catching up from any `syncId` converges to the same state.
- **Scope, not backend, is the lever for large data.** Per-workspace collections (`<entity>:<scope>`) keep the working set small; both persistence backends hold a collection's working set in memory.

### Not built yet (and why)

Mentioned so you don't reach for them: automatic unmounted-workspace eviction, offline-durable writes, a registry eviction backstop, and target-aware resync. Last-applied marks are already batched; resync currently snapshots every active subscriber in place.

---

## Docs

- [`docs/protocol.md`](docs/protocol.md) — the wire contract: `SyncEvent`/`HydratedSyncEvent`, sync-group grammar, the squasher, resync targets.
- [`docs/backend.md`](docs/backend.md) — the contract your server must satisfy: the `/catchup` and SSE endpoints and the invariants the client depends on.
- [`docs/architecture.md`](docs/architecture.md) — `makeLiveRuntime`, the two execution surfaces, the loop, persistence, and the sync cursor in depth.
- [`docs/collections.md`](docs/collections.md) — `defineCollection`, scoping, the optimistic write path, and `LiveCollection<T>`.
- [`docs/react.md`](docs/react.md) — `useLiveSync` lifecycle and reading with `useLiveQuery`.

> Several `docs/*.md` are written by companion passes; if a link 404s, that page hasn't landed yet.
