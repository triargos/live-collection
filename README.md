# `@triargos/live-collection`

A **frontend-only**, Effect-native live-sync engine built on [TanStack DB](https://tanstack.com/db). The hero type is `LiveCollection<T>` — a *native* TanStack collection that persists locally (SQLite/OPFS), hydrates from disk on reload, and stays live against your backend over SSE + catchup.

**How you use it:** you define one collection per model, point it at your backend's read path (a `listFn`) and optionally its write path (optimistic `onInsert`/`onDelete`/`onUpdate` handlers), then mount one sync loop near your app root. Reads are plain `useLiveQuery`; the engine owns persistence, catchup, the durable cursor (the **watermark**), and replay-on-mount.

> **The backend is yours.** This library ships no server. It speaks a wire contract — the SSE stream, the `/catchup` request/response, and the squasher fold — defined in `@triargos/live-collection-protocol`. You implement the routes, auth, and permission resolution. See [`docs/backend.md`](docs/backend.md) and [`docs/protocol.md`](docs/protocol.md).

---

## The three packages

The dependency DAG is acyclic: `protocol → live-collection → react`.

| Package | What it is | You import it when… |
|---|---|---|
| **`@triargos/live-collection-protocol`** | The shared **contract kit** — pure, no I/O. `SyncEvent`/`HydratedSyncEvent` schemas, the sync-group grammar, the squasher fold, the `/catchup` request/response schemas, and the branded `ModelId`. The backend implements against it. | …you build the backend, or need the branded `ModelId` / wire schemas in app code. |
| **`@triargos/live-collection`** | The frontend engine. `defineCollection`, `makeLiveRuntime`, the SSE transport, catchup, persistence, the durable `EventLogStore`, and the `LiveCollection<T>` hero type. | …always, in the client app. |
| **`@triargos/live-collection-react`** | One React-specific piece: `useLiveSync`, which forks the sync loop on mount and interrupts it on unmount. Reads use `@tanstack/react-db`'s `useLiveQuery` directly — this package does **not** wrap it. | …your app is React. |

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

**1. Build the runtime once at startup.** `persistence` is your app-owned value; `loop` is the transport/catchup/cursor layer (plus the durable event log); `onResync` is the live-resync action.

```ts
import { makeLiveRuntime, reloadWindow, EventLogStore } from "@triargos/live-collection"
import { Layer } from "effect"
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"

const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "app" })
const persistence = createBrowserWASQLitePersistence({ database })

const runtime = makeLiveRuntime({
  persistence,
  loop: Layer.merge(myTransportLayer, EventLogStore.layer({ databaseName: "app-eventlog" })),
  onResync: reloadWindow, // prod default: reload the whole app on a resync sentinel
})
```

**2. Define one collection per model.** The model name is written once; the handle is runtime-bound and registry-backed. `scopeOf` present ⇒ scoped (`webhooks(orgId)`); absent ⇒ global (`projects()`).

```ts
import { defineCollection, type SyncModels } from "@triargos/live-collection"
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

const models: SyncModels = [webhooks] // the models the loop drives; wire name = each handle's `entity`
```

**3. Mount the loop once, then read with `useLiveQuery`.** `useLiveSync` forks the SSE/catchup/cursor loop for the app's lifetime; reads are plain `@tanstack/react-db`.

```tsx
import { useLiveSync } from "@triargos/live-collection-react"
import { useLiveQuery } from "@tanstack/react-db"

function App() {
  useLiveSync(runtime, models) // mount ONCE near the root; models is captured at mount — keep it stable
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

- **`makeLiveRuntime`** has two surfaces: a **sync** mount surface (`registry` + `persistence`) the collection handles run against during render, and an **async** `forkLoop` surface that runs catchup/cursor/tail off the render path. `useLiveSync` drives the loop.
- **The watermark** (`LastSyncId`) is a durable, global cursor that gates catchup — *not* the framework's `staleTime` (which resets on reload). Catchup deltas write through the **synced-store** path (`writeSynced`/`deleteSynced`), never the optimistic path.
- **Replay-on-mount** (`EventLogStore`): a durable IndexedDB log of confirmed events lets a freshly mounted collection replay locally before the network catches up. Decision logic lives in `mount-decision` (replay / skip / bootstrap).
- **The squasher** (in `protocol`) is a pure fold both ends rely on: a client catching up from any `syncId` converges to the same state.
- **Scope, not backend, is the lever for large data.** Per-workspace collections (`<entity>:<scope>`) keep the working set small; both persistence backends hold a collection's working set in memory.

### Not built yet (and why)

Mentioned so you don't reach for them: unmounted-workspace eviction policy, offline-durable writes (writes today require online for the `writeSynced` confirm), throttled watermark flush, a registry eviction backstop, and per-target resync. Today a resync sentinel triggers a full `onResync` (default: window reload).

---

## Docs

- [`docs/protocol.md`](docs/protocol.md) — the wire contract: `SyncEvent`/`HydratedSyncEvent`, sync-group grammar, the squasher, resync sentinels.
- [`docs/backend.md`](docs/backend.md) — what **you** must build: `/catchup`, the SSE `/sync` stream, dispatch, permission resolution.
- [`docs/architecture.md`](docs/architecture.md) — `makeLiveRuntime`, the two execution surfaces, the loop, persistence, and the watermark in depth.
- [`docs/collections.md`](docs/collections.md) — `defineCollection`, scoping, the optimistic write path, and `LiveCollection<T>`.
- [`docs/react.md`](docs/react.md) — `useLiveSync` lifecycle and reading with `useLiveQuery`.

> Several `docs/*.md` are written by companion passes; if a link 404s, that page hasn't landed yet.
