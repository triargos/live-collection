# Persistence

Every collection stores its rows in local SQLite. A page reload hydrates from disk — no re-fetch — and sync resumes with deltas from the stored cursor. You set persistence up once, when building the runtime; after that it's invisible.

## Setup (browser)

The library takes a persistence **value**, built from TanStack DB's browser persistence package:

```ts
import {
  createBrowserWASQLitePersistence,
  openBrowserWASQLiteOPFSDatabase,
} from "@tanstack/browser-db-sqlite-persistence"
import { makeLiveRuntime } from "@triargos/live-collection"

const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "myapp" })
const persistence = createBrowserWASQLitePersistence({ database })

const runtime = makeLiveRuntime({ persistence, sync })
```

Open the database once at startup (it's async — the database lives in [OPFS](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system)); every collection then persists through it automatically. `defineCollection` derives a stable table id per `(entity, scope)` and wires the persisted collection for you — you never call `persistedCollectionOptions` yourself.

### Vite configuration

The persistence package bundles a worker and a wasm SQLite engine. Exclude both from dependency pre-bundling or the worker URLs break:

```ts
// vite.config.ts
export default defineConfig({
  optimizeDeps: {
    exclude: ["@tanstack/browser-db-sqlite-persistence", "@journeyapps/wa-sqlite"],
  },
})
```

## What persistence guarantees

- **Hydrate from disk.** A mounted collection loads its saved rows immediately; `listFn` only runs when there is no trustworthy local base (first ever mount, schema change, resync).
- **Deltas persist.** Every synced write — live events, catchup, replay, confirmed optimistic writes — lands in SQLite, so the base is always as fresh as the last event applied.
- **Writes are not offline-durable.** The database holds *synced* server truth. An optimistic write made while offline exists only in memory and will not survive a reload. (A durable offline mutation queue is a possible future addition, not a current feature.)

## Schema versioning

The persisted table's schema version is **derived automatically** from your Effect schema — a structural hash covering field names, types, and brands. Change the schema and the next start dumps and rebuilds that table from the server; there is no version number to bump or forget. The worst failure mode is a spurious rebuild (a harmless refetch), never a silently stale table.

## Multiple tabs

Tabs sharing one `databaseName` share one persisted state — fine when they're the same logical client. If you want tabs to act as independent clients (each with its own cursor and journal), give each a distinct `databaseName` for both the SQLite database and the `SyncJournal`.

## Outside the browser

`persistedCollectionOptions` and the `PersistedCollectionPersistence` type come from `@tanstack/db-sqlite-persistence-core`; the browser package builds on it. In Node (e.g. tests) you can assemble a persistence value over any SQLite driver against the same core interface — the library only sees the value.

Note that `@tanstack/db` is pinned exactly (currently `0.6.7`) because the persistence integration is alpha; bump it deliberately, together with the persistence packages.

## See also

- [Getting started](./getting-started.md) — the runtime setup this slots into.
- [Architecture](./architecture.md) — how synced writes reach the store, and why scope (not persistence) bounds memory.
