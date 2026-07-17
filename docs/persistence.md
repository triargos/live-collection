# Persistence

The client persistence base — how a `LiveCollection<T>` keeps its working set in local SQLite so a
fresh page load hydrates from disk instead of re-listing the server. You set this up exactly once, at
app startup, when you build the persistence **value** and hand it to `makeLiveRuntime`. After that it's
invisible: `defineCollection` closes over it, and every collection persists through it automatically.

This is the read-path durability layer. It does **not** make writes durable across offline restarts —
offline-durable writes are deferred (see [Not built](#not-built-and-why)). What it does guarantee is the
**A.3 gate**: hydrate-from-storage → no full re-list → catchup deltas persist via the sync source.

> **Frontend-only.** Everything here runs in the browser. The server contract (catchup, SSE, the
> squasher) lives in [`./protocol.md`](./protocol.md) and the reader's own backend in
> [`./backend.md`](./backend.md). See [`./architecture.md`](./architecture.md) for the runtime/registry/loop
> wiring this slots into.

---

## TL;DR — the composition

You build one persistence value at startup and thread it through `makeLiveRuntime`:

```ts
// prod, browser, once at startup — async (OPFS open is async)
const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "app.sqlite" })
const persistence = createBrowserWASQLitePersistence({ database }) // PersistedCollectionPersistence value
const runtime = makeLiveRuntime({ persistence, sync })
```

`defineCollection` then composes the persisted collection internally — you never write
`persistedCollectionOptions` yourself:

```
createCollection(
  persistedCollectionOptions({
    persistence,                       // ← the closed-over VALUE from the runtime
    id: serializeKey(key),             // ← stable per (entity, scope)
    schemaVersion: deriveSchemaVersion(schema),
    ...liveCollectionOptions({ getKey })   // ← our inner creator
  })
)
```

---

## `persistence` is a closed-over VALUE, not a tag

Persistence is **not** an Effect service tag. It is a plain `PersistedCollectionPersistence` value the
app constructs and passes to `makeLiveRuntime`, which stores it on the runtime
([`live-runtime.ts:24`](../packages/live-collection/src/runtime/live-runtime.ts), [`live-runtime.ts:53`](../packages/live-collection/src/runtime/live-runtime.ts)):

```ts
export interface LiveRuntime {
  readonly registry: CollectionRegistryShape
  readonly persistence: PersistedCollectionPersistence   // app-owned value, threaded into each make
  readonly forkSync: () => Fiber.RuntimeFiber<void>
  readonly forkDrain: (drain: Effect<void, never, SyncBroker>) => Fiber.RuntimeFiber<void>
  readonly dispose: () => void
}
```

`defineCollection`'s `make` reads it straight off the runtime as a captured value —
`persistence: runtime.persistence` — not via `yield* SomeTag`
([`define-collection.ts:136`](../packages/live-collection/src/registry/define-collection.ts)).

**Why a value and not a tag.** Mounting happens **during render** (inside `useLiveQuery`'s queryFn). The
mount path is `Effect.runSync(registry.getOrCreate({ key, make }))`, and `make` is
`Effect.sync(() => createCollection(...))` requiring only `Scope` (for `cleanup`), which the registry
discharges ([`define-collection.ts:131-151`](../packages/live-collection/src/registry/define-collection.ts)). Because
persistence is closed over rather than a context dependency, that Effect is `Effect<A, never, never>` and
`runSync` can never hit an async boundary. A persistence **tag** would force the mount path through async
layer resolution — illegal at render time. Persistence therefore stays a plain value; never introduce a
persistence service tag.

---

## Browser (prod): OPFS via `@tanstack/browser-db-sqlite-persistence`

The library ships **no** browser persistence builder — the app owns it. In production you build
the value from the official package, pinned **`0.1.11`**:

```ts
import {
  openBrowserWASQLiteOPFSDatabase,
  createBrowserWASQLitePersistence,
} from "@tanstack/browser-db-sqlite-persistence"

const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "app.sqlite" }) // async
const persistence = createBrowserWASQLitePersistence({ database })                     // sync, → value
```

- `openBrowserWASQLiteOPFSDatabase({ databaseName })` is **async** (opens an OPFS-backed SQLite database)
  and you call it **once** at startup. The result feeds `createBrowserWASQLitePersistence({ database })`,
  which returns a `PersistedCollectionPersistence`.
- The browser SQLite engine is **`@journeyapps/wa-sqlite`** (`^1.4.1`), via OPFSCoopSyncVFS in a bundled
  worker — **not** `@sqlite.org/sqlite-wasm`. It is a peer dependency of the persistence package.
- This package also **re-exports** `persistedCollectionOptions` and `PersistedCollectionPersistence`, so
  in a browser app you can import them from here rather than the core package below.

> **Vite gotcha.** The persistence package bundles a worker + wasm. Your `vite.config` must
> `optimizeDeps.exclude` **both** `@tanstack/browser-db-sqlite-persistence` and `@journeyapps/wa-sqlite`,
> or the worker/wasm URLs break.

> **`createOpfsSQLitePersistence` does not exist.** The real API is the
> `openBrowserWASQLiteOPFSDatabase` → `createBrowserWASQLitePersistence` pair above.

### Per-tab databases

If you want two tabs to behave as independent clients (the playground does — each tab is its own sync
client with its own watermark), give each tab a **distinct `databaseName`**. A single origin-shared
default DB lets tabs clobber each other's persisted state. See
[`playground.ts:41-43`](../examples/playground/src/live/playground.ts), which derives `dbName` from a
per-tab session.

---

## Node (test only): hand-ported SQLite persistence

`persistedCollectionOptions` and `PersistedCollectionPersistence` come from
**`@tanstack/db-sqlite-persistence-core`**, *not* `@tanstack/db` core — core only exports
`createCollection`. That is the import the library itself uses
([`define-collection.ts:8`](../packages/live-collection/src/registry/define-collection.ts),
[`live-runtime.ts:2`](../packages/live-collection/src/runtime/live-runtime.ts)).

Node cannot run OPFS, so the test suite builds its persistence value over a node SQLite driver. That node
builder (`examples/playground/test`'s `makeSqlitePersistence`) is a **hand-port of the official
`createBrowserWASQLitePersistence`** — same adapter-cache + `resolvePersistenceForCollection` assembly,
swapping the engine. It is test infra only; never ship it as a browser builder.

> **Verify the alpha surface against your installed version.** `@tanstack/db` is pinned **`0.6.7`
> (alpha)** and the browser persistence adapter at **`0.1.11`**. The persistence surface shifts between
> alpha releases. Before relying on any signature here, `git grep` the symbol and read the export in your
> installed package — do not assume the shapes are stable.

---

## `liveCollectionOptions({ getKey })` — the inner creator

`liveCollectionOptions` is the **inner** options creator: the live-sync analogue of TanStack's
`queryCollectionOptions`. You spread its result into `persistedCollectionOptions`, exactly as the
TanStack docs spread `queryCollectionOptions`. It contributes the live-sync fields; `persistence`,
`schemaVersion`, and `id` are added at the outer level by `defineCollection`'s `make`.

Its single param is an object (one own field today, kept as an object for extension):

```ts
export const liveCollectionOptions = <T extends object>(config: {
  readonly getKey: (entity: T) => ModelId
}): LiveCollectionOptions<T>
```

The fields it sets ([`live-collection-options.ts:13-20`](../packages/live-collection/src/persistence/live-collection-options.ts)):

| field | value | why |
|---|---|---|
| `getKey` | your `(entity) => ModelId` | branded key extractor |
| `gcTime` | `Infinity` | the **registry** is the sole GC — never let TanStack evict |
| `syncMode` | `"eager"` | load the persisted base on start, not query-driven |
| `startSync` | `true` | start sync on mount → session captured + hydration runs |
| `utils` | `SyncWrite<T>` | `writeSynced` / `deleteSynced`, hosted in `utils` |
| `sync` | a **network-free** `SyncConfig` | installs the session holder, then `markReady` |

The `sync` it returns does **no network I/O**. It only installs the [`SyncSession`](#the-sync-session-holder)
behind `utils.writeSynced` / `deleteSynced` and signals ready. Server truth reaches the store
through the collection's **broker drain** writing to `utils`, never through this `sync`. That is the whole point of the A.3
gate: a cold mount is fed by **OPFS only**, because the collection's own `sync` never lists.

It is synchronous by design — `createCollection` (its caller) is sync, so the one-shot session `Deferred`
is built with `Effect.runSync`, a pure step with no async boundary
([`live-collection-options.ts:34`](../packages/live-collection/src/persistence/live-collection-options.ts)).

### The sync-session holder

`utils.writeSynced` / `deleteSynced` are constructed at config time, but the `begin/write/commit` trio they
need only exists once TanStack calls `sync()`. A one-shot `Deferred` bridges the two: a write issued
before the session is provided simply **waits** ([`sync-session.ts:23-35`](../packages/live-collection/src/persistence/sync-session.ts)).
This is sound only because `gcTime: Infinity` keeps the collection alive, so `sync()` runs exactly once and
never restarts.

`SyncWrite<T>` is the synced-store write path — distinct from the optimistic-mutation path the UI writes
through. Synced writes reflect confirmed server truth and are never rolled back
([`sync-write.ts`](../packages/live-collection/src/dispatch/sync-write.ts)):

```ts
export interface SyncWrite<T> {
  readonly writeSynced: (entity: T) => Effect.Effect<void>   // upsert; idempotent
  readonly deleteSynced: (id: ModelId) => Effect.Effect<void> // no-op if absent
  // + a structural-only index so utils is a Record<string, Fn> for useLiveQuery joins
}
```

---

## `deriveSchemaVersion(schema)` — automatic local-base reset

`persistedCollectionOptions` takes a numeric `schemaVersion`; when it changes, the local base is dumped and
rebuilt. We **derive** it from your Effect Schema so a model change resets the base automatically — no
manual version to bump or forget:

```ts
export const deriveSchemaVersion = (schema: Schema.Schema.Any): number
```

- The hash input is `String(schema.ast)` — the schema's full structural type string, which folds in **types
  and brands** (not just field names). So `name: string → number` (same field name) still bumps the version.
  That matters because we **trust** the local base: a missed type change would silently keep
  stale-typed rows ([`schema-version.ts:8-12`](../packages/live-collection/src/persistence/schema-version.ts)).
- It is **FNV-1a 32-bit → uint32**, the same family TanStack uses for table names
  ([`schema-version.ts:19-27`](../packages/live-collection/src/persistence/schema-version.ts)).
- **Trade-off:** if Effect's AST-string format shifts between versions, the hash changes and you get a
  spurious reset on upgrade — a harmless refetch, deliberately chosen over a *missed* change (a real bug).

You don't call this directly in app code; `defineCollection` calls it for you with the `schema` you pass
([`define-collection.ts:110`](../packages/live-collection/src/registry/define-collection.ts)).

---

## The A.3 three-step gate

The persistence design is a hard gate: persistence is not rolled out across entities until the
three-step flow is verified against the alpha. The flow:

1. **Hydrate-from-storage.** A mount reads its rows from the local SQLite base (`syncMode: "eager"`).
2. **No full re-list.** The collection's own `sync` is network-free and **never lists** — only OPFS feeds a
   cold mount.
3. **Catchup deltas persist via the sync source.** Server truth arrives through the broker drain calling
   `utils.writeSynced` / `deleteSynced`, and those synced writes are persisted to OPFS.

Node cannot run OPFS, so the **only** proof over real OPFS is a browser test. The smoke
([`opfs-smoke.browser.test.ts`](../examples/playground/test/opfs-smoke.browser.test.ts)) mirrors the node
gate verbatim, swapping only the persistence builder for `createBrowserWASQLitePersistence` over an OPFS
database, and asserts:

- **rehydration**: a fresh mount sees a row a previous mount persisted
  (`reloadUntil(... c.has(k("r1")))`), and
- **delta accumulation**: catchup-style deltas pile onto the OPFS base, latest value wins
  (`r1: "a" → "b"`, `r2: "z"`).

It runs in **vitest browser mode** (Playwright Chromium — `pnpm exec playwright install chromium` first).
Synced writes persist fire-and-forget, so the test polls a fresh remount (`reloadUntil`) until the predicate
holds rather than awaiting a write promise — the production read path is exactly this: mount, hydrate from
OPFS, observe deltas land.

The worked production composition lives in
[`playground.ts:40-50`](../examples/playground/src/live/playground.ts): open the per-tab OPFS database,
build the persistence value, merge it into the runtime, and let `defineCollection` close over it.

---

## Not built (and why)

These are deferred, not present — do not wire them as if they exist:

- **Offline-durable writes.** Persistence makes the **read** base durable; an optimistic write made while
  offline is not replayed on restart. See [`./optimistic-writes.md`](./optimistic-writes.md) for the online optimistic path that
  *is* built (A.10).
- **Registry eviction backstop / unmounted-workspace policy (A.11).** `gcTime: Infinity` means the registry
  is the sole GC; an automatic eviction backstop for long-lived unmounted scopes is not built.
- **Throttled watermark flush** and **per-target resync** — deferred in the transport tier; see
  [`./protocol.md`](./protocol.md) and [`./architecture.md`](./architecture.md).

---

## See also

- [`./architecture.md`](./architecture.md) — the runtime, lifetime registry, and broker this plugs into.
- [`./protocol.md`](./protocol.md) — the wire contract decoded by transport and collection drains.
- [`./backend.md`](./backend.md) — your responsibility; the server side of the read path.
- [`./optimistic-writes.md`](./optimistic-writes.md) — the optimistic write path (A.10) that reconciles through `writeSynced`.
