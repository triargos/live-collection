# Bucket A — the collection factory & persistence base

> **Status: LOCKED (design-first, Phase 2 done; Phase 3 next).** Signed off 2026-06-03. Not yet
> implemented. Every signature below is interface + wiring only — no bodies — and every framework
> claim is verified against `@tanstack/db@0.6.7` + the `0.1.11` persistence adapters (file/line refs
> inline). This designs the one seam decision 3 calls *the only seam*: the per-entity collection
> factory (`effectCollectionOptions`) and the value it returns (`LiveCollection<T>`), behind which
> **all** of TanStack DB hides. Scope of this pass (agreed): the **read/persist path** — the return
> surface, the factory input, and the persistence-base inner seam the A.3 gate validates. The
> **optimistic-mutation handlers** and the **catchup / SSE transport** seams are named here as
> dependencies but designed in their own passes. On sign-off → Phase 3 (tests against this interface,
> red→green). Derived from [`live-sync-system.md`](../../live-sync-system.md) §14, §22, §A; constrained
> by the protocol's [`DESIGN.md`](../protocol/DESIGN.md) DEC-6 / DEC-11 / DEC-12.

## What this is

The typed entry point an app writes once per entity, co-located with its aggregate. It knows the
entity's key format and hides the TanStack alpha entirely: `createCollection`,
`persistedCollectionOptions`, `createBrowserWASQLitePersistence`, the `sync` begin/write/commit
loop, catchup, and the durable cursor. Its return value is the hero **`LiveCollection<T>`** — a
TanStack `Collection` that the UI reads via `useLiveQuery` and the `SyncDispatcher` writes via its
`utils`.

**Inclusion test.** An item belongs in this bucket iff it is *frontend* plumbing that touches
TanStack DB or the persisted base. Anything pure-contract is protocol (Bucket 0); anything that
needs a server is the backend (Bucket C).

**The deletion test.** Delete the factory and every entity re-wires `createCollection` +
`persistedCollectionOptions` + the sync loop + the SyncWrite adapter by hand — the alpha surface
leaks into N entity files. Complexity reappears N× → the factory is deep, and earns the seam.

## Module layout

```
packages/live-collection/src/
  persistence/
    live-collection.ts      §1  LiveCollection<T> — the return type (a Collection, utils-hosted SyncWrite)
    effect-collection.ts    §2  effectCollectionOptions(...) — the factory
    schema-version.ts       §2  deriveSchemaVersion(schema) — FNV-1a of String(schema.ast) → uint32
    persistence-base.ts     §3  PersistenceBase tag + layerSqliteDriver (core-only imports — node-safe)
    persistence-base-opfs.ts §3 PersistenceBase.layer (browser OPFS) — the ONLY module importing wa-sqlite
    sync-session.ts         §4  the holder that lets utils.writeSynced reach the sync loop's write()
  dispatch/                     (built) — §1 relocates its SyncWrite access to `.utils`
  registry/                     (built) — make returns LiveCollection<T>; teardown via addFinalizer
```

> **Platform split (Phase-3 structural rule).** The OPFS layer pulls `@tanstack/browser-db-sqlite-persistence`
> (peer: wa-sqlite, browser-only). It must live in its **own module** so the node:sqlite gate test —
> importing the `PersistenceBase` tag + `layerSqliteDriver` (core-only) — never loads wa-sqlite, and the
> browser bundle never loads `node:sqlite`. The shared tag stays import-clean; each layer carries its
> platform deps. (Same reason the playground, not the test, owns the OPFS wiring.)

Dependencies designed in later passes, referenced here only as tags:
`BootstrapSource` (full-list refetch), `CatchupClient`, `LastSyncIdStore`, `SyncTransport`.
The source strategy is **snapshot-and-tail**, not catchup-from-zero — see DEC-A12.

---

## §1 — `LiveCollection<T>` (the return surface)

```typescript
import type { Collection } from "@tanstack/db"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "../dispatch/sync-write.js"

/**
 * The hero type. NOT a wrapper — it *is* a TanStack `Collection` whose key is `ModelId` and whose
 * `utils` host the server-truth write path. The UI reads it directly (`useLiveQuery(collection)`);
 * the `SyncDispatcher` reaches the synced-write path through `collection.utils`.
 *
 * `Collection<T, TKey, TUtils, TSchema, TInsertInput>`:
 *   - TKey   = ModelId      (branded string; assignable to TanStack's `string | number`)
 *   - TUtils = SyncWrite<T> (writeSynced / deleteSynced — hosted in utils, not bolted alongside)
 *   - TSchema = never       (schema-less overload — see DEC-A1: data is already decoded+branded
 *                            at the dispatch seam, so TanStack does no validation of its own)
 */
export type LiveCollection<T extends object> = Collection<T, ModelId, SyncWrite<T>, never, T>
```

**Relocation of the dispatcher's `SyncWrite` access** (the only change to shipped code):

```typescript
// sync-dispatcher.ts — SyncWrite moves from the top level into `.utils`:
//   Delete:  registry.getByEntity<{ utils: SyncWrite<unknown> }>(name) → c.utils.deleteSynced(id)
//   Ins/Upd: registry.getById<{ utils: SyncWrite<T> }>(key)           → c.utils.writeSynced(data)
// One hop deeper; behavior identical. Lands test-backed in this design, not as silent drift.
```

**Open decisions (grilling §1):**

- **DEC-A1 — schema-less TanStack collection.** The factory does *not* hand its Effect schema to
  TanStack; it uses the `schema?: never` overload. Decoding already happens once at the dispatch
  seam (`sync-dispatcher.ts:71`, `descriptor.schema`), so `writeSynced(data: T)` receives a decoded,
  branded `T`. *Recommend: yes* — avoids the Effect-Schema ↔ StandardSchema bridge, and the gate
  needs no second validation.
- **DEC-A2 — the SQLite↔memory boundary is internal to the library; there is no decode for us to
  do *or* skip.** Verified in `@tanstack/db-sqlite-persistence-core` `persisted.js`
  (`createWrappedSyncConfig`): `persistedCollectionOptions` wraps our `sync` and (a) hydrates the
  in-memory store from `adapter.loadSubset` itself on startup, before our sync's `markReady` fires,
  and (b) intercepts our `write()` calls, persisting them via `applyCommittedTx` *and* forwarding
  them to the in-memory store. So rehydrated rows never enter *our* code as `unknown` — TanStack
  reads back its own JSON, its responsibility, gated by `schemaVersion`. This is **not** a carve-out
  from "validate at boundaries": our only `unknown → T` boundary is the dispatch seam, which we
  decode. (The registry's "an in-process object we put here ourselves" note, now extended across a
  reload by the library, not by us.)
- **TKey = `ModelId`.** Branded string → assignable to `string | number`; `getKey: (e) => e.id`.
  *Recommend: yes.*
- **Surface = nothing beyond `Collection` + `utils: SyncWrite<T>`.** Mounting is `MountRef`'s job;
  optimistic mutation is a later slice. *Recommend: keep it bare.*

---

## §2 — `effectCollectionOptions(...)` (the factory)

The body of `defineCollection`'s `make`. Returns the built `LiveCollection<T>` as an Effect that
registers its own teardown (so `R` carries `Scope`) and requires the persistence base. In *this*
pass there is no network: the `sync` config hydrates (internal, §1/DEC-A2) and `markReady`s; the
catchup/SSE wiring is the transport pass (§5).

```typescript
import { Effect, type Schema, type Scope } from "effect"
import type { ModelId } from "@triargos/live-collection-protocol"
import { PersistenceBase } from "./persistence-base.js"
import { deriveSchemaVersion } from "./schema-version.js"
import type { LiveCollection } from "./live-collection.js"

export const effectCollectionOptions = <T extends object>(args: {
  /** Stable, unique-per-(entity,scope) id for the SQLite table + TanStack collection id.
   *  INJECTED by defineCollection (DEC-A3) — the app never hand-builds it. */
  readonly collectionId: string
  /** The entity schema — used ONLY to derive schemaVersion (DEC-A6) and infer T; never handed to
   *  TanStack, performs no validation (DEC-A4 amended). schemaVersion = deriveSchemaVersion(schema). */
  readonly schema: Schema.Schema<T, any, never>
  /** T → its row key. */
  readonly getKey: (entity: T) => ModelId
}): Effect.Effect<LiveCollection<T>, never, PersistenceBase | Scope.Scope> => Effect.gen(/* …§4 */)
```

Call site (what an app writes once per entity):

```typescript
export const webhookCollection = defineCollection({
  entity:  "Webhook",
  scopeOf: (orgId: OrgId) => orgId,
  // mount carries the injected collectionId; args is the scoped arg. T infers from `schema`.
  make: ({ collectionId }) => effectCollectionOptions({
    collectionId,
    schema: Webhook,
    getKey: (w) => w.id,
  }),
})
```

**Open decisions (grilling §2):**

- **DEC-A3 — `defineCollection` injects `collectionId`; the app never builds it.** The factory needs
  a stable id for the persistence table (`createPersistedTableName`). It must equal one value per
  `(entity, scope)` across mounts. `defineCollection` already mints the `CollectionKey`, so it
  passes the id into `make` via a `mount` object: `make: (mount: { collectionId; args }) => Effect`.
  **Resolved:** id = `serializeKey(key)`. No new string grammar (decision 9), and the raw form is
  never used as an identifier — TanStack's `createPersistedTableName` FNV-1a-hashes the
  `collectionId` into a safe `c_<base32>_<len>` table name, so **arbitrary org ids need no
  sanitizing, lowercasing, or hand-hashing**. **Cost:** changes `define-collection.ts` (`make`
  gains the `mount` param, both overloads); test-backed.
- **DEC-A4 — the factory takes the schema *only* to derive `schemaVersion` + infer `T`, never to
  validate** *(amended in Phase 3)*. Originally schema-free (validation happens at the dispatch seam,
  DEC-A1). Reopened so `schemaVersion` is **auto-derived** from the schema instead of a manual number
  the app must remember to bump (the hosting smell): `schemaVersion = deriveSchemaVersion(String(
  schema.ast))` — FNV-1a over the schema's structural type string, which folds in field **names,
  types, and brands** (a same-named field whose type changed still resets — closing hosting's
  names-only blind spot, which matters under DEC-A2's "trust the local base"). The schema is **not**
  forwarded to `createCollection` (still schema-less, DEC-A1) and runs no validation. Bonus: `T`
  infers from `schema`, so call sites drop the explicit generic. Trade-off: an Effect AST-format
  change could cause a spurious reset on upgrade — a harmless refetch, the right side vs. a *missed*
  change. Verified by `deriveSchemaVersion` behavior tests (same⇒equal; add/remove/rename/retype/
  rebrand⇒different).
- **DEC-A5 — this pass's `sync` is network-free.** It installs the sync-session holder (§4) and
  `markReady`s after the library's internal hydration; catchup + SSE land in the transport pass.
  Keeps the A.3 gate (persist → reload → rehydrate) provable with zero backend. *Recommend: yes.*
- **DEC-A6 — schema evolution = dump-and-rebuild, native, no table bookkeeping of our own.** Pass
  `schemaVersion` (bump on entity-shape change) + `schemaMismatchPolicy: 'sync-present-reset'` (the
  default). On mismatch the lib resets the table *in place* (`collection_reset_epoch++`, same hashed
  name) and re-syncs — so a schema change drops the local copy and a full refetch rebuilds it, with
  **no migrations to write**. This deliberately adopts the library mechanism over porting the old
  `hosting` `collection-manager` bookkeeping (hot-map of `tableHash`, `staleTables` cleanup, batched
  migration window) — that whole-table per-collection bookkeeping is a CLAUDE.md anti-reference. The
  only cache is `CollectionRegistry` (mounts are canonical); reset-in-place means no orphaned tables
  to sweep, so no hot-table of names is needed. *Recommend: yes.*

---

## §3 — `PersistenceBase` (the inner seam)

The shared, app-wide persistence every collection reuses (each passes its own `collectionId`; they
share one DB). Injectable via a tag — `PersistenceBase` — with two layers. The seam sits at the
`PersistedCollectionPersistence` level, **not** the raw driver: `createBrowserWASQLitePersistence`
builds its driver internally from `new BrowserWASQLiteDriver(db)`, and `BrowserWASQLiteDriver` is an
**internal** class (not re-exported; the package `exports` map blocks deep imports), so prod can
never hand us a raw `SQLiteDriver`. The finest *public* seam is the persistence object — so that's
the tag. The node test layer replicates the *full* builder logic over the injected driver — adapter
cache + `resolvePersistenceForCollection` included, not just `{ adapter, coordinator }` (Phase 3
found the short version drops per-collection `schemaVersion`; DEC-A8). Only the driver differs.

```typescript
import { Context, Effect, Layer, Scope } from "effect"
import type { PersistedCollectionPersistence, SQLiteDriver } from "@tanstack/db-sqlite-persistence-core"
import { createSQLiteCorePersistenceAdapter, SingleProcessCoordinator } from "@tanstack/db-sqlite-persistence-core"
import { openBrowserWASQLiteOPFSDatabase, createBrowserWASQLitePersistence } from "@tanstack/browser-db-sqlite-persistence"

export interface PersistenceBaseShape {
  /** The shared TanStack persistence object the factory hands to persistedCollectionOptions. */
  readonly persistence: PersistedCollectionPersistence
}

export class PersistenceBase extends Context.Tag("PersistenceBase")<PersistenceBase, PersistenceBaseShape>() {
  /** Prod: wa-sqlite over OPFS via the browser package's public builder. One DB app-wide,
   *  closed on scope release. Single-tab (SingleProcessCoordinator). */
  static readonly layer: (args: { readonly databaseName: string }) =>
    Layer.Layer<PersistenceBase, never, Scope.Scope>
  //  = Layer.scoped(PersistenceBase, Effect.gen(function* () {
  //      const database = yield* Effect.acquireRelease(
  //        Effect.promise(() => openBrowserWASQLiteOPFSDatabase({ databaseName })),
  //        (db) => Effect.promise(() => db.close?.()),
  //      )
  //      return { persistence: createBrowserWASQLitePersistence({ database, schemaMismatchPolicy: "sync-present-reset" }) }
  //    }))

  /** Test/node: the test supplies only a raw SQLiteDriver (node:sqlite wrapper); the LIB must
   *  replicate createBrowserWASQLitePersistence's FULL logic over it — NOT just { adapter,
   *  coordinator }. The load-bearing part is `resolvePersistenceForCollection`, which mints a
   *  per-collection adapter carrying that collection's schemaVersion (an adapter cache keyed by
   *  policy|schemaVersion); without it the version is dropped and DEC-A6's reset never fires. Library
   *  ships no node driver (frontend-only pin); the driver is test infra. */
  static readonly layerSqliteDriver: (driver: SQLiteDriver) => Layer.Layer<PersistenceBase>
  //  builds: coordinator = new SingleProcessCoordinator(); an adapterCache keyed by `policy|version`;
  //  adapterFor(mode, schemaVersion) = createSQLiteCorePersistenceAdapter({ driver, schemaMismatchPolicy,
  //    ...(schemaVersion === undefined ? {} : { schemaVersion }) }) (cached);
  //  persistence = { ...forCollection("sync-absent", undefined),
  //    resolvePersistenceForCollection: ({mode, schemaVersion}) => forCollection(mode, schemaVersion),
  //    resolvePersistenceForMode: (mode) => forCollection(mode, undefined) }
}
```

Wiring:
- **prod:** `PersistenceBase.layer({ databaseName })`
- **test:** `PersistenceBase.layerSqliteDriver(<node:sqlite driver>)`

**Open decisions (grilling §3):**

- **DEC-A7 — `PersistenceBase` is the injectable tag; the seam is the persistence object, not the
  driver.** A shared, scoped resource (one DB, opened once, closed on release) with two real adapters
  → Tag + layers. The finer raw-`SQLiteDriver` seam is **impossible via public API**
  (`BrowserWASQLiteDriver` is internal + the `exports` map forbids deep imports), so the tag lives one
  level up. It bends decision 3 ("the factory is the *only* seam") — the deliberate exception
  OPFS-can't-test forces. *Answers "injectable via a tag?" — yes, at the persistence level.*
- **DEC-A8 — test supplies a raw `SQLiteDriver` to `layerSqliteDriver`; the lib wires the rest.**
  CLAUDE.md pins the library frontend-only, so the node/`node:sqlite` driver is test infra. The lib
  must replicate `createBrowserWASQLitePersistence`'s **full** logic over that driver — the adapter
  cache + `resolvePersistenceForCollection` (Phase 3 found that `{ adapter, coordinator }` alone drops
  per-collection `schemaVersion`, so the reset never fires). With that, the headless node gate is
  **faithful**: it proves the real persist → reset → rehydrate semantics, and the playground only
  proves "OPFS works here." *Open: node:sqlite (zero-dep, Node ≥22, experimental)
  vs better-sqlite3 (native build).*
- **DEC-A9 — single-tab now, multi-tab deferred.** `createBrowserWASQLitePersistence` already
  defaults to `SingleProcessCoordinator` (no election); the `BrowserCollectionCoordinator` (Broadcast
  Channel leader election across tabs) is opt-in. Defer it. *Recommend: yes.*
- **DEC-A8.1 — node test driver = `node:sqlite`.** Zero-dep, no native build; requires Node ≥22 (the
  repo's toolchain), accepting the "experimental" flag for test-only infra. *Locked.*

---

## §4 — the sync-session holder + lifetime (how `utils.writeSynced` reaches the sync loop)

`utils.writeSynced`/`deleteSynced` are built at **config-construction** time, but `begin/write/commit`
only exist inside the `sync` closure, which runs at **sync-start**. A `Deferred` bridges the two: the
closure fills it once on start; `writeSynced` awaits it (a write issued before start simply waits).

This is only sound if sync **starts on mount and never tears down while mounted**. The TanStack
source settles how: on the last unsubscribe the collection does *not* pause — it only starts a
**gcTime** timer (`changes.js:124` → `lifecycle.startGCTimer`), and the only thing that ever calls
`sync.cleanup()` is that timer (or an explicit `collection.cleanup()`). `startGCTimer` with a
**non-finite gcTime returns without scheduling** (`lifecycle.js:99-103`), and `startSync()` is
guarded so `sync()` is invoked once. So setting **`gcTime: Infinity`** makes the registry the *sole*
GC: sync starts on mount, stays up with zero subscribers, the session is captured exactly once, and
disposal happens only through the registry's finalizer (`collection.cleanup()`). No keep-alive
subscription (DEC-A10).

```typescript
import { Deferred, Effect, Exit } from "effect"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "../dispatch/sync-write.js"

/** The live handle into a started collection's synced-write path: the begin/write/commit trio the
 *  sync closure captures, wrapped as the two operations our write path needs. `upsert` resolves
 *  insert-vs-update from current membership (the synced store is keyed by `getKey`). */
interface SyncSession<T> {
  readonly upsert: (entity: T) => void  // begin → write({type:"update", value}) → commit  (update upserts; no in-mem read)
  readonly remove: (id: ModelId) => void // begin → write({type:"delete", key:id}) → commit
}

/** Builds the utils-hosted SyncWrite<T> and the `provide` the sync closure calls once on start.
 *  writeSynced/deleteSynced await the session, so order between "collection handed out" and
 *  "sync started" never races. */
const makeSyncWrite = <T>(): Effect.Effect<{
  readonly syncWrite: SyncWrite<T>                     // → utils on the collection
  readonly provide: (session: SyncSession<T>) => void  // ← called inside sync(), plain JS
}> => Effect.gen(function* () {
  const session = yield* Deferred.make<SyncSession<T>>()
  const syncWrite: SyncWrite<T> = {
    writeSynced: (entity) => Deferred.await(session).pipe(Effect.map((s) => s.upsert(entity))),
    deleteSynced: (id)    => Deferred.await(session).pipe(Effect.map((s) => s.remove(id))),
  }
  const provide = (s: SyncSession<T>) => Deferred.unsafeDone(session, Exit.succeed(s)) // sync, idempotent guard
  return { syncWrite, provide }
})
```

Factory body (§2) assembled, sketch:

```typescript
// inside effectCollectionOptions(...):
const { syncWrite, provide } = yield* makeSyncWrite<T>()
const collection = createCollection(persistedCollectionOptions({
  id: collectionId, getKey, schemaVersion,
  startSync: true,        // start sync on mount (so the session is captured + hydration runs)
  gcTime: Infinity,       // DEC-A10 — registry is the sole GC; sync never tears down while mounted
  utils: syncWrite,
  persistence: (yield* PersistenceBase).persistence,
  sync: { sync: ({ begin, write, commit, collection }) => {
    provide(makeSession({ begin, write, commit, collection, getKey })) // capture the trio
    // (markReady is the lib's wrapped one — fires after internal hydration; DEC-A5: no network here)
  }},
}))
yield* Effect.addFinalizer(() => Effect.promise(() => collection.cleanup())) // the only GC
return collection as LiveCollection<T>
// NO preload() — see DEC-A13. startSync kicks hydration in the background; consumers await readiness.
```

**Open decisions (grilling §4):**

- **DEC-A10 — `gcTime: Infinity` makes the registry the sole GC; no keep-alive subscription.**
  TanStack tears sync down only via the gcTime timer (or explicit `cleanup()`); a non-finite gcTime
  schedules nothing (`lifecycle.js:99-103`), and `startSync()` is guarded so `sync()` runs once. So
  `gcTime: Infinity` + `startSync: true` keeps sync active with zero subscribers, and the registry's
  `addFinalizer(() => collection.cleanup())` is the only teardown. This replaces the earlier
  keep-alive-subscription idea — it treated the symptom; this expresses the actual ownership (the
  registry owns the lifetime, not the subscriber-GC). **Must be `Infinity`, not merely large:** a
  finite gcTime lets an idle collection clean up, and `validateCollectionUsable` would auto-restart
  `sync()` → a new `begin/write/commit`, leaving the one-shot `Deferred` (DEC-A11) holding a stale
  session. *Recommend: yes.*
- **DEC-A11 — bridge primitive = one-shot `Deferred<SyncSession<T>>`.** Sound *because* of DEC-A10:
  `gcTime: Infinity` guarantees `sync()` is captured exactly once and never restarts while mounted.
  *Recommend: yes.* (If a future change ever lets sync restart mid-mount — e.g. finite gcTime for
  memory pressure — revisit with a re-provisionable holder like `SubscriptionRef`; flagged, not
  built.)
- **`upsert` = unconditional `write({ type: "update" })`; no membership read** *(resolved in Phase 3)*.
  An external synced `update` upserts an absent key (proven: the gate writes a row into an empty
  collection via `update`, 10/10 green), so the session reads **no in-memory state** — making
  `writeSynced` **readiness-independent**. The dispatcher therefore never needs to `preload` before
  writing, and no readiness gate belongs in `utils` (the wrapper queues mid-hydration writes anyway).
  This deletes the dependency rather than synchronizing on it — strictly better than gating the
  session on `onFirstReady`, which would block the first write per collection.

---

## Call graph

**Prod (browser / OPFS):**

```
app bootstrap
  ├─ PersistenceBase.layer({ databaseName })        opens ONE wa-sqlite/OPFS DB (shared)
  └─ CollectionRegistry.layer                       owns per-collection child scopes

webhookCollection(orgId)                            → MountRef { key, make }
  └─ yield* ref  →  registry.getOrCreate({ key, make })          (child scope forked here)
        └─ make = effectCollectionOptions({ collectionId: serializeKey(key), getKey, schemaVersion })
             ├─ makeSyncWrite<T>()                  Deferred<SyncSession>; utils = SyncWrite<T>
             ├─ createCollection(persistedCollectionOptions({ …, startSync, gcTime: Infinity, persistence }))
             │     └─ wrapped sync: loadSubset → hydrate in-mem (INTERNAL) → provide(session) → markReady
             └─ addFinalizer(collection.cleanup)    registry child scope = the SOLE gc (DEC-A10)
                  (no preload — startSync hydrates in background; consumers await readiness, DEC-A13)

SyncDispatcher.dispatch(event)                      server-truth (A.5)
  ├─ Insert/Update → getById(key)      → c.utils.writeSynced(data) → session.upsert → begin/write/commit
  │                                       └─ wrapper: applyCommittedTx (SQLite) + forward to in-mem
  └─ Delete        → getByEntity(name) → c.utils.deleteSynced(id)  → session.remove → …

reload (new page)  → mount again → loadSubset returns persisted rows BEFORE any network (the gate)
```

**Test (node:sqlite) — same graph, three swaps only:**

```
PersistenceBase.layerSqliteDriver(<node:sqlite driver>)   ← instead of .layer (OPFS)
no SSE/catchup (DEC-A5)        ← the test calls c.utils.writeSynced directly
"reload" = close the mount's child scope (collection.cleanup) but KEEP the node:sqlite DB handle
           open, then mount again → assert rehydration  (the test owns the DB's lifetime)
```

The only difference between the two is the driver — so the node test exercises the real
persist/hydrate/reset logic (`createSQLiteCorePersistenceAdapter` + `SingleProcessCoordinator`),
and the playground only proves OPFS itself works.

## Test plan (design-first Phase 3 — against this interface only)

- **`effect-collection`** (node:sqlite, `it.live` + `reloadUntil` poll — durability is eventually
  consistent, DEC-A14) — **the A.3 gate, all GREEN**:
  1. *hydrate-from-storage* — `utils.writeSynced(e)` → dispose mount → re-mount on the same DB → `e`
     is present before any network call.
  2. *no full re-list* — there is no `queryFn`; the re-mount populates purely from the persisted base.
  3. *deltas persist + converge* — write on top of an existing base; the latest value wins, durably.
  - `deleteSynced` removes and persists the removal; a repeat delete is idempotent.
  - a **schema change** (different derived `schemaVersion`) on the same `collectionId` → base dropped,
    rebuild starts empty (DEC-A6, end-to-end through `resolvePersistenceForCollection`).
  - *(skipped — write-before-ready)*: with `startSync: true` the session is captured synchronously, so
    the "before the session exists" window the `Deferred` guards can't occur; not worth a contrived test.
- **`schema-version`** (pure, `vitest`) — `deriveSchemaVersion` is deterministic and changes on field
  add/remove/rename, **same-name type change**, and brand change; returns a `uint32`. Asserts the
  *relation* between schemas, not the exact hash (survives an Effect AST-format change).
- **`persistence-base`** (node) — `layerSqliteDriver` yields a persistence with a working
  `resolvePersistenceForCollection` (exercised transitively by the gate's schema-reset slice).
- **`define-collection`** (shipped-code change) — `make` receives `mount.collectionId =
  serializeKey(key)`; global vs scoped overloads still type-check (`@ts-expect-error` on phantom-arg
  misuse).
- **`sync-dispatcher`** (the relocation) — the existing 17 tests stay green after the `.utils` hop;
  add one asserting `writeSynced`/`deleteSynced` are reached through `.utils`.
- **playground** (browser, manual/Playwright smoke) — the OPFS DB persists across a *real* reload:
  the one thing node can't prove.

Out of scope here (own passes): catchup/SSE transport, optimistic-mutation reconciliation (DEC-11),
multi-tab coordination.

**Phase-3 findings (all 4 gate slices GREEN, 0/10 flaky). The happy-path step-1 test masked two real
issues that the deltas/delete/schema-reset slices exposed — corrections to earlier claims:**
- *Insert-vs-update (was "unknown b"):* unconditional `update` **does** insert in-memory, even for an
  absent key — so write-*type* was never the problem. (Earlier I wrongly concluded the gate "proved
  upsert"; step 1 only ever inserted into an *empty* store.) `writeSynced` stays unconditional
  `update`.
- *Write durability is fire-and-forget (corrects "unknown a"):* the wrapped `commit` does
  `void runtime.persistAndBroadcastExternalSyncTransaction(...)` — the persist is **async and not
  awaited**, and the alpha exposes **no durability handle** on a sync-wrapped collection (`utils` is
  only our `writeSynced`/`deleteSynced`; `cleanup()` does not flush). The persist *survives* dispose
  and completes a few ticks later — it isn't killed, just not waited on. So the gate is **eventually
  consistent**: tests use `it.live` + a `reloadUntil` poll (remount-and-read until the state settles)
  and serialize each write phase's persistence before the next, so an orphaned persist can't clobber
  a later cross-remount write. Production durability is a *(rows + `lastSyncId`)* property — secured
  by persisting the cursor in the collection's metadata (same transaction as the rows) so reload is
  self-consistent and catchup heals the tail (DEC-A14, transport pass). We do **not** try to force
  per-write durability (no handle exists; it fights the alpha's design).
- *`layerSqliteDriver` must replicate the full browser builder, not just `{ adapter, coordinator }`
  (corrects DEC-A8):* the per-collection `schemaVersion` reaches the adapter only via
  `resolvePersistenceForCollection` (an adapter cache keyed by `policy|schemaVersion`). My first
  hand-built version omitted it, so the version was dropped and the schema-reset never fired. Fixed
  by replicating `createBrowserWASQLitePersistence`'s logic over the injected driver.

## Changes to shipped code

- `registry/define-collection.ts` — `make` gains a `mount: { collectionId; args }` param;
  `defineCollection` fills `collectionId = serializeKey(key)` (both overloads). *(DEC-A3)*
- `dispatch/sync-dispatcher.ts` — read the synced-write path via `.utils`
  (`getById<{ utils: SyncWrite<T> }>` / `getByEntity<…>`, then `c.utils.writeSynced` / `.deleteSynced`). *(§1)*
- `package.json` — add `@tanstack/browser-db-sqlite-persistence` + its `@journeyapps/wa-sqlite` peer
  (both pin `@tanstack/db@0.6.7` — no core bump). Node test uses built-in `node:sqlite` (no dep).
- `CLAUDE.md` decision 2 — correct the import path: `persistedCollectionOptions` lives in
  `@tanstack/browser-db-sqlite-persistence`, **not** `@tanstack/db` core (the rest of decision 2 was right).

## Decisions log (load-bearing — do not re-litigate without a new reason)

- **DEC-A1** Schema-less TanStack collection; decode happens once at the dispatch seam.
- **DEC-A2** The SQLite↔memory boundary is *internal to the library* — no decode of ours to do or skip.
- **DEC-A3** `defineCollection` injects `collectionId = serializeKey(key)`; lib FNV-hashes it → no org-id rules.
- **DEC-A4** *(amended)* The factory takes the schema **only** to auto-derive `schemaVersion` + infer `T`, never to validate; `deriveSchemaVersion(String(schema.ast))`.
- **DEC-A5** This pass's `sync` is network-free (catchup/SSE is the transport pass).
- **DEC-A6** Schema evolution = native dump-and-rebuild (`schemaVersion` + `sync-present-reset`); no table bookkeeping.
- **DEC-A7** `PersistenceBase` is the injectable tag at the persistence level (raw-driver seam impossible — internal class).
- **DEC-A8 / A8.1** Test injects a raw `SQLiteDriver`; driver = `node:sqlite`; faithful headless gate.
  `layerSqliteDriver` must replicate `createBrowserWASQLitePersistence` *fully* — incl. the adapter
  cache + `resolvePersistenceForCollection` that threads per-collection `schemaVersion` (else DEC-A6's
  reset never fires). `{ adapter, coordinator }` alone is insufficient.
- **DEC-A9** Single-tab (`SingleProcessCoordinator`) now; multi-tab deferred.
- **DEC-A10** `gcTime: Infinity` → the registry is the sole GC; no keep-alive subscription.
- **DEC-A11** One-shot `Deferred<SyncSession<T>>` bridges config-time utils to run-time sync (sound via A10).
- **DEC-A13** **The factory `startSync: true` but never `preload()`.** `startSync` *starts* sync on
  mount (non-blocking — kicks local-base hydration in the background); `preload()` would *await* full
  hydration before `make` returns, blocking every mount (bad UX, worse for large bases). Consumers
  handle readiness: `useLiveQuery` reflects loading state; the dispatcher's writes are queued during
  hydration; the session is captured synchronously on start so `writeSynced` never races. `startSync`
  itself **is** required (unlike hosting, which is lazy via `startSyncImmediate` in its mutation
  handlers) because **our persistence is coupled to sync** — `persistedCollectionOptions` makes the
  `begin/write/commit` path the *only* door to the persistence layer, so the dispatcher can't persist
  server truth to a mounted collection unless its sync is running. Hosting's persistence (Dexie) is
  decoupled, so it could afford to be lazy; we can't. Verified: gate test moves its readiness await
  (`collection.preload()`) into the test, 10/10 green.
- **DEC-A14** **Synced writes are fire-and-forget / eventually-consistent; durability is a
  (rows + cursor) property, not a per-write guarantee.** The alpha's wrapped `commit` `void`s the
  async persist and exposes no durability handle. We don't fight it: `writeSynced` returns once the
  in-memory write is applied; the persist completes shortly after (it survives dispose). Reload
  durability is secured in the transport pass by persisting `lastSyncId` in the collection's metadata
  (same tx as the rows) so rows+cursor are mutually consistent and catchup heals any tail. The offline
  gate, lacking catchup, verifies *eventual* persistence by polling (`it.live` + `reloadUntil`).
- **DEC-A12** *(transport-pass scope; recorded now)* **Source strategy is snapshot-and-tail, not
  catchup-from-zero.** On mount the source slot chooses: if the local base was **never bootstrapped**,
  or the durable "last bootstrapped at" is **older than a per-collection threshold**, do a **full
  refetch from a list/snapshot endpoint** (`BootstrapSource`) and replace the base; otherwise
  **catchup** incrementally from `lastSyncId`. Then SSE for live deltas. Rationale: `catchup(from: 0)`
  forces the server to scan + squash the *entire* event log (O(total events) vs a list's O(live
  rows)), and once the log is retention-pruned it *cannot* reconstruct cold state at all — so a
  snapshot endpoint is a correctness requirement, not just an optimization. The freshness threshold is
  **our durable metadata** (a `lastBootstrapAt` alongside `lastSyncId`), consistent with decision 5
  (freshness is ours) — it is *not* the framework's `staleTime` (which resets on reload and is
  rejected for gating catchup). Consequence: the factory gains `bootstrapFn` + a staleness config in
  the transport pass; this offline pass is unaffected.

**CLAUDE.md cross-refs:** bends decision **3** (the factory is no longer the *only* seam — `PersistenceBase`
sits inside it, DEC-A7); honors decision **9** (no string grammar — DEC-A3); leans on decisions **2/5**
(persistence base + durable cursor) and protocol **DEC-6/DEC-11/DEC-12**.

---

# Transport tier (A.6–A.9) — LOCKED + IMPLEMENTED

> **Status: LOCKED + GREEN.** Signed off 2026-06-08. The read path over the Bucket-A factory: one
> global SSE connection + global catchup feeding the central `SyncDispatcher`; per-collection `sync`
> stays network-free (DEC-A5). Modules live in `src/client/`. 13 tests added (44 total in the
> package). Decode SSE/`/catchup` bodies against the protocol schemas at the boundary — never cast.

## Modules (`src/client/`)
- `last-sync-id-store.ts` — `LastSyncIdStore`: the global durable cursor. `get`/`set`(monotonic by
  `compareSyncId`)/`clear`. `layer` (localStorage), `layerMemory` (Ref).
- `catchup-client.ts` — `CatchupClient.fetch({from}) → CatchupResponse` decoded at the boundary;
  `CatchupFailed` is modeled+recoverable (orchestrator tails anyway). `layer({url})` over `HttpClient`,
  `layerMemory(canned)`.
- `sync-transport.ts` — `SyncTransport.connect: Stream<HydratedSyncEventEnvelope, SyncConnectionLost>`,
  one decoded SSE connection that **fails on drop**. `layer({url, keepAlive})`, `layerMemory(queue)`.
- `sync-client.ts` — `SyncClient.start(specs)`: the orchestrator (A.8 resync folded in). `BootstrapSpec`
  + `bootstrapSpec` erase-helper; `reloadWindow` convenience. `layer({onResync})`.

## The loop (`SyncClient.start`, forked; runs forever)
```
each cycle (retry on SyncConnectionLost, spaced 3s):
  from = cursor ?? "0"
  resp = catchup({from})                  CatchupFailed ⇒ log + tail anyway
  resp has a Resync arm ?  snapshot every spec (bootstrapFn → upsert + delete-absent)
                        :  dispatch resp's entity events
  cursor.set(resp.lastSyncId)             cursor ALWAYS from catchup
  tail transport.connect:  entity → SyncDispatcher.dispatch ; live Resync → cursor.clear *> onResync (stop)
```

## Decisions log (DEC-T*, load-bearing — do not re-litigate without a new reason)
- **DEC-T1** One global SSE connection + central dispatch (Model Y); per-collection `sync` stays
  network-free. *Rejected:* per-collection network (Model X) — fights the global server contract.
- **DEC-T2** Cursor durability = a single `localStorage` `lastSyncId`. Fire-and-forget row persistence
  means (rows, cursor) can skew; healed by catchup overlap + cold re-snapshot. **Revises DEC-A14** —
  per-write same-tx durability is unattainable (no alpha handle), so we don't pursue it.
- **DEC-T3** Cursor's single source of truth = the sync stream (`CatchupResponse.lastSyncId`, live
  `event.syncId`). `bootstrapFn` returns **rows only**. *Rejected:* `bootstrapFn` returns
  `{rows, syncId}` — couples every list endpoint to the cursor.
- **DEC-T4** `SyncTransport.connect` **fails** on drop; the orchestrator's outer retry re-runs catchup
  each reconnect (heals the gap). *Rejected:* internally-retrying transport (gap relies on server
  `Last-Event-ID`).
- **DEC-T5** Cold/warm unified: `from = cursor ?? "0"`; snapshot-vs-delta decided by the catchup
  response — a `Resync` arm ⇒ snapshot via `bootstrapFn`, else dispatch deltas. Preserves DEC-A12
  snapshot-and-tail (a mature server resyncs `from:"0"`) with **no per-collection staleness store**.
- **DEC-T6** Resync is blunt and context-split: **in a catchup response** ⇒ snapshot (no reload → no
  loop); **live in SSE** ⇒ `cursor.clear *> onResync` (full reload, Model A). Target ignored entirely —
  no `groupScope` hook, no `isUnder`. *Rejected:* per-target dispose mapping; in-place collection reset
  (a future option if reload UX proves harsh).
- **DEC-T7** `onResync` is an injected `Effect<void>` (prod: `reloadWindow`), keeping core
  framework-neutral and the reload assertable in tests.
- **DEC-T8** `start` runs forever and is forked; no "ready" signal — local hydration drives first paint
  (DEC-A13).
- **DEC-T9** Snapshot reconcile = upsert fetched (`writeSynced`) + delete-absent (`deleteSynced` for
  `currentKeys − fetchedKeys`), so a snapshot is a true replacement.
- **DEC-T10** *(2026-06-09 hardening)* **Undecodable `data` on a known model is skipped wholesale,
  never fatal** — the same forward-compatibility policy as undecodable envelopes (transport) and
  unknown models (ingest): warn + drop, no log append, no apply, no cursor advance (catchup overlap
  re-delivers; a snapshot/resync heals divergence). Replay applies the same policy to logged rows
  that no longer decode. The loop's only remaining death is a genuine defect, surfaced by a
  `tapDefect` log in `forkLoop` (a forked fiber otherwise dies silently). *Rejected:* `orDie` on the
  per-event decode — one schema-drifted event from a newer server killed sync permanently and silently.

## Deferred (flagged, not built)
- Incremental **workspace-switch** bootstrap (mounting a new scope while the cursor is `Some` won't
  snapshot it). DEC-A12's staleTime *threshold* (`lastBootstrapAt`). Browser OPFS `PersistenceBase.layer`
  (playground territory; only `layerSqliteDriver` test infra exists). In-place resync (DEC-T6).

# Native-collection redesign + React bindings (DEC-R*) — LOCKED

> **Status: LOCKED.** Signed off 2026-06-08. Goal: collections are **native TanStack collections** —
> `defineCollection(...)` returns a value you pass straight to `useLiveQuery`, no provider/hook/wrapper
> ceremony. This **revises** several Bucket-A decisions (noted per DEC-R below). The transport-tier
> *behaviour* (DEC-T*) is unchanged — catchup/cursor/tail/resync are identical; only the wiring shape
> changes: the app declares one `defineCollection` per model + one explicit `SyncMap`, instead of
> hand-assembling `SyncDispatcher.fromEntries` + `SyncClient.start(specs)`.

## The UX (the north star)
```ts
const database    = await openBrowserWASQLiteOPFSDatabase({ databaseName: "app.sqlite" }) // async, once, at startup
const persistence = createBrowserWASQLitePersistence({ database })                        // app value (off render path)
const runtime     = makeLiveRuntime({ persistence, loop: TransportInfra, onResync: reloadWindow })

export const webhookCollection = defineCollection({
  runtime, entity: "Webhook", schema: Webhook, getKey: (w) => w.id,
  scopeOf: (w) => w.orgId, listFn: (orgId) => api.listWebhooks(orgId),       // omit scopeOf+make listFn an Effect ⇒ global
})

function App() { useLiveSync(runtime, { Webhook: webhookCollection }); return <Webhooks/> }      // start loop once
function Webhooks({ orgId }) {
  const { data } = useLiveQuery(() => webhookCollection(orgId), [orgId])                          // native, stable (DEC-R9)
}
```

## The architecture (acyclic DAG)
```
runtime (infra: registry value + persistence value | async transport+cursor+catchup)   ← built first, knows no collections
   ↑
collections = defineCollection({ runtime, … })   ← registry-backed handle → native LiveCollection<T>, carries _meta
   ↑
SyncMap { Webhook: webhookCollection, … } → syncLoop   ← assembled last; references the handles
```
A collection needs only **infra** (registry + persistence) to exist; it does **not** depend on the
dispatcher (the loop *pushes into* it via `utils.writeSynced`). So the runtime can be an input to
`defineCollection` with no cycle.

## Two execution surfaces (the load-bearing mechanic)
Mounting happens **during render** (inside `useLiveQuery`'s queryFn); the loop runs **in an effect**.
They use different paths:
- **mount (sync):** the `registry` is a plain **value** built once via `Effect.runSync` in a long-lived
  scope. `webhookCollection(scope) ≡ Effect.runSync(registry.getOrCreate({ key, make }))`. `make` is
  `Effect.sync(() => createCollection(...))` + an `addFinalizer(cleanup)`, so it requires only `Scope`,
  which the registry discharges ⇒ `Effect<A, never, never>` ⇒ `runSync` can't hit an async boundary.
  `persistence` is a **value closed over**, not a context dep — that's why the mount path needs no async
  layers. After first mount it's a `Map.get`: referentially **stable** identity, no rebuild, no churn.
- **loop (async):** `transport + catchup + cursor` run by `useLiveSync` via `runFork` in `useEffect`,
  interrupted on unmount. Never on the render path. The runtime's registry value is shared into the loop
  via `Layer.succeed(CollectionRegistry, registry)`, so dispatch sees the same instances the UI mounts.

Registry lifetime = the **app's**, not the loop fiber's: unmounting `useLiveSync` stops the SSE loop but
does **not** dispose collections (a remount reuses the warm local store). Disposal is `disposeScope`
(workspace switch) / `disposeAll` (logout) / app teardown closing the long-lived scope.

## The creator pattern (TanStack-idiomatic)
`liveCollectionOptions({ getKey })` is the **inner** options creator (the live-sync analogue of
`queryCollectionOptions`): network-free `sync` (installs the `SyncWrite` session + `markReady`, DEC-T1)
+ `utils: SyncWrite<T>` + `gcTime: Infinity` (DEC-A10) + `eager`/`startSync`. Persistence wraps it, exactly
like the TanStack docs: `createCollection(persistedCollectionOptions({ persistence, schemaVersion, id, ...liveCollectionOptions({ getKey }) }))`.
This composition lives inside `defineCollection`'s `make`; the app never writes it.

## The explicit map + loop
`SyncMap = Record<modelName, Handle>` where each `Handle` carries `_meta: ModelMeta<T>` =
`{ entity, schema, getKey, scopeOf: Option<(t)=>string>, listFn }`. `syncLoop(map, onResync)` is the
DEC-T loop, re-driven by `_meta`:
- **dispatch** entity event: `h = map[modelName]`; `Delete` ⇒ fan-out `deleteSynced` over every mounted
  instance of `entity`; `Insert/Update` ⇒ `decode(_meta.schema)`, `key = scopeOf ? scopedKey{entity, scopeOf(data)} : globalKey(entity)`, `registry.getById(key)` → `writeSynced` (only **mounted** instances).
- **snapshot** (catchup `Resync`): for each model, for each **mounted** instance, `_meta.listFn(scope)` →
  reconcile (upsert + delete-absent, DEC-T9). Mounting on first `useLiveQuery` render seeds the workspace.

## Decisions log (DEC-R*, load-bearing — do not re-litigate without a new reason)
- **DEC-R1** Collections are **native** `createCollection` results; `useLiveQuery` consumes them with no
  wrapper. *Rejected:* a `useLiveCollection` subscription hook (would shadow `useLiveQuery`, deletion test
  fails) and a `(collection, entry)` pair (doubles names).
- **DEC-R2** `defineCollection({ runtime, … })` is **runtime-bound** and returns a registry-backed
  **callable handle** (`() => LiveCollection` global / `(scope) => LiveCollection` scoped), not an inert
  `MountRef`. **Revises DEC-10** (MountRef/yieldable) and **DEC-A3** (collectionId now `serializeKey(key)`
  computed in `make`).
- **DEC-R3** Persistence is an **app value** (`PersistedCollectionPersistence`) passed to
  `makeLiveRuntime`, not a service tag. **Retires `PersistenceBase` tag** (**revises DEC-6/DEC-A2/DEC-A7**
  for this seam). The node/sqlite persistence builder is test infra only (DEC-A8 unchanged). **Prod
  builds the value with the official `@tanstack/browser-db-sqlite-persistence` (pinned `0.1.11`, matching
  our `db-sqlite-persistence-core` pin): `await openBrowserWASQLiteOPFSDatabase({ databaseName })` →
  `createBrowserWASQLitePersistence({ database })` over `@journeyapps/wa-sqlite`. The library ships no
  browser builder — our node `makeSqlitePersistence` is a hand-port of that same official assembly.**
- **DEC-R4** `effectCollectionOptions` (`Effect<Collection>`) → `liveCollectionOptions` (plain
  `CollectionConfig` fields + `utils`); `createCollection` moves into `defineCollection`'s `make`.
- **DEC-R5** No auto-registration. The **explicit `SyncMap`** is passed to `syncLoop`/`useLiveSync`.
  Metadata rides on the handle (`_meta`) so the map is literal `{ ModelName: collection }` with no
  duplicated `schema`/`scopeOf` (D4=b). *Rejected:* per-collection bus subscription (auto-register);
  metadata-only map (re-declares schema).
- **DEC-R6** `scopeOf` is `(entity: T) => string` and the mount arg **is** the scope string
  (`webhookCollection(orgId)`). **Revises DEC-10**'s `(args) => string`. One scope function, and the
  dispatcher gets entity→scope directly. *Rejected:* `Args`-mapped mounting (two scope functions).
- **DEC-R7** `SyncClient.start(specs)` → `syncLoop(map, onResync)`; `SyncDispatcher` survives as an
  internal driven by the map. App-facing `dispatchEntry`/`fromEntries`/`BootstrapSpec`/`bootstrapSpec`
  **retire**. Loop behaviour (DEC-T1…T9) unchanged.
- **DEC-R8** Two runtime surfaces (sync registry+persistence value for mount; async ManagedRuntime over
  `loop` for the fiber). The registry value is built with `Effect.runSync` in a long-lived scope and
  shared into the loop via `Layer.succeed`. `useLiveSync` `runFork`s the loop on mount, `Fiber.interrupt`
  on unmount; collections are NOT disposed on unmount.
- **DEC-R9** Both read forms are native (typechecked in `react/test/use-live-query.types.test.ts`):
  the direct overload `useLiveQuery(() => webhookCollection(orgId))` **and** the join/filter form
  `useLiveQuery((q) => q.from({ w: coll }))`. The join form requires a collection's `utils` to be a
  `Record<string, Fn>` (TanStack `Fn = (...args) => any`), so `SyncWrite<T>` carries a structural-only
  index signature `readonly [util: string]: (...args: never[]) => unknown` — the widest function the two
  real methods satisfy, **no `any`**. *Trade-off (accepted):* `.utils` typo-safety is partially relaxed
  (a bare typo'd access compiles), but the named `writeSynced`/`deleteSynced` stay precise and a typo
  *called with a real arg* still errors (args are `never[]`). `.utils` is effectively internal (dispatcher
  reads it; UI reads via `useLiveQuery`), so exposure is minimal. *Rejected:* precise utils + a cast at
  every join site.

---

# Write path (A.10 revision) — library-reconciled handlers (DEC-W*) — LOCKED + IMPLEMENTED

> **Status: LOCKED + GREEN.** Implemented 2026-06-09 (`ee5177e` + the DEC-W2 guard). Amends the
> original A.10 contract (where the app handler called `collection.utils.writeSynced` itself).

- **DEC-W1** `onInsert`/`onUpdate` return the **server-confirmed row** (`Effect<T, unknown, R>`); the
  library reconciles it into the synced baseline (`writeSynced`) inside `bridge` **before the mutation
  resolves** (Model B). `onDelete` returns `void`; the library `deleteSynced`s by the mutation key.
  Apps never touch `collection.utils`. *Rejected:* handler-side `writeSynced` (the original A.10
  shape) — every app must remember the call, nothing in the types enforces it, and a missed call is an
  invisible flicker bug. Returning `T` makes the reconcile input part of the handler's type.
- **DEC-W2** **One mutation per transaction, enforced loudly.** A bridged handler receiving
  `mutations.length > 1` dies with `BatchedMutationsUnsupported` *before the server call* — the
  library reconciles exactly `mutations[0]`, so a batch would silently lose rows 2..n the moment the
  optimistic tx drops. Array-batch reconcile (`ReadonlyArray<T>`-returning handlers) is a flagged
  future pass, not built.
- **DEC-W3** **`services` is the executor, not a context donor.** Handlers run ON the runtime
  (`services.runPromise(handler → reconcile)`); the loop-facing `listFn` runs WITH it
  (`Effect.provide(services)` — the same memoized runtime, kept in Effect-land so interruption and
  causes survive). The runtime builds lazily on first use and memoizes. *Rejected:* the original
  eager `services.runSync(Effect.context<R>())` capture at define time — it forced the app's layer
  graph to construct synchronously at module import (one `Effect.promise` in a layer ⇒
  `AsyncFiberException` at import, pointing at library internals) and the frozen context kept serving
  finalized services after `services.dispose()` instead of failing loudly.

# EventLog manager — replay-on-mount (A.12, DEC-E*) — LOCKED + IMPLEMENTED

> **Status: LOCKED + GREEN.** Signed off 2026-06-09; implemented in commit `f291562` (58 package tests,
> `pnpm -r typecheck` clean). Fixes the snapshot-on-mount bug: a scope mounted **after** its events streamed
> past rendered **empty** because `syncLoop` dropped events for not-yet-mounted scopes *while advancing the
> global cursor past them*. Now every received event is **logged** (even when dropped), and on mount a
> collection converges by **replaying the durable local log** when it can — falling back to `listFn`
> bootstrap only when the log can't cover the gap. Frontend-only; one backend contract (DEC-E8/E9).
> The durable **IndexedDB `EventLogStore.layer` now ships** alongside `layerMemory` — proven against real
> IDB by `examples/playground/test/event-log-store.browser.test.ts` (append-dedupe, scope+Delete read,
> magnitude-not-lexicographic ordering, prune-floor, watermark/resync — each surviving a layer-scope reload).

## Modules (`src/client/`)
- `event-log-store.ts` — `EventLogStore`: the durable log **and** its sync metadata, one seam.
  `append` (upsert by `syncId` ⇒ free dedupe) · `read({modelName, scope, since})` (syncId-ordered slice;
  `scope` `Some` ⇒ that scope's rows **plus** scope-less `Delete`s) · `prune({perModel, total})` ·
  `floor(modelName)` · `get/setBaseWatermark` · `get/setLastResync`. `LoggedEvent` is the schema-agnostic
  at-rest row (`syncId, modelName, scope, tag, modelId, data`). `layerMemory` only (Refs).
- `mount-decision.ts` — `decideOnMount(...)`: a pure `MountDecision` enum (`Skip`/`Replay`/`Bootstrap`)
  from syncId positions alone. The heart of the design.
- `prune-plan.ts` — `prunePlan(...)`: the pure per-model + global retention policy, shared by any adapter.
- `sync-loop.ts` — refactored: `ingest` (append → apply → cursor), source-agnostic `applyWrite`/`applyDelete`,
  `onMount`, one merged single-fiber inbox (`transport ⊕ registry.mounts`), prune every N ingests.
- `collection-registry.ts` — gains `mounts: Stream<CollectionKey>` (unbounded queue; emits on first create).

## The decision (heart)
```
decideOnMount(baseWatermark, cursor, modelFloor, lastResyncAt):
  no base             ⇒ Bootstrap     // never had a base
  base >= cursor      ⇒ Skip          // already complete to the cursor
  resync since base   ⇒ Bootstrap     // local log invalidated (DEC-E9)
  floor > base        ⇒ Bootstrap     // pruned past the base ⇒ gap not covered
  else                ⇒ Replay        // the log fully covers (base, cursor]
```

## Invariants (named in code; correctness rests on these)
- **idempotency** — application is key-addressed upsert/delete (`writeSynced`/`deleteSynced` by `ModelId`)
  in `syncId` order; an under-estimated `baseWatermark` only re-applies dominated events. Replay never
  touches the optimistic-mutation handlers.
- **floor-guard** — never `Replay` when `floor > base` (or the model's log is pruned-but-uncovered);
  `Bootstrap` instead. The single line that keeps the size cap from corrupting the base.
- **cursor-completeness** — the cursor never advances past terminal state the client hasn't received
  (catchup delivers the visible terminal set up to `lastSyncId`, else `Resync`; the tail is in-order;
  `ingest` appends *before* routing). floor-guard + completeness ⇒ replay is hole-proof.

## Decisions log (DEC-E*, load-bearing — do not re-litigate without a new reason)
- **DEC-E1** Manager is **loop-internal** (`ingest`/`onMount` + the pure `decideOnMount`), not a tag — its
  only caller is the loop and its deps are already seams (one adapter ⇒ hypothetical seam). May become a
  single-default-layer `Context.Tag` if cleaner; **never `Effect.Service`**; no speculative 2nd adapter.
- **DEC-E2** Watermarks **and** `lastResyncAt` live **on `EventLogStore`** (one IDB, one metadata home),
  not a separate `WatermarkStore`. *Rejected:* a second tag always provided/swapped as a pair = false split.
- **DEC-E3** `baseWatermark` is written **once per mount, at `onMount` completion** (bootstrap/replay arms).
  **No dispose-writes, no per-event writes, no flush.** Lag is underestimate-safe (only causes extra
  idempotent replay). *Rejected:* dispose-writes (need a strictly-ordered queue to avoid over-estimating
  past post-dispose events); per-event writes (O(mounted) IDB writes per event). Throttled flush **deferred**.
- **DEC-E4** Registry gains **`mounts` only** (unbounded queue, `runSync`-safe). **No `disposes`** — the
  prune cap bounds the log and `onMount` rereads freshness, so a disposed scope freezes safely on its own.
- **DEC-E5** **One merged inbox** (`Stream.merge(transport.connect, registry.mounts)`), single fiber — so
  replay and live ingest **never interleave** (a freshly-mounted scope sees `replay[..base]` then live).
- **DEC-E6** **`dispatch` is the single source-agnostic application** (`applyWrite`/`applyDelete` on decoded
  data + meta) — live, catchup, and replay all flow through it; only the *recording* (append + cursor) is
  ingest-specific. `LoggedEvent` is keyed by **`modelName`**; the loop maps `entity → modelName` from the
  `SyncMap`. *Rejected:* a fat `EntityEvent`-in-the-log (carries envelope-only fields dispatch never reads).
- **DEC-E7** Prune = **per-model cap `perModel` + global capacity `total`**, denominated in **events
  (size), not wall-clock** (so no `createdAt` on the row). Per-model isolation solves chatty-evicts-quiet;
  the global cap is the backstop. Runs every `everyEvents` ingests. *Rejected:* single global cap (a chatty
  model evicts a quiet model's gap); `minWatermark`-driven prune (the caps + floor-guard subsume it).
- **DEC-E8** **Backend contract:** server catchup retention is a *deployment* property; a `from` **below
  the retention floor MUST return `Resync`** (never silently-incomplete deltas) or cursor-completeness
  breaks. Retention is **two independent axes**: client `perModel`/`total` (events, bounds local replay)
  vs. server retention (wall-clock, bounds the offline gap; surfaces to the client only as a `Resync`).
  The client never computes cursor age.
- **DEC-E9** A resync that passed **while a scope was unmounted ⇒ Bootstrap**, via a **single global
  `lastResyncAt`** (newest resync syncId, monotonic), consistent with DEC-T6's blunt/target-ignored resync.
  Resync events are **not** appended to the log (not `AppliedEvent`s); ingesting one only bumps `lastResyncAt`.
- **DEC-E10** *(TDD refinement)* `modelFloor` is the per-model **prune boundary** (highest deleted syncId),
  **not the oldest event**. `None` ⇒ nothing pruned ⇒ complete from the start ⇒ **Replay**; `Some(f)` ⇒
  replay safe iff `f <= base`. First cut had `None ⇒ Bootstrap`, which made the common "caught-up → few
  events → remount" path always bootstrap — wrong.
- **DEC-E11** *(TDD refinement; amended 2026-06-09)* After a catchup, `baseWatermark = lastSyncId` is
  set for mounted scopes that **rode it from a complete base** — after a `Resync` catchup: every mounted
  scope (`snapshotAll` just healed them all); after a **delta** catchup: only scopes with `base >= from`,
  or no base when `from = "0"` (cursor-completeness ⇒ the full visible state was delivered). The original
  blanket form stamped a never-bootstrapped scope that mounted before the first warm catchup as caught-up;
  its `onMount` then `Skip`ped and the scope durably rendered only the delta window (the deep-link bug).
  The premounted-cold-start wipe the blanket form guarded against is exactly the `from = "0"` case, which
  still marks. The cycle additionally runs **`healMounted`** (an idempotent `onMount` over every mounted
  instance) after each catchup, so healing is a property of every cycle — not of mount-signal queue
  delivery, which a connection drop can eat.
- **DEC-E12** `syncId` is **unique-per-event** (the exclusive cursor already requires it) ⇒ safe as the IDB
  **primary key** + dedupe key. The system is **gap-tolerant**: syncIds are opaque *positions* compared by
  magnitude (`compareSyncId`), never subtracted; `floor`/caps count **rows**, never id-distance.
- **DEC-E13** *(storage; built in the browser-proof pass)* Events in **IndexedDB** keyed by `syncId` (PK);
  watermarks / `lastResyncAt` / per-model prune-floors in a **sibling keyval store in the same DB**, and
  **separate from the OPFS collection DB** (cross-store eventual consistency already accepted, DEC-A14/T2).
  *Rejected:* localStorage (too small/sync/string-only — home of the tiny global cursor only); a second
  wa-sqlite/OPFS DB (couples to a peer-dep's internals, no cross-store tx anyway).
- **DEC-E13a** *(implementation refinement — supersedes the `[modelName, scope]` compound index + syncId
  range in the locked sketch)* The index is on **`modelName` alone**; `read`/`prune` filter, **sort, and
  retain in memory** with `compareSyncId`. Two facts force this: (1) IDB orders string keys
  **lexicographically** but `syncId`s order by **magnitude** (`"10" < "2"`), so a `syncId` range scan/delete
  would be wrong — never range-scan the key; (2) a `[modelName, scope]` compound index **drops scope-less
  `Delete`s** (a `null` index-key path makes the record un-indexed), but a scoped `read` must include them.
  Cap-bounded stores keep the per-model `getAll` + in-memory pass cheap. Rows round-trip through a
  `StoredEvent` schema (`Option` ⇄ `string|null` / `unknown|null` at the seam); driver faults are **defects**
  (`Effect.promise` dies on rejection — the method error channel stays empty). The `layer({databaseName?})`
  is `Layer.scoped`: opens on acquire, `db.close()` on release.

## Deferred (flagged, not built)
- Throttled flush of `baseWatermark` while mounted (DEC-E3 optimization — shortens reload replay).
- Registry **eviction backstop** (collections are resident until explicit `dispose*`).
- Per-target resync (kept blunt/global via `lastResyncAt`, DEC-E9).