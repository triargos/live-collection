# @triargos/live-collection

## 4.3.0

### Minor Changes

- 00096a1: Upgrade to TanStack DB `0.12.1`.

  The `@tanstack/db` peer range moves to `^0.12.1`, `@tanstack/db-sqlite-persistence-core` to
  `^0.4.5`, and `@tanstack/react-db` to `^0.5.5`. Upgrade `@tanstack/browser-db-sqlite-persistence`
  to `^0.2.30` with them; the persistence packages pin `@tanstack/db` exactly.

  **Synced writes no longer wait for hydration.** `db-sqlite-persistence-core@0.4.5` fixes the
  `SingleProcessCoordinator` bug where a sync commit made during startup hydration never settled,
  so the guard that held `SyncWrite` calls until the collection's first hydration is removed. A
  synced write now commits as soon as the collection's sync has started.

## 4.2.0

### Minor Changes

- fb0385f: Upgrade to Effect `4.0.1`, the first stable v4 release.

  The `effect` peer range moves to `^4.0.1`. Upgrade Effect in lockstep; a workspace cannot
  mix Effect versions in one type graph. No library API changes.

  **Brands no longer affect the derived schema version.** `Schema.brand` is TypeScript-only
  in Effect `4.0.1` and absent from the schema AST that `deriveSchemaVersion` hashes. A
  brand-only change keeps the version; field names, types, and checks still change it.

  **One-time local rebuild for branded schemas.** A collection whose schema contains a
  brand (for example a branded id field) derives a new version on first load: TanStack
  dumps and rebuilds the persisted local table and the collection re-lists from the server
  once. Unbranded schemas keep their version.

- f355f32: Upgrade to Effect `4.0.0-rc.118`.

  The `effect` peer range moves to `^4.0.0-rc.118`. Upgrade Effect in lockstep; a
  workspace cannot mix Effect release candidates in one type graph.

  Renamed APIs, applied across all packages and the reference app:

  - Every `effect/unstable/*` module moved to `effect/*`; the old paths are removed.
    `effect/unstable/http` is now `effect/http`.
  - `effect/unstable/httpapi` is now `effect/http-api`.
  - `NetAddress` lives in `effect/net`; `HttpServer.address` is a `NetAddress.SocketAddress`
    (`InetAddressV4 | InetAddressV6 | UnixPathAddress`) instead of `TcpAddress`.
  - Built-in `Config` constructors are PascalCase: `Config.string` is `Config.String`, and
    `Config.Port("PORT")` replaces `Config.schema(Config.Port, "PORT")`.

  The library's exported services, layers, and error classes keep their names, tags, and
  fields. Apps only update their own Effect imports, for example `FetchHttpClient` from
  `effect/http`.

  **One-time local rebuild on first load.** `deriveSchemaVersion` hashes Effect's
  AST representation, and that representation changed shape. Every collection
  derives a new version, so TanStack dumps and rebuilds the persisted local table,
  the journal mark is orphaned, and the mount decides `Snapshot` and re-lists from
  the server. This costs one full refetch per collection and then settles.

- cbbc4a1: Partial indexes: load keyed subsets of a model on demand and keep them live.

  A model's cost becomes proportional to what the user opens instead of table size.
  The server declares a closed set of index keys per model — no predicate language —
  and the client asks for one key value at a time.

  `defineCollection` gains a third variant next to global and scoped. `partial.by`
  replaces `listFn` (the batch endpoint is the snapshot path) and excludes `scopeOf`;
  each key generates one flat ensure, `templateId` → `utils.loadByTemplateId`:

  ```ts
  defineCollection({
    entity: "SelectionTemplateValue",
    partial: { by: { templateId: (v) => v.templateId } },
    // ...
  });
  ```

  `loadBy*` is an idempotent ensure, not a query and not a lease. It decides between
  skip (durable coverage mark is current), replay (journal still holds the gap, no
  network), and snapshot (`POST /sync/batch`). Calls in one microtask window coalesce
  into a single batch request across keys and collections.

  New public API:

  - **protocol** — `HydrateBatchRequest`, `HydrateBatchResponse`, `HydrateBatchResult`,
    `IndexValue`; `indexes` on the model registry entry.
  - **server** — `SyncFeed.hydrateBatch`, `UnknownIndexError` (⇒ 400 at the app's route).
  - **live-collection** — `HydrateClient` (`layer({ url })`), the `partial` collection
    variant, coverage tracking, and `SyncJournal.subsetMarks`.
  - **react** — `usePartialLoad` and `SubsetStatus` (`Loading | Ready | Forbidden | Failed`,
    `Failed` carrying a `retry` handle).

  Apps without partial collections change nothing and omit `HydrateClient`; a `loadBy*`
  snapshot without it is a defect with a clear message. Reads stay `useLiveQuery`, and
  writes keep the existing optimistic handlers.

  **No local rebuild.** The journal's last-applied record gained optional `scope` and
  `subset` fields, so records written by earlier versions still decode. Schema versions
  are untouched.

  Known residual: a write committing during an in-flight snapshot fetch can flicker once
  and self-heals. There is no subset eviction — rows and marks stay for the session.
  See `docs/partial-indexes.md`.

- ddf2ab8: Upgrade to TanStack DB `0.12.0`.

  The `@tanstack/db` peer range moves to `^0.12.0` and `@tanstack/db-sqlite-persistence-core`
  to `^0.4.4`. The persistence packages pin `@tanstack/db` exactly, so upgrade `@tanstack/db`,
  `@tanstack/react-db` (`^0.5.4`), and `@tanstack/browser-db-sqlite-persistence` (`^0.2.29`)
  together.

  **Synced writes complete once durable.** `utils.writeSynced`, `deleteSynced`, `replaceSynced`,
  and `patchSynced` now finish only after the sync transaction is applied and written to SQLite.
  TanStack DB `0.12` queues a sync transaction behind one still persisting and aborts queued
  transactions on `cleanup()`. Before this change, a sync write followed by `cleanup()` could
  be dropped while its last-applied mark had already advanced, so the row stayed missing until
  the next full snapshot. A write dropped by `cleanup()` now interrupts the caller instead of
  completing, and a persistence failure surfaces as a defect instead of a warning. Do not
  run these methods inside a mutation handler: TanStack holds sync transactions until the
  persisting mutation settles, so waiting there deadlocks. `defineCollection`'s handlers
  already reconcile without waiting.

  **Synced writes wait for hydration.** A `SyncWrite` call made before the collection's first
  hydration finishes now waits until it does. This guards a TanStack bug in browsers: with the
  default `SingleProcessCoordinator`, a sync commit made during startup hydration never settles,
  and every collection on the shared persistence stays in `loading` forever.

  **Pass a `BrowserCollectionCoordinator`** to `createBrowserWASQLitePersistence` when more
  than one tab can open the same database. It coordinates writers across tabs and is not
  affected by the hydration bug. The docs and pi-demo now use it.

  **Breaking changes in TanStack DB to check in app code:**

  - `Collection.update` keys must match the collection's key type. Pass a `ModelId`
    (for example `ModelId.make(row.id)`), not a differently branded id.
  - `tx.isPersisted.promise` is deprecated. Use `await tx.when("settled")`.
  - In development, a second loaded copy of `@tanstack/db` now throws
    `DuplicateDbInstanceError` in browser bundles too.
  - Browser OPFS databases fail to open after 30 seconds by default. Pass `timeoutMs: 0` to
    `openBrowserWASQLiteOPFSDatabase` to keep the old unbounded wait.

  The SQLite storage migration only adds a table and columns, so persisted collections load
  without a rebuild.

- 22fbdfd: Upgrade to TanStack DB `0.9.2`.

  The `@tanstack/db` peer range moves to `^0.9.2` and `@tanstack/db-sqlite-persistence-core`
  to `^0.2.23`. The persistence packages pin `@tanstack/db` exactly, so upgrade `@tanstack/db`,
  `@tanstack/react-db` (`^0.4.1`), and `@tanstack/browser-db-sqlite-persistence` (`^0.2.23`)
  together.

  The library's exported types and behavior are unchanged, and the SQLite storage format is
  identical, so persisted collections load without a rebuild.

  A synced write whose SQLite persistence fails now logs a warning
  (`[liveCollection] sync transaction for "<id>" failed to persist`). TanStack's persistence
  wrapper stopped reporting these failures itself in this range.

  `useLiveQuery`'s dependency-array form is deprecated by `@tanstack/react-db` `0.3.0` and
  logs a development warning. Drop the array: the query's identity now derives from the query
  itself. The docs examples are updated.

## 4.1.0

### Minor Changes

- ed65ddd: Upgrade to Effect `4.0.0-rc.108`.

  The `effect` peer range moves to `^4.0.0-rc.108`. Upgrade Effect in lockstep; a
  workspace cannot mix beta and rc in one type graph.

  Renamed APIs, applied across all packages:

  - `Schema.TaggedErrorClass` is now `Schema.TaggedError`.
  - The `SchemaError` module folded into `Schema`; the decode failure type is
    `Schema.SchemaError`.
  - `SchemaRepresentation.fromAST` is now `SchemaRepresentation.toRepresentation`.

  Exported error classes keep their tags and fields, so `catchTag` call sites do
  not change.

  **One-time local rebuild on first load.** `deriveSchemaVersion` hashes Effect's
  AST representation, and that representation changed shape. Every collection
  derives a new version, so TanStack dumps and rebuilds the persisted local table,
  the journal mark is orphaned, and the mount decides `Snapshot` and re-lists from
  the server. This costs one full refetch per collection and then settles.

## 4.0.0

### Major Changes

- Version numbers now encode the Effect major this build targets. This release jumps from
  1.0.0 to 4.0.0 with **no code changes**: it is byte-identical to 1.0.0 apart from
  version metadata.

  These packages ship as two twins with the same names and the same public API, differing only in
  the Effect major they build against:

  | line      | versions         | install                                      |
  | --------- | ---------------- | -------------------------------------------- |
  | Effect v4 | `4.x` and upward | `pnpm add @triargos/live-collection`         |
  | Effect v3 | `3.x`, frozen    | `pnpm add @triargos/live-collection@effect3` |

  The `4.x` line only ever grows away from `3.x`, so a consumer range can never resolve across
  Effect majors, and the `effect` peer range fails at install time if the wrong twin is picked.

  Versions `0.0.1`-1.0.0 are Effect v4 builds whose numbers predate this scheme. They still work
  and are not unpublished; they are deprecated on npm pointing here.

## 1.0.0

### Minor Changes

- 530b9e9: Move shared runtime deps to peerDependencies.

  `effect`, `@tanstack/db`, `@tanstack/db-sqlite-persistence-core`,
  `@triargos/live-collection-protocol`, `@triargos/live-collection`, and `react` all appear
  in the public type surface, so duplicate installs broke `Context` tag identity and
  collection identity. They are now peers with caret ranges; install them alongside the
  library. `idb` stays an internal dependency, and `@tanstack/react-db` is no longer a
  runtime dependency of the React package (it was only used by a type test).

### Patch Changes

- Updated dependencies [dde4c65]
- Updated dependencies [530b9e9]
  - @triargos/live-collection-protocol@0.1.0

## 0.0.3

### Patch Changes

- f9d4506: fix(types): use different error generics for collection handlers to prevent type errors when mutation methods return different errors

## 0.0.2

### Patch Changes

- f64a352: fixed serialization of non-encodable types like dates and maps. use a schema codec to properly encode / decode them at the wire edges instead of letting the http client encode them

## 0.0.1

### Patch Changes

- e8a3b0a: First public beta of the live-collection package consortium.

  - `@triargos/live-collection-protocol` — shared contract kit: wire schemas, sync-group routing keys, resync targets, the pure squasher, model-registry types, and catchup schemas.
  - `@triargos/live-collection-server` — optional backend kernel: `SyncEventStore` port, event bus, persist-then-publish dispatcher, and `SyncFeed` (catchup + SSE frames).
  - `@triargos/live-collection` — the frontend library: registry/scoping, TanStack DB SQLite-WASM persistence, catchup/SSE adapters, `SyncBroker`, and runtime. Hero type: `LiveCollection<T>`.
  - `@triargos/live-collection-react` — optional React lifecycle bindings.

- 3452186: Rename `LastSyncIdStore` to `SyncCursor` (breaking): the service tag, `Shape` interface, and layers are now `SyncCursor`/`SyncCursorShape`. The durable `localStorage` key is unchanged, so existing clients keep their cursor. Internal module layout was also restructured (`core/` for shared identity primitives, `dispatch/` folded into `persistence/`, `defineCollection` hoisted to top level) — no other public API changes.
- Updated dependencies [e8a3b0a]
  - @triargos/live-collection-protocol@0.0.1
