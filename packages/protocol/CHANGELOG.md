# @triargos/live-collection-protocol

## 4.2.0

### Minor Changes

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
  0.1.0 to 4.0.0 with **no code changes**: it is byte-identical to 0.1.0 apart from
  version metadata.

  These packages ship as two twins with the same names and the same public API, differing only in
  the Effect major they build against:

  | line      | versions         | install                                               |
  | --------- | ---------------- | ----------------------------------------------------- |
  | Effect v4 | `4.x` and upward | `pnpm add @triargos/live-collection-protocol`         |
  | Effect v3 | `3.x`, frozen    | `pnpm add @triargos/live-collection-protocol@effect3` |

  The `4.x` line only ever grows away from `3.x`, so a consumer range can never resolve across
  Effect majors, and the `effect` peer range fails at install time if the wrong twin is picked.

  Versions `0.0.1`-0.1.0 are Effect v4 builds whose numbers predate this scheme. They still work
  and are not unpublished; they are deprecated on npm pointing here.

## 0.1.0

### Minor Changes

- dde4c65: feat!: `narrowModelName` returns `Option`, and `UnknownModelError` is removed

  An unregistered model name is the routine outcome of talking to a newer backend,
  not a failure — every caller already discarded the error and dropped the event.
  Modelling it as `Option.Option<N>` says that directly, and drops a tagged error
  whose `known` payload nothing ever read.

  ```diff
  -Result.match(narrowModelName(knownNames, event.modelName), {
  -  onFailure: () => Effect.logDebug(`skipping ${event.modelName}`),
  -  onSuccess: (name) => dispatch(registry[name], event),
  -})
  +Option.match(narrowModelName(knownNames, event.modelName), {
  +  onNone: () => Effect.logDebug(`skipping ${event.modelName}`),
  +  onSome: (name) => dispatch(registry[name], event),
  +})
  ```

- 530b9e9: Move shared runtime deps to peerDependencies.

  `effect`, `@tanstack/db`, `@tanstack/db-sqlite-persistence-core`,
  `@triargos/live-collection-protocol`, `@triargos/live-collection`, and `react` all appear
  in the public type surface, so duplicate installs broke `Context` tag identity and
  collection identity. They are now peers with caret ranges; install them alongside the
  library. `idb` stays an internal dependency, and `@tanstack/react-db` is no longer a
  runtime dependency of the React package (it was only used by a type test).

## 0.0.1

### Patch Changes

- e8a3b0a: First public beta of the live-collection package consortium.

  - `@triargos/live-collection-protocol` — shared contract kit: wire schemas, sync-group routing keys, resync targets, the pure squasher, model-registry types, and catchup schemas.
  - `@triargos/live-collection-server` — optional backend kernel: `SyncEventStore` port, event bus, persist-then-publish dispatcher, and `SyncFeed` (catchup + SSE frames).
  - `@triargos/live-collection` — the frontend library: registry/scoping, TanStack DB SQLite-WASM persistence, catchup/SSE adapters, `SyncBroker`, and runtime. Hero type: `LiveCollection<T>`.
  - `@triargos/live-collection-react` — optional React lifecycle bindings.
