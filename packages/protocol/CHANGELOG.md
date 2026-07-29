# @triargos/live-collection-protocol

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
