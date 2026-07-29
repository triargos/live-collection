# @triargos/live-collection-server

## 4.0.0

### Major Changes

- Version numbers now encode the Effect major this build targets. This release jumps from
  1.0.0 to 4.0.0 with **no code changes**: it is byte-identical to 1.0.0 apart from
  version metadata.

  These packages ship as two twins with the same names and the same public API, differing only in
  the Effect major they build against:

  | line | versions | install |
  | ---- | -------- | ------- |
  | Effect v4 | `4.x` and upward | `pnpm add @triargos/live-collection-server` |
  | Effect v3 | `3.x`, frozen | `pnpm add @triargos/live-collection-server@effect3` |

  The `4.x` line only ever grows away from `3.x`, so a consumer range can never resolve across
  Effect majors, and the `effect` peer range fails at install time if the wrong twin is picked.

  Versions `0.0.1`-1.0.0 are Effect v4 builds whose numbers predate this scheme. They still work
  and are not unpublished; they are deprecated on npm pointing here.

## 1.0.0

### Minor Changes

- dde4c65: feat!: the event bus exposes a stream instead of a subscription handle

  `SyncEventBusShape.subscribe: Effect<PubSub.Subscription<SyncEvent>, never, Scope>`
  becomes `SyncEventBusShape.events: Stream.Stream<SyncEvent>`. The port no longer
  leaks PubSub into the interface an app implements, and the subscription lifetime
  moves inside the stream — so a Redis or Postgres-`LISTEN` adapter needs no scope
  story of its own. The sole consumer already unwrapped the handle into a stream on
  the next line.

  `SyncFeed.streamEvents` consequently returns `Stream.Stream<string>` with no
  `Scope` requirement, so SSE routes can drop `Stream.provideContext(requestScope)`:

  ```diff
  -const requestScope = yield* Effect.context<Scope.Scope>()
   return HttpServerResponse.stream(
  -  feed.streamEvents({ syncGroups }).pipe(Stream.provideContext(requestScope), Stream.encodeText),
  +  feed.streamEvents({ syncGroups }).pipe(Stream.encodeText),
     { contentType: "text/event-stream" },
   )
  ```

  Custom bus adapters must return a stream that subscribes per run and releases on
  termination, not a shared already-subscribed stream.

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

### Patch Changes

- Updated dependencies [dde4c65]
- Updated dependencies [530b9e9]
  - @triargos/live-collection-protocol@0.1.0

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

- Updated dependencies [e8a3b0a]
  - @triargos/live-collection-protocol@0.0.1
