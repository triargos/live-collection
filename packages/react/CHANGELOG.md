# @triargos/live-collection-react

## 4.0.0

### Major Changes

- Version numbers now encode the Effect major this build targets. This release jumps from
  1.0.0 to 4.0.0 with **no code changes**: it is byte-identical to 1.0.0 apart from
  version metadata.

  These packages ship as two twins with the same names and the same public API, differing only in
  the Effect major they build against:

  | line | versions | install |
  | ---- | -------- | ------- |
  | Effect v4 | `4.x` and upward | `pnpm add @triargos/live-collection-react` |
  | Effect v3 | `3.x`, frozen | `pnpm add @triargos/live-collection-react@effect3` |

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

- Updated dependencies [530b9e9]
  - @triargos/live-collection@1.0.0

## 0.0.3

### Patch Changes

- Updated dependencies [f9d4506]
  - @triargos/live-collection@0.0.3

## 0.0.2

### Patch Changes

- Updated dependencies [f64a352]
  - @triargos/live-collection@0.0.2

## 0.0.1

### Patch Changes

- e8a3b0a: First public beta of the live-collection package consortium.

  - `@triargos/live-collection-protocol` — shared contract kit: wire schemas, sync-group routing keys, resync targets, the pure squasher, model-registry types, and catchup schemas.
  - `@triargos/live-collection-server` — optional backend kernel: `SyncEventStore` port, event bus, persist-then-publish dispatcher, and `SyncFeed` (catchup + SSE frames).
  - `@triargos/live-collection` — the frontend library: registry/scoping, TanStack DB SQLite-WASM persistence, catchup/SSE adapters, `SyncBroker`, and runtime. Hero type: `LiveCollection<T>`.
  - `@triargos/live-collection-react` — optional React lifecycle bindings.

- Updated dependencies [e8a3b0a]
- Updated dependencies [3452186]
  - @triargos/live-collection@0.0.1
