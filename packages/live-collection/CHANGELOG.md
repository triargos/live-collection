# @triargos/live-collection

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
