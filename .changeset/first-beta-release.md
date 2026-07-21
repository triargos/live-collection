---
"@triargos/live-collection-protocol": patch
"@triargos/live-collection-server": patch
"@triargos/live-collection": patch
"@triargos/live-collection-react": patch
---

First public beta of the live-collection package consortium.

- `@triargos/live-collection-protocol` — shared contract kit: wire schemas, sync-group routing keys, resync targets, the pure squasher, model-registry types, and catchup schemas.
- `@triargos/live-collection-server` — optional backend kernel: `SyncEventStore` port, event bus, persist-then-publish dispatcher, and `SyncFeed` (catchup + SSE frames).
- `@triargos/live-collection` — the frontend library: registry/scoping, TanStack DB SQLite-WASM persistence, catchup/SSE adapters, `SyncBroker`, and runtime. Hero type: `LiveCollection<T>`.
- `@triargos/live-collection-react` — optional React lifecycle bindings.
