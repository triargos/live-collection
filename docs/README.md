# Documentation

`@triargos/live-collection` gives you local-first live collections for Effect + TanStack DB: native collections that persist to local SQLite, hydrate from disk on reload, and stay in sync with your backend over SSE + catchup.

**Start with [getting started](./getting-started.md)** — the step-by-step guide from backend endpoints to a rendered, writable list.

## Guides

- [Getting started](./getting-started.md) — the implementer walkthrough: what to build where, and what's expected of each piece.
- [Collections](./collections.md) — `defineCollection` in full: scoped vs global, optimistic writes, reading, lifecycle.
- [Persistence](./persistence.md) — local SQLite setup, schema versioning, Vite config, multi-tab behavior.
- [React](./react.md) — `useLiveSync` and reading with `useLiveQuery`.

## Under the hood

- [Architecture](./architecture.md) — the sync loop, the durable journal, replay-on-mount, pruning, resync, and epochs.

## Your backend

- [Backend contract](./backend.md) — the two endpoints your server provides and the invariants the client relies on. Stack-agnostic; Effect backends can use [`@triargos/live-collection-server`](../packages/server/README.md).
- [Protocol reference](./protocol.md) — every wire schema, the sync-group grammar, resync targets, and the squasher.

## Example

[`examples/pi-demo`](../examples/pi-demo) is the complete picture: an Effect backend on the server kernel, a React SPA with scoped collections and live joins, OPFS persistence, and cross-device sync.
