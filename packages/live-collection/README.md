# `@triargos/live-collection`

A **frontend-only**, Effect-native live-sync engine built on [TanStack DB](https://tanstack.com/db). The hero type is `LiveCollection<T>` — a *native* TanStack collection that persists locally (SQLite/OPFS), hydrates from disk on reload, and stays live against your backend over SSE + catchup.

**How you use it:** you define one collection per model, point it at your backend's read path (a `listFn`) and optionally its write path (optimistic `onInsert`/`onDelete`/`onUpdate` handlers), then start one shared broker near your app root. Each mounted collection subscribes itself. Reads are plain `useLiveQuery`; the engine owns persistence, catchup, the durable cursor, and replay-on-mount.

> **The backend is yours.** This library ships no server. It speaks a wire contract — the SSE stream, the `/catchup` request/response, and the squasher fold — defined in [`@triargos/live-collection-protocol`](https://www.npmjs.com/package/@triargos/live-collection-protocol). You implement the routes, auth, and permission resolution; an optional Effect backend kernel is [`@triargos/live-collection-server`](https://www.npmjs.com/package/@triargos/live-collection-server).

## Install

```bash
pnpm add @triargos/live-collection @triargos/live-collection-protocol
pnpm add @triargos/live-collection-react   # React apps only
```

## The package family

The dependency DAG is acyclic: `protocol → live-collection → react`, plus `protocol → server`.

| Package | What it is |
|---|---|
| [`@triargos/live-collection-protocol`](https://www.npmjs.com/package/@triargos/live-collection-protocol) | The shared contract kit — pure, no I/O. Wire schemas, sync-group grammar, squasher, `/catchup` schemas. |
| **`@triargos/live-collection`** | The frontend engine. `defineCollection`, `makeLiveRuntime`, SSE transport, catchup, persistence, the durable `SyncJournal`. |
| [`@triargos/live-collection-react`](https://www.npmjs.com/package/@triargos/live-collection-react) | One React-specific piece: `useLiveSync`. Reads use `@tanstack/react-db` directly. |
| [`@triargos/live-collection-server`](https://www.npmjs.com/package/@triargos/live-collection-server) | Optional Effect backend kernel enforcing the contract's invariants. |

## Documentation

Full quick start, worked examples, and in-depth guides live in the repository:

- [Quick start](https://github.com/triargos/live-collection#quick-start)
- [Collections](https://github.com/triargos/live-collection/blob/main/docs/collections.md) — `defineCollection`, global vs scoped, the `LiveCollection<T>` surface.
- [Persistence](https://github.com/triargos/live-collection/blob/main/docs/persistence.md) — the local-SQLite durability base.
- [Optimistic writes](https://github.com/triargos/live-collection/blob/main/docs/optimistic-writes.md) — Effect-returning write handlers and reconciliation.
- [Architecture](https://github.com/triargos/live-collection/blob/main/docs/architecture.md) — package boundaries, `makeLiveRuntime`, the sync loop.
- [Backend contract](https://github.com/triargos/live-collection/blob/main/docs/backend.md) — what your server must provide.

## License

MIT
