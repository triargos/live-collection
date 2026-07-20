# Documentation

`@triargos/live-collection` is a **frontend-only** Effect + TanStack DB live-sync engine. The hero
type is `LiveCollection<T>` — a *native* TanStack collection that persists locally, hydrates from disk
on reload, and stays live against your backend over SSE + catchup. These docs are written for the
developer **using** the library in an app; the backend is yours, and [`backend.md`](./backend.md)
specifies what it must provide.

New here? Start with the root [`README.md`](../README.md) for install + a quick start, then come back
for depth.

## Using the library

- [`collections.md`](./collections.md) — `defineCollection` (global vs scoped overloads + `scopeOf`),
  the registry-backed handle, the structured `CollectionKey {entity, scope}`, and the
  `LiveCollection<T>` surface you read with `useLiveQuery`.
- [`persistence.md`](./persistence.md) — the local-SQLite durability base: building the `persistence`
  value, browser OPFS vs node, schema versioning, and the A.3 hydrate-from-disk gate.
- [`optimistic-writes.md`](./optimistic-writes.md) — the write half: Effect-returning
  `onInsert`/`onUpdate`/`onDelete` handlers that return the confirmed row, library-side reconciliation
  (Model B, no flicker), client-minted ids, and rollback on failure.
- [`react.md`](./react.md) — the one React-specific piece: `useLiveSync` to fork the loop near your
  app root, and reading collections with `useLiveQuery`.

## Under the hood

- [`architecture.md`](./architecture.md) — the package boundaries and DAG, `makeLiveRuntime`, the two
  execution surfaces, the seam convention, and the codebase-wide conventions.
- [`read-path.md`](./read-path.md) — the forever-running sync loop: catchup from a durable cursor,
  then tail SSE; the transport, the durable cursor, and reconnect/resync behaviour.
- [`replay-on-mount.md`](./replay-on-mount.md) — how a late-mounted collection heals itself:
  skip / replay / bootstrap from the durable event log.

## The wire contract & your backend

- [`protocol.md`](./protocol.md) — the shared, pure-`effect` contract kit: sync-event schemas, the
  sync-group grammar, resync targets, branded ids, the squasher, and the `/catchup` schemas. Both the
  frontend and your backend depend on it.
- [`backend.md`](./backend.md) — the contract your server must satisfy: the `/catchup` and SSE
  endpoints the client calls, and the invariants (cursor semantics, epoch, resync, no echo
  suppression) its correctness depends on.
