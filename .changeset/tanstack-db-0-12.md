---
"@triargos/live-collection": minor
---

Upgrade to TanStack DB `0.12.0`.

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
hydration finishes now waits until it does. In browsers, a sync commit made while TanStack's
SQLite persistence is still hydrating never settles and stalls every collection on the shared
persistence. Collections stayed in `loading` forever.

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
