---
"@triargos/live-collection": minor
"@triargos/live-collection-react": minor
---

Upgrade to TanStack DB `0.12.1`.

The `@tanstack/db` peer range moves to `^0.12.1`, `@tanstack/db-sqlite-persistence-core` to
`^0.4.5`, and `@tanstack/react-db` to `^0.5.5`. Upgrade `@tanstack/browser-db-sqlite-persistence`
to `^0.2.30` with them; the persistence packages pin `@tanstack/db` exactly.

**Synced writes no longer wait for hydration.** `db-sqlite-persistence-core@0.4.5` fixes the
`SingleProcessCoordinator` bug where a sync commit made during startup hydration never settled,
so the guard that held `SyncWrite` calls until the collection's first hydration is removed. A
synced write now commits as soon as the collection's sync has started.
