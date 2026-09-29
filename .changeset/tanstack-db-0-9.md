---
"@triargos/live-collection": minor
---

Upgrade to TanStack DB `0.9.2`.

The `@tanstack/db` peer range moves to `^0.9.2` and `@tanstack/db-sqlite-persistence-core`
to `^0.2.23`. The persistence packages pin `@tanstack/db` exactly, so upgrade `@tanstack/db`,
`@tanstack/react-db` (`^0.4.1`), and `@tanstack/browser-db-sqlite-persistence` (`^0.2.23`)
together.

The library's exported types and behavior are unchanged, and the SQLite storage format is
identical, so persisted collections load without a rebuild.

A synced write whose SQLite persistence fails now logs a warning
(`[liveCollection] sync transaction for "<id>" failed to persist`). TanStack's persistence
wrapper stopped reporting these failures itself in this range.

`useLiveQuery`'s dependency-array form is deprecated by `@tanstack/react-db` `0.3.0` and
logs a development warning. Drop the array: the query's identity now derives from the query
itself. The docs examples are updated.
