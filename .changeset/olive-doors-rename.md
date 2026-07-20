---
"@triargos/live-collection": minor
---

Rename `LastSyncIdStore` to `SyncCursor` (breaking): the service tag, `Shape` interface, and layers are now `SyncCursor`/`SyncCursorShape`. The durable `localStorage` key is unchanged, so existing clients keep their cursor. Internal module layout was also restructured (`core/` for shared identity primitives, `dispatch/` folded into `persistence/`, `defineCollection` hoisted to top level) — no other public API changes.
