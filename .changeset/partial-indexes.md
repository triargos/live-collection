---
"@triargos/live-collection-protocol": minor
"@triargos/live-collection-server": minor
"@triargos/live-collection": minor
"@triargos/live-collection-react": minor
---

Partial indexes: load keyed subsets of a model on demand and keep them live.

A model's cost becomes proportional to what the user opens instead of table size.
The server declares a closed set of index keys per model — no predicate language —
and the client asks for one key value at a time.

`defineCollection` gains a third variant next to global and scoped. `partial.by`
replaces `listFn` (the batch endpoint is the snapshot path) and excludes `scopeOf`;
each key generates one flat ensure, `templateId` → `utils.loadByTemplateId`:

```ts
defineCollection({
  entity: "SelectionTemplateValue",
  partial: { by: { templateId: (v) => v.templateId } },
  // ...
})
```

`loadBy*` is an idempotent ensure, not a query and not a lease. It decides between
skip (durable coverage mark is current), replay (journal still holds the gap, no
network), and snapshot (`POST /sync/batch`). Calls in one microtask window coalesce
into a single batch request across keys and collections.

New public API:

- **protocol** — `HydrateBatchRequest`, `HydrateBatchResponse`, `HydrateBatchResult`,
  `IndexValue`; `indexes` on the model registry entry.
- **server** — `SyncFeed.hydrateBatch`, `UnknownIndexError` (⇒ 400 at the app's route).
- **live-collection** — `HydrateClient` (`layer({ url })`), the `partial` collection
  variant, coverage tracking, and `SyncJournal.subsetMarks`.
- **react** — `usePartialLoad` and `SubsetStatus` (`Loading | Ready | Forbidden | Failed`,
  `Failed` carrying a `retry` handle).

Apps without partial collections change nothing and omit `HydrateClient`; a `loadBy*`
snapshot without it is a defect with a clear message. Reads stay `useLiveQuery`, and
writes keep the existing optimistic handlers.

**No local rebuild.** The journal's last-applied record gained optional `scope` and
`subset` fields, so records written by earlier versions still decode. Schema versions
are untouched.

Known residual: a write committing during an in-flight snapshot fetch can flicker once
and self-heals. There is no subset eviction — rows and marks stay for the session.
See `docs/partial-indexes.md`.
