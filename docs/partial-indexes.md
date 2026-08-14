# Partial indexes

Load keyed subsets of a model on demand instead of syncing the whole table — and keep
them live. A model's cost becomes proportional to what the user opens, not to table
size: 300 templates × 40 values is 12,000 rows nobody wants at bootstrap, but "the
values of the template I just opened" is 40 rows, one request, live afterwards.

The server's vocabulary is a **closed, declared set of index keys** per model — no
predicate language. The client asks "give me all `SelectionTemplateValue` where
`templateId = t1`"; the server answers only for keys its registry declares.

## Define a partial collection

`partial` is a third `defineCollection` variant, next to global and scoped. It
**replaces `listFn`** (the batch endpoint is the snapshot path) and excludes `scopeOf`:

```ts
export const templateValues = defineCollection({
  runtime,
  entity: "SelectionTemplateValue",
  schema: SelectionTemplateValue,
  getKey: (v) => v.id,
  partial: {
    by: {
      templateId: (v) => v.templateId,   // client-side membership extractor
    },
  },
  // optimistic writes: unchanged, same handlers as any collection
  onInsert: ({ transaction }) =>
    Effect.flatMap(ValuesApi, (api) => api.create(transaction.mutations[0].modified)),
  services,
})
```

Each key under `partial.by` generates one flat `utils` method: `templateId` →
`utils.loadByTemplateId`. The key must match what the server registry declares under
`indexes` — drift between the two is caught loudly at runtime (`UnknownIndexError` →
400), never a silent empty result.

## Load a subset — three call sites, same call

```ts
// (a) route loader — warms the subset before render
loader: ({ params }) => templateValues().utils.loadByTemplateId(params.templateId)

// (b) imperative, outside React
await templateValues().utils.loadByTemplateId("t1")

// (c) React — usePartialLoad, below
```

`loadBy*` is an **idempotent ensure**, not a query and not a lease. It resolves when
the subset's local rows are current, deciding between three tiers — only one hits the
wire:

- **Skip** — the subset's durable mark says nothing happened since the last load ⇒
  resolve immediately. The common case after first load, because marks keep advancing
  while events stream in.
- **Replay** — the mark is behind, but the local journal still holds every event after
  it ⇒ re-apply from disk. No network. This is how a page reload catches a subset up
  without refetching it.
- **Snapshot** — the rows can't be trusted (first load ever, journal pruned past the
  gap, or a server-declared resync) ⇒ `POST /sync/batch` fetches the subset fresh.

More behavior:

- Several calls in one microtask window — different keys, even different collections —
  coalesce into **one** batch request. Duplicate in-flight calls share one promise.
- Rejects with `SubsetForbidden` when the server refuses visibility; no mark is
  written, the next call retries. Rejects with `HydrateFailed` on network trouble.
- There is no unload. Rows and marks stay; session memory is the union of subsets the
  user opened. (Eviction is a deliberate deferral — see the design doc.)

## Read in React

Reads stay exactly what they are for every collection: `useLiveQuery` from
`@tanstack/react-db`. The only new hook is the ensure:

```tsx
function TemplateEditor({ templateId }: { templateId: string }) {
  const values = templateValues()

  const subset = usePartialLoad(values, { templateId })

  const { data } = useLiveQuery(
    (q) => q.from({ v: values }).where(({ v }) => eq(v.templateId, templateId)),
    [templateId],
  )

  return SubsetStatus.$match(subset, {
    Loading:   () => (data.length === 0 ? <Spinner /> : <ValueList values={data} />), // persisted rows: stale-while-revalidate
    Ready:     () => <ValueList values={data} />,   // live: SSE inserts/updates/deletes land here
    Forbidden: () => <NoAccess />,
    Failed:    ({ error, retry }) => <RetryBanner error={error} onRetry={retry} />,
  })
}
```

- The subset argument names exactly one index key from `partial.by`; a key outside it —
  or a two-key literal — is a compile error.
- `SubsetStatus` is a Result-shaped tagged union: `Loading | Ready | Forbidden |
  Failed`, each failure carrying its typed error. `Failed` has a `retry` handle — not
  an eternal spinner.
- `usePartialLoad` re-runs the ensure when the value changes; after the first load
  that's a Skip, so it's free. With a route loader warming the subset, the hook
  resolves `Ready` immediately.

**The completeness contract** (documented, not enforced): a query over a partial
collection is only complete for subsets you have ensured. Ensure and `where` on the
same key — the pairing above is the pattern.

## Live behavior while mounted

All automatic:

- Colleague adds a value to an ensured template → SSE Insert → appears live.
- Value moves from ensured t1 to ensured t2 → updated in place; to an un-ensured
  template → **deleted** live (delete-on-mismatch).
- Value moves from an un-ensured template into an ensured one → inserted live.
- Devices that never call `loadBy*` never pay a byte for the model's rows.

Writes are unchanged: optimistic mutation → handler returns the confirmed row → folded
into the synced baseline → the SSE echo is an idempotent no-op. Known residual: a write
committing during an in-flight snapshot fetch can flicker once and self-heals; the
write barrier that closes it is deferred.

## Server — declare the vocabulary, mount the route

```ts
export const RegistryLayer = ModelRegistry.layer(Effect.gen(function* () {
  const values = yield* SelectionTemplateValueRepo
  return defineModelRegistry({
    SelectionTemplateValue: {
      modelName: "SelectionTemplateValue",
      schema: SelectionTemplateValue,
      hydrate: (id, groups) => values.findVisible(id, groups),
      indexes: {
        // Option.none() ⇒ Forbidden (visibility refusal) — mirrors hydrate exactly.
        // Option.some([]) ⇒ valid empty membership.
        templateId: (keyValue, groups) => values.listByTemplate(keyValue, groups),
      },
    },
  })
}))
```

The route is app-owned, like `/catchup`: decode `HydrateBatchRequest` →
`SyncFeed.hydrateBatch` → encode `HydrateBatchResponse`; `UnknownIndexError` ⇒ 400.
The kernel reads the log head **before** running the index queries and stamps it on the
response — that stamp is the safe floor each subset's coverage mark starts from.

## Runtime wiring — one new layer

```ts
const runtime = makeLiveRuntime({
  persistence,
  sync: Layer.mergeAll(
    SyncTransport.layer({ url: "/api/sync/stream" }),
    CatchupClient.layer({ url: "/api/sync/catchup" }),
    HydrateClient.layer({ url: "/api/sync/batch" }),   // required iff any collection is partial
    SyncJournal.layer(),
  ).pipe(Layer.provide(FetchHttpClient.layer)),
})
```

Apps without partial collections omit `HydrateClient` and nothing changes. A `loadBy*`
snapshot with no `HydrateClient` provided is a defect with a clear message.

## How it stays correct

- **Coverage marks.** Each ensured subset has a durable mark — "my rows reflect the
  server through position N" — stored as a widened last-applied record in the sync
  journal. Marks advance with every applied signal (an event that didn't touch a subset
  still proves it current through that syncId) and participate in journal pruning.
- **The stamp.** `HydrateBatchResponse.lastSyncId` is the server head read before the
  index queries — every fetched row reflects at least that position. The client lands
  the slice, then replays journal events above the stamp; a re-applied event is an
  idempotent no-op.
- **Slice replace.** A refetched subset lands as delete + upsert in one synced
  transaction (`patchSynced`), so rows deleted server-side disappear — a merge could
  never remove them.
- **Resync.** A server-declared resync invalidates every subset marked before it: rows
  and coverage are dropped, and each subset's next ensure refetches.

## See also

- [Architecture](./architecture.md) — the broker, journal, and mount replay this builds on.
- [Protocol reference](./protocol.md#partial-index-batch-schemas) — the batch wire schemas.
- `plans/partial-index-loading.md` — the full design (decisions, deferrals: write barrier, eviction).
