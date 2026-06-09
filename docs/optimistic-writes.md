# Optimistic writes (A.10)

**What this is.** The optimistic write path is how the UI *mutates* a live collection: `coll.insert(...)`/`coll.delete(...)` apply locally and instantly, an Effect-returning handler you supply talks to your backend and returns the confirmed row, and **the library folds that row into the synced store before the mutation resolves**. It is the write half of the library; the read half (catchup + SSE tail + replay) is documented in [architecture.md](./architecture.md). **You use it when** you wire `onInsert`/`onUpdate`/`onDelete` into `defineCollection` for a model whose rows users edit. The backend that those handlers call is yours — see [backend.md](./backend.md) and [protocol.md](./protocol.md) for the wire contract.

> Frontend-only, throughout. Nothing here runs server-side. The handler is an Effect that *calls* your server; the server's job is to accept the mutation, mint a `syncId`, and echo a `HydratedSyncEvent` back down the tail.

**The whole contract, in one line:** your handler does the server call and **returns the confirmed row** (insert/update) or `void` (delete). The library reconciles collection state for you — you never reach into `collection.utils`.

---

## The shape

Handlers are **optional fields on `defineCollection`** — there is no separate handler type to learn. They are TanStack DB's native mutation params (`InsertMutationFnParams` / `UpdateMutationFnParams` / `DeleteMutationFnParams`), but **Effect-returning**, carrying your app's `R`. Insert/update **return the server-confirmed row** (`Effect<T>`); delete returns `Effect<void>` (the library has the key already):

```ts
// packages/live-collection/src/registry/define-collection.ts:58
interface MutationHandlers<T extends object, R> {
  readonly onInsert?: (params: InsertMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<T, unknown, R>
  readonly onUpdate?: (params: UpdateMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<T, unknown, R>
  readonly onDelete?: (params: DeleteMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<void, unknown, R>
}
```

You write a **pure Effect** — `yield* SomeApi` — with no `runPromise` boilerplate. The library bridges it to the native Promise handler internally (`define-collection.ts:123`) **and reconciles the result** — folding the returned row into the synced baseline (insert/update) or removing the row by key (delete). The `R` you depend on is discharged by a `services` runtime (next section).

> **Insert/update must return `T` — there is no `void` opt-out.** Without the confirmed row the library can't reconcile, and an unreconciled optimistic row flickers (see [Model B](#model-b--why-the-library-reconciles-before-resolving)). If your server returns nothing, return the row you sent (`Effect.as(modified)`) — with client-minted ids that *is* the confirmed row. **One mutation per transaction:** the library reconciles `mutations[0]`; batched transactions are not reconciled (a future pass).

### `services` discharges `R`

The handler's `R` (and `listFn`'s `R`) is satisfied by a `ManagedRuntime` you pass as `services`. The field is **required iff `R ≠ never`** — a model whose handlers need no services may omit it. This is enforced at the type level:

```ts
// packages/live-collection/src/registry/define-collection.ts:69
type ServicesOf<R> = [R] extends [never]
  ? { readonly services?: ManagedRuntime.ManagedRuntime<never, never> }
  : { readonly services: ManagedRuntime.ManagedRuntime<R, never> }
```

`defineCollection<T, R>` infers `R` from `listFn` plus the handlers; `services` discharges it. The context is captured **once at define time** (`define-collection.ts:116-119`), so the handler runs against your services with no per-call wiring. `LiveRuntime` itself stays non-generic infra — `R` lives only on the collection definition.

### `SyncWrite<T>` — the reconcile surface (library-internal)

The reconcile the library performs for you writes through `collection.utils`, the **synced-store write path** — the same path the SSE dispatcher uses. This is `SyncWrite<T>`:

```ts
// packages/live-collection/src/dispatch/sync-write.ts:15
export interface SyncWrite<T> {
  /** Upsert one entity into the local baseline (insert if absent, replace if present). */
  readonly writeSynced: (entity: T) => Effect.Effect<void>
  /** Remove the entity with `id` from the local baseline. A no-op if it isn't present. */
  readonly deleteSynced: (id: ModelId) => Effect.Effect<void>
}
```

`writeSynced`/`deleteSynced` write to the **synced baseline** (confirmed server truth), not the optimistic overlay — they must not be rolled back. The hero type carries them in `utils` (`packages/live-collection/src/persistence/live-collection.ts:17`): `LiveCollection<T> = Collection<T, ModelId, SyncWrite<T>, never, T>`. **Your handler doesn't call these** — the library's `bridge` (`define-collection.ts:123`) calls `writeSynced(returnedRow)` after `onInsert`/`onUpdate` succeed, and `deleteSynced(key)` after `onDelete` succeeds. `params.collection` is still on the params if you ever need an escape hatch, but the happy path never touches it.

---

## Model B — why the library reconciles before resolving

You don't have to think about this — it's why the library reconciles inside `bridge` rather than leaving it to the SSE echo. The load-bearing rule the library upholds: **the confirmed row is written to the synced store before the mutation's Effect resolves.**

```
coll.insert(row)            UI: optimistic row visible instantly
  └─ onInsert(params)       your Effect runs on `services`
       └─ yield* api.create(...)        round-trip to your server → returns confirmed row
  └─ library reconcile      writeSynced(confirmed)   ← Model B: BEFORE the Promise resolves
  ⇒ Effect resolves         TanStack drops the completed optimistic tx
  ⇒ SSE echo arrives later  same row, same key ⇒ idempotent re-write (no-op)
```

Why "before"? In `@tanstack/db@0.6.7` (pinned alpha — **verify against your installed version**), a completed optimistic transaction is cleaned up the instant the handler's promise resolves; the optimistic row is only retained while that transaction is alive. If the library waited for the SSE echo to land the row (a "pure-echo" Model A), there would be a frame where the optimistic row is gone and the synced row hasn't arrived — a **flicker**. Folding the confirmed row in *before* resolving means the synced store already holds it at the instant TanStack drops the optimistic tx. The eventual SSE echo of the same row is then a redundant, idempotent `writeSynced`. This is exactly why insert/update **must return the row** — it's what the library reconciles.

---

## Client-minted ids (DEC-8)

Recommended: **mint the id on the client** before inserting. The UI in the playground does exactly this:

```tsx
// examples/playground/src/routes/WebhooksPage.tsx:27
coll.insert({ id: crypto.randomUUID(), orgId, url }) // client-minted id
```

Because the id is the app's and the server preserves it, the SSE self-echo carries the **same key** as the row you already confirmed — so it collapses to an idempotent `writeSynced` and **no `clientId` / echo-suppression filter is needed** (this is what validates DEC-8). Server-assigned ids also work (you'd swap a temp id for the real one in the handler), but client-minted keeps the self-echo trivially idempotent and avoids the swap.

---

## Failure ⇒ rollback

A handler is an Effect. If it **fails** (a `Schema.TaggedError`, or any error in the channel), the bridged Promise rejects and **TanStack rolls the optimistic write back** — the row that appeared instantly disappears. The reconcile runs only on success (it's chained after the handler with `Effect.flatMap`), so a failed handler never reaches `writeSynced`. Model your backend's "no" as a tagged error and `Effect.fail` it; never `throw`, never `new Error` across the boundary. The playground's fake server does this with `BackendRejected`:

```ts
// examples/playground/src/live/shared-backend.ts:30
export class BackendRejected extends Schema.TaggedError<BackendRejected>()("BackendRejected", {
  operation: Schema.Literal("create", "delete"),
  id: Schema.String,
}) {}
```

The integration test pins the rollback behavior: an `onInsert` returning `Effect.fail("server rejected")` leaves the optimistic row visible immediately, then gone once the handler rejects (`packages/live-collection/test/write-path.integration.test.ts`).

---

## Worked example — the playground

The playground wires a scoped `Webhook` collection. The handlers `yield*` an Effect service (`WebhookApi`, discharged by `services`) and return the result — the library reconciles:

```ts
// examples/playground/src/live/playground.ts:52
const webhooks = defineCollection({
  runtime,
  services: backend.services,               // ManagedRuntime<WebhookApi, never> — discharges R
  entity: "Webhook",
  schema: Webhook,
  getKey: webhookKey,
  scopeOf: (w) => w.orgId,
  listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)),
  // Handlers only call the server and return the confirmed row / void — the library reconciles.
  onInsert: ({ transaction }) =>
    Effect.flatMap(WebhookApi, (api) => api.create(transaction.mutations[0]!.modified)),
  onDelete: ({ transaction }) =>
    Effect.flatMap(WebhookApi, (api) => api.remove(transaction.mutations[0]!.key)),
})
```

The service it discharges is a plain `Context.Tag` seam — in production this would be your HTTP client; failures are modeled, not thrown:

```ts
// examples/playground/src/live/shared-backend.ts:20
export class WebhookApi extends Context.Tag("WebhookApi")<
  WebhookApi,
  {
    readonly create: (w: Webhook) => Effect.Effect<Webhook, BackendRejected>
    readonly remove: (id: ModelId) => Effect.Effect<void, BackendRejected>
    readonly list: (orgId: string) => Effect.Effect<ReadonlyArray<Webhook>>
  }
>() {}
```

The UI never sees any of this. It reads the native collection and writes through it directly:

```tsx
// examples/playground/src/routes/WebhooksPage.tsx:20
const coll = pg.webhooks(orgId)
const { data } = useLiveQuery(() => coll, [orgId])
// insert:
coll.insert({ id: crypto.randomUUID(), orgId, url })
// delete:
coll.delete(webhookKey(w))   // webhookKey is the boundary mapper (raw id → ModelId)
```

`pg.webhooks(orgId)` is a `ScopedHandle<Webhook>` — calling it mounts (or returns) the per-`orgId` instance from the registry. Branded ids (`ModelId`) are minted only at boundary mappers like `webhookKey` (`examples/playground/src/live/schema.ts`), never cast inside the app.

---

## The full round-trip

Putting it together for one insert:

1. `coll.insert(row)` — optimistic row visible **instantly**.
2. `onInsert` runs on your `services` runtime; `yield* api.create(row)` and **returns the confirmed row**.
3. Server accepts, mints a `syncId`, returns the confirmed row.
4. The library calls `writeSynced(confirmed)` — synced baseline now holds it (**Model B**).
5. Handler Effect resolves ⇒ TanStack drops the optimistic tx; no flicker (the synced row is already there).
6. The server echoes a `HydratedSyncEvent` down the SSE tail; the dispatcher decodes it and calls `writeSynced` again — **same key, idempotent no-op**.
7. The persisted (synced) store flushes to OPFS; the row survives reload.

Steps 6–7 are the read path — see [architecture.md](./architecture.md) and the wire shape in [protocol.md](./protocol.md). The decode at step 6 happens against the protocol's `HydratedSyncEvent` schema at the client boundary; the wire shape is never cast.

---

## Not built (and why)

- **Offline-durable writes — deferred.** Persisted collections persist the *synced* store, not the optimistic overlay, so a write made offline does not survive reload today. A durable offline queue needs a **separate mutation log** (replayed on reconnect), which is a future pass. (`@tanstack/offline-transactions` does not exist — this path is built on the native handlers above, not a phantom dep.)
- **`clientId` / echo-suppression — removed (DEC-8/protocol DEC-11).** Not needed: client-minted ids make the self-echo idempotent, so there is no server-side originator filter on events, `SyncContext`, or the HTTP contract.

---

### Quick reference

| Thing | Where | Note |
|---|---|---|
| `onInsert`/`onUpdate`/`onDelete` | `define-collection.ts:58` | optional, Effect-returning, carry `R`; insert/update return `T`, delete returns `void` |
| `services` requirement | `define-collection.ts:69` | required iff `R ≠ never` |
| library reconcile (`bridge`) | `define-collection.ts:123` | folds the returned row / deletes by key — apps don't call `utils` |
| `writeSynced` / `deleteSynced` | `sync-write.ts:15` | the synced-baseline reconcile surface (library-internal) |
| Model B (confirm-before-resolve) | this doc | required by TanStack tx-cleanup timing (0.6.7) |
| Rollback on failure | `write-path.integration.test.ts` | handler `Effect.fail` ⇒ optimistic row removed |
| Client-minted id | `WebhooksPage.tsx:27` | makes the self-echo idempotent (DEC-8) |

Versions that matter: `@tanstack/db` is pinned at **0.6.7** (alpha); the browser persistence adapter at **0.1.11**. The persistence/mutation surface shifts between alphas — **verify signatures against your installed version.**
