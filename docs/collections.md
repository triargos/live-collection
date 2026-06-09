# Collections

**What this is.** A *collection* is one model's live, persisted, queryable working set on the
client — a native TanStack DB `Collection` (`LiveCollection<T>`) that the UI reads with
`useLiveQuery`. You declare one with `defineCollection`, which hands back a **registry-backed
handle**: a function you call to mount-or-fetch the canonical instance for a given scope.

**How you use it.** You write `defineCollection` once per model at app wiring time, then call
the handle wherever the UI needs the collection (typically inside a component, passed to
`useLiveQuery`). Reach for the registry's `dispose*` methods only at lifecycle boundaries —
switching workspaces, logging out. The sync loop ([./read-path.md](./read-path.md)) drives every
mounted instance through the registry; you never wire instances into it by hand.

This is the **client** library. The backend that serves catchup/SSE and accepts writes is your
responsibility — see [./backend.md](./backend.md) and [./protocol.md](./protocol.md) for the wire
contract, and [./architecture.md](./architecture.md) for how the pieces compose.

---

## `defineCollection` — the typed skin

`defineCollection` binds a model to a `LiveRuntime` and returns a handle. It has **two overloads**,
split on whether `scopeOf` is present
([`define-collection.ts:103-104`](../packages/live-collection/src/registry/define-collection.ts#L103)):

```ts
// global: no scopeOf  → handle is () => LiveCollection<T>
export function defineCollection<T extends object, R = never>(config: GlobalConfig<T, R>): GlobalHandle<T>
// scoped: scopeOf present → handle is (scope: string) => LiveCollection<T>
export function defineCollection<T extends object, R = never>(config: ScopedConfig<T, R>): ScopedHandle<T>
```

The split is load-bearing. A single optional `scopeOf?` would infer `Args = unknown` and force a
phantom argument onto global call sites; the two overloads keep `webhookCollection()` and
`webhookCollection(orgId)` each exactly as wide as they need to be (DEC-R10).

### Shared config

Both overloads take ([`define-collection.ts:73-89`](../packages/live-collection/src/registry/define-collection.ts#L73)):

| field | type | role |
|-------|------|------|
| `runtime` | `LiveRuntime` | the two-surface runtime from `makeLiveRuntime` ([./architecture.md](./architecture.md)) |
| `entity` | `string` | the **wire model name** — written once, so the registry key and the persisted table id (`serializeKey(key)`) can never drift |
| `schema` | `Schema.Schema<T, any, never>` | the row schema; drives the schema-version / dump-and-rebuild path |
| `getKey` | `(entity: T) => ModelId` | the row's primary key |
| `listFn` | snapshot source (see below) | the cold/resync list; bridged to `R = never` at define time |

Optional **optimistic write handlers** — `onInsert` / `onUpdate` / `onDelete` — are
Effect-returning TanStack mutation handlers
([`define-collection.ts:58`](../packages/live-collection/src/registry/define-collection.ts#L58)).
Each handler **only calls your server**: insert/update return the confirmed row (`Effect<T>`), delete
returns `Effect<void>`. The **library** reconciles — folding the returned row into the synced store
(`writeSynced`), or removing the row by key (`deleteSynced`), **before resolving** (Model B), so the
synced store holds the row at the instant TanStack drops the optimistic transaction — no flicker, and
the later SSE echo is an idempotent `writeSynced`. Apps never touch `collection.utils`. See
[./optimistic-writes.md](./optimistic-writes.md) for the full path.

`R` is inferred from `listFn` plus the handlers. If `R ≠ never`, you must pass `services` — a
`ManagedRuntime` that discharges it; `defineCollection` captures its context once at define time and
provides it into `listFn` and every handler, so they reach the loop as `R = never`
([`define-collection.ts:69-71`, `116-126`](../packages/live-collection/src/registry/define-collection.ts#L69)).

### Global vs scoped, in the config

- **Global** (`GlobalBase`): no `scopeOf`; `listFn: Effect.Effect<ReadonlyArray<T>, never, R>` — one
  instance app-wide (e.g. the current user).
- **Scoped** (`ScopedBase`): `scopeOf: (entity: T) => string` and
  `listFn: (scope: string) => Effect.Effect<ReadonlyArray<T>, never, R>` — one instance per scope.
  `scopeOf` is the **only** place your app's "workspace" notion enters the library; the library
  itself stays scope-generic (DEC-R6 / DEC-R10). The dispatcher reads the scope straight off each
  decoded event via `scopeOf`.

> The handle also carries `_meta` (`ModelMeta<T>`) — `entity`, `schema`, `getKey`, `scopeOf`,
> `listFn` — which is what the `SyncMap` and sync loop read. You pass the handle into the `SyncMap`;
> the loop reaches *instances* through the registry, never by calling the handle
> ([`define-collection.ts:25-46`](../packages/live-collection/src/registry/define-collection.ts#L25)).

---

## The handle and `CollectionKey`

Calling the handle mounts through the registry — **synchronously** — and returns the native
`LiveCollection<T>` ([`define-collection.ts:150-151`](../packages/live-collection/src/registry/define-collection.ts#L150)).
After the first mount, the call is a `Map.get`: cheap and referentially stable, so calling it inline
in render is fine.

Identity is a **structured** `CollectionKey<A>`, not a string id
([`collection-key.ts:15-19`](../packages/live-collection/src/registry/collection-key.ts#L15)):

```ts
export interface CollectionKey<A> {
  readonly entity: string
  readonly scope: Option.Option<string> // None = global, Some = scoped
  readonly _A?: A                        // phantom: carries the decoded entity type, never assigned
}
```

There is **deliberately no string grammar** — no separator, no glob, no escaping (DEC-9, the same
structure-over-sentinels choice the protocol made for resync targets). The library never *parses* an
id. Keys are minted only by the factory, through two constructors
([`collection-key.ts:22-34`](../packages/live-collection/src/registry/collection-key.ts#L22)):

```ts
globalKey<A>(entity)                      // { entity, scope: None }
scopedKey<A>({ entity, scope })           // { entity, scope: Some(scope) }
```

`serializeKey` produces an injective string used **only** as the registry `Map` key
(`JSON.stringify([entity, scope-or-null])`); it is never parsed back, so it has no contract beyond
collision-freedom over `(entity, scope)`
([`collection-key.ts:41-42`](../packages/live-collection/src/registry/collection-key.ts#L41)).

The phantom `_A` lets `getById` recover the decoded entity type without an unchecked decode: the key
and the stored value share `A` because both come from one `make` call.

---

## `LiveCollection<T>` — the surface you read

`LiveCollection<T>` is **not a wrapper** — it *is* a TanStack `Collection`
([`live-collection.ts:17`](../packages/live-collection/src/persistence/live-collection.ts#L17)):

```ts
export type LiveCollection<T extends object> = Collection<T, ModelId, SyncWrite<T>, never, T>
```

- `TKey = ModelId` — the branded row id.
- `TUtils = SyncWrite<T>` — the **server-truth write path**, hosted on `collection.utils`:
  `writeSynced(entity)` (upsert) and `deleteSynced(id)`, both `Effect.Effect<void>`
  ([`sync-write.ts:17-19`](../packages/live-collection/src/dispatch/sync-write.ts#L17)).
  The dispatcher and your optimistic handlers reconcile through these; the UI never calls them.
- `TSchema = never` — the schema-less overload. Rows are already decoded and branded at the dispatch
  seam, so TanStack does no validation of its own (DEC-A1).

Because it *is* a TanStack collection, the UI uses the native API directly: `useLiveQuery(coll)` to
read, and `coll.insert(...)` / `coll.update(...)` / `coll.delete(...)` to write (which trigger your
optimistic handlers). Nothing in the library re-exposes those.

---

## Worked snippet (playground)

From [`examples/playground/src/live/playground.ts`](../examples/playground/src/live/playground.ts):

```ts
const webhooks = defineCollection({
  runtime,
  services: backend.services,                 // discharges R for listFn + handlers
  entity: "Webhook",
  schema: Webhook,
  getKey: webhookKey,
  scopeOf: (w) => w.orgId,                     // present ⇒ scoped handle: (orgId) => collection
  listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)),
  // Handlers only call the server and return the confirmed row / void — the library reconciles (Model B).
  onInsert: ({ transaction }) =>
    Effect.flatMap(WebhookApi, (api) => api.create(transaction.mutations[0]!.modified)),
  onDelete: ({ transaction }) =>
    Effect.flatMap(WebhookApi, (api) => api.remove(transaction.mutations[0]!.key)),
})
```

And the read side, from
[`examples/playground/src/routes/WebhooksPage.tsx`](../examples/playground/src/routes/WebhooksPage.tsx):

```ts
const coll = pg.webhooks(orgId)               // mount-or-fetch the (Webhook, orgId) instance
const { data } = useLiveQuery(() => coll, [orgId])
// ...
coll.insert({ id: crypto.randomUUID(), orgId, url }) // client-minted id (DEC-8)
```

Note the **client-minted id**: minting it here keeps the self-echo idempotent and avoids any
temp-id swap.

---

## Scoping is the lever for large data

Persistence backend is **not** how large data stays small — scoping is (decision 4). A collection's
working set lives in memory under either persistence backend. Per-workspace collections plus
windowed queries keep that set bounded:

- Define the model **scoped** (`scopeOf`) so each workspace gets its own `(entity, scope)` instance
  instead of one global collection holding every org's rows.
- Mount only the scopes the UI is showing; dispose the rest at workspace boundaries (below).

This is why the registry keys on `(entity, scope)` and why `disposeScope` exists: tearing down a
workspace is a single structural operation, not a glob match.

---

## The registry — mount, peek, dispose

The handle is sugar over `CollectionRegistry`, the generic, long-lived cache of instances keyed by
`CollectionKey`. It hands out the canonical instance for a key and owns teardown via `Scope`; it
knows nothing about entities, workspaces, or TanStack
([`collection-registry.ts:21-59`](../packages/live-collection/src/registry/collection-registry.ts#L21)).
You reach it as `runtime.registry`.

| method | signature | sync/async | what it does |
|--------|-----------|------------|--------------|
| `getOrCreate` | `({ key, make }) => Effect<A, never, Exclude<R, Scope>>` | **sync** mount | builds on first request (in a per-collection child scope), caches, and announces the mount on `mounts`; a cache hit just returns the stored instance. `make` declares teardown with `Effect.addFinalizer`, and the registry discharges that `Scope` so it never leaks to the caller. The handle calls this via `Effect.runSync`. |
| `getById` | `(key) => Effect<Option<A>>` | sync | a **peek** — the instance if mounted, else `None`. Never builds. |
| `getByEntity` | `(entity) => Effect<ReadonlyArray<{ key, collection }>>` | sync | every mounted instance for a model across all scopes (plus the global one if mounted), each paired with its `CollectionKey`. Used to fan a `Delete` whose id may live in any scope, or to read `key.scope` per snapshot. |
| `dispose` | `(key) => Effect<void>` | **async** | tears down and evicts one collection by closing its child scope (which runs `cleanup()`); a no-op if not mounted. |
| `disposeScope` | `(scope) => Effect<void>` | **async** | tears down every collection whose `scope` **equals** `scope` (globals untouched). Scope equality — not a glob — is the match. |
| `disposeAllScoped` | `() => Effect<void>` | **async** | tears down every *scoped* collection, leaving globals mounted (workspace reset). |
| `disposeAll` | `() => Effect<void>` | **async** | tears down *every* collection, globals included (logout). |
| `mounts` | `Stream<CollectionKey<unknown>>` | — | emits a key the **first** time `getOrCreate` builds it (not on cache hits). The sync loop drains it to heal a freshly-mounted collection — skip / replay / bootstrap (see [./read-path.md](./read-path.md)). |

**`getOrCreate` is synchronous, `dispose*` is asynchronous.** Mount has to be `runSync`-able so the
handle can be called inline in render; `persistence` is a closed-over value and only `Scope` is
required of `make`, which the registry discharges, so there is no async boundary on the mount path
(DEC-R8). Disposal, by contrast, closes a child scope and runs the collection's `cleanup()`
finalizer, which is async ([`define-collection.ts:131-151`](../packages/live-collection/src/registry/define-collection.ts#L131),
[`collection-registry.ts:126-159`](../packages/live-collection/src/registry/collection-registry.ts#L126)).

### Lifetimes are scopes, not bookkeeping

Each collection is built in its own **child scope** forked from the registry's layer scope
([`collection-registry.ts:61-105`](../packages/live-collection/src/registry/collection-registry.ts#L61)):

- `dispose` closes one child scope — selective teardown a single shared scope (LIFO,
  all-or-nothing) could not express.
- Releasing the registry layer closes the parent, which closes every surviving child — an automatic
  backstop, no finalizer loop of our own.

### Which `dispose*` when

- **Switch workspace** → `disposeScope(oldOrgId)` (or `disposeAllScoped()` if you're tearing every
  scoped instance down), leaving globals mounted.
- **Logout** → `disposeAll()`.
- **Drop one collection** → `dispose(key)`.

> Note: the loop fiber's lifetime is the app's, separate from the registry. Interrupting the loop
> does **not** dispose collections, and disposing collections does not stop the loop (DEC-R8).

---

## Not built (and why)

- **Unmounted-workspace policy (A.11)** — what to do with deltas for a scope no UI currently mounts
  (drop / buffer / lazy-replay) is deferred. Today an unmounted scope simply isn't healed until it's
  mounted again.
- **Registry eviction backstop** — no LRU/idle eviction; instances live until an explicit `dispose*`
  or layer release. Scoping + manual disposal at workspace boundaries is the intended lever.
- **`variant`-within-a-scope** — a future additive `variant: Option<string>` dimension that
  `disposeScope` would ignore. It is *not* folded into `scope` (which would break workspace
  teardown). Until built, one instance per `(entity, scope)`.

---

## See also

- [./architecture.md](./architecture.md) — `makeLiveRuntime`, the two-surface runtime, the `SyncMap`.
- [./read-path.md](./read-path.md) — how `mounts` drives skip / replay / bootstrap.
- [./optimistic-writes.md](./optimistic-writes.md) — the optimistic write path and `SyncWrite` reconciliation.
- [./protocol.md](./protocol.md) / [./backend.md](./backend.md) — the wire contract you implement
  server-side.
