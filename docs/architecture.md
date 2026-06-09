# Architecture

**What this is.** `@triargos/live-collection` is a **frontend-only** Effect + TanStack DB live-sync
engine. This document maps the system for the developer integrating it into an app: the package
boundaries and why they fall where they do, the acyclic dependency DAG, the two execution surfaces
that keep mounting off the render path's async hot loop, the seam convention every service follows,
and the codebase-wide rules that make the examples here trustworthy. Read it to understand how the
pieces compose before you wire the library in. The backend is yours and is documented separately —
see [`./backend.md`](./backend.md)
and the wire contract in [`./protocol.md`](./protocol.md).

> The authoritative design history is [`../packages/live-collection/DESIGN.md`](../packages/live-collection/DESIGN.md),
> which is layered by date — **later sections supersede earlier ones**. This doc reflects the current
> shipped surface (the native-collection redesign, DEC-R*, plus the EventLog replay path, DEC-E*); where
> the design log still names an older API (`effectCollectionOptions`, `PersistenceBase` tag, `MountRef`),
> that name is **dead** — the live name is cited from `src/` below.

---

## The three published packages

```
packages/
  protocol/         @triargos/live-collection-protocol   deps: effect
  live-collection/  @triargos/live-collection            deps: effect, @effect/platform, @tanstack/db
  react/            @triargos/live-collection-react       deps: react, @tanstack/react-db, + main
```

Three packages, **acyclic**:

```
protocol  →  live-collection  →  react
```

`protocol → live-collection`: `live-collection` imports the wire schemas and the squasher from the
protocol kit (`HydratedSyncEventEnvelope`, `CatchupResponse`, `ModelId`, `SyncId`, `ModelName` — see
the imports at the top of [`sync-loop.ts:1-8`](../packages/live-collection/src/client/sync-loop.ts#L1-L8)).
`live-collection → react`: the React bindings import `LiveRuntime` and `SyncModels` as types only
([`react/src/index.ts:11`](../packages/react/src/index.ts#L11)). Nothing points back up the chain.

### Why `core` / `persistence` / `client` are directories, not packages

The single most important boundary decision: **`core`, `persistence`, and `client` are *directories*
inside `live-collection`, not separate npm packages.** They always travel together — no consumer wants
the registry without the persistence factory, or the SSE transport without the registry to write into.
Splitting them into packages would buy nothing but version-skew risk and a deeper install graph.

Their *seams* survive as **modules + Effect service tags**, not as npm boundaries. The internal layout
([`index.ts:5-13`](../packages/live-collection/src/index.ts#L5-L13)):

```
src/
  registry/     CollectionRegistry, CollectionKey, defineCollection (the runtime-bound handle)
  dispatch/     the SyncWrite contract (the synced-store write path)
  persistence/  liveCollectionOptions (inner creator) over TanStack DB 0.6
  client/       SSE transport, catchup, lastSyncId store, the durable event log, the sync loop
  runtime/      makeLiveRuntime (two-surface: sync registry+persistence | async loop)
```

### Why `protocol` and `react` *do* earn separation

Two boundaries are real, because each has a **different consumer or a different dependency**:

- **`protocol`** is consumed by a *different consumer* — the **backend**, which lives in a separate repo
  and must implement against the contract **without** any frontend dependency. So `protocol` depends on
  `effect` and nothing else (not even `@effect/platform`). It is a pure **contract kit**: the
  `SyncEvent` / `HydratedSyncEvent` schemas, the sync-group grammar, the resync sentinel codecs, the
  pure **squasher** (property-tested *in* `protocol`, imported by both ends), the expected interface
  types, and the `/catchup` request/response **schemas** (not an `HttpApi` — the backend owns its
  routes, errors, and auth). See [`./protocol.md`](./protocol.md).
- **`react`** carries a *different dependency* — `react` + `@tanstack/react-db` — that a non-React app
  must be able to avoid. The core stays framework-neutral; React is opt-in.

### Version pins (verify against the installed version)

The persistence surface is **alpha and shifts** — pin deliberately, and re-check signatures against the
version actually installed:

- `@tanstack/db` is pinned **exactly `0.6.7`** (alpha). Core only exports `createCollection`.
- `persistedCollectionOptions` is imported from **`@tanstack/db-sqlite-persistence-core`**, *not* `@tanstack/db` core.
- The prod browser persistence builder (`@tanstack/browser-db-sqlite-persistence`) is pinned **`0.1.11`**,
  matching the persistence-core pin (DEC-R3). See it in use at
  [`playground.ts:11-14`](../examples/playground/src/live/playground.ts#L11-L14).

---

## The two execution surfaces (DEC-R8)

This is the load-bearing runtime mechanic. **Mounting a collection happens during render** (inside
`useLiveQuery`'s queryFn); **the sync loop runs in an effect, off the render path.** They must not share
one async path — so `LiveRuntime` exposes exactly two surfaces
([`live-runtime.ts:20-30`](../packages/live-collection/src/runtime/live-runtime.ts#L20-L30)):

```ts
export interface LiveRuntime {
  readonly registry: CollectionRegistryShape          // mount surface — sync value
  readonly persistence: PersistedCollectionPersistence // app-owned value, threaded into each make
  readonly forkLoop: (models: SyncModels) => Fiber.RuntimeFiber<void> // loop surface — async fiber
  readonly dispose: () => void                         // app teardown / logout
}
```

**Mount surface (synchronous).** The `registry` is a plain **value**, built once via `Effect.runSync` in
a long-lived scope ([`live-runtime.ts:45-46`](../packages/live-collection/src/runtime/live-runtime.ts#L45-L46)).
A collection handle mounts by `Effect.runSync(registry.getOrCreate({ key, make }))`
([`define-collection.ts:150-151`](../packages/live-collection/src/registry/define-collection.ts#L150-L151)).
The `make` is `Effect.sync(() => createCollection(...))` plus an `addFinalizer(cleanup)`, so it requires
only `Scope` — which the registry discharges via `Exclude<R, Scope>`
([`collection-registry.ts:28-31`](../packages/live-collection/src/registry/collection-registry.ts#L28-L31)) —
yielding `Effect<A, never, never>`. That is why `runSync` **cannot hit an async boundary**. `persistence`
is a **value closed over** in `make` ([`define-collection.ts:138`](../packages/live-collection/src/registry/define-collection.ts#L138)),
not a context dependency, so the mount path needs no async layers. After first mount it is a `Map.get`:
referentially **stable** identity, no rebuild, no churn.

**Loop surface (asynchronous).** `forkLoop` runs `syncLoop(map, onResync)` on a `ManagedRuntime` built
over the app's `loop` layer ([`live-runtime.ts:47-54`](../packages/live-collection/src/runtime/live-runtime.ts#L47-L54)).
This fiber owns catchup → cursor → SSE tail → resync — never on the render path. In React it is forked by
`useLiveSync` in a `useEffect` and `Fiber.interrupt`ed on unmount
([`react/src/index.ts:26-31`](../packages/react/src/index.ts#L26-L31)).

**The shared registry is the join.** The same `registry` value is handed into the loop's runtime via
`Layer.succeed(CollectionRegistry, registry)`
([`live-runtime.ts:47-49`](../packages/live-collection/src/runtime/live-runtime.ts#L47-L49)), so the loop's
dispatch writes (`writeSynced` / `deleteSynced`) land on **exactly the instances the UI mounted** — and
only those (`registry.getById` returns `None` for unmounted scopes, so their events are dropped from the
store but still recorded in the durable log for later replay,
[`sync-loop.ts:83-88`](../packages/live-collection/src/client/sync-loop.ts#L83-L88)).

**Lifetime: the registry belongs to the *app*, not the loop fiber.** Interrupting `useLiveSync` stops the
SSE loop but does **not** dispose collections — a remount reuses the warm local store
([`live-runtime.ts:25-27`](../packages/live-collection/src/runtime/live-runtime.ts#L25-L27),
[`react/src/index.ts:17-19`](../packages/react/src/index.ts#L17-L19)). Disposal is explicit and
scope-shaped: `disposeScope(scope)` on a workspace switch, `disposeAll()` on logout, or `dispose()`
closing the long-lived scope at app teardown ([`collection-registry.ts:53-58`](../packages/live-collection/src/registry/collection-registry.ts#L53-L58)).

---

## Seams are `Context.Tag` + `Shape` + `Layer` (decision 6)

Every service seam in the library is a **hand-rolled** `Context.Tag` — **never `Effect.Service`** (which
fuses tag, impl, and default layer, and is being removed in Effect v4). The exact shape:

- the **interface** is `interface <Name>Shape` — the contract (`Shape`, never `Impl`);
- the **tag** is `class <Name> extends Context.Tag("<Name>")<<Name>, <Name>Shape>() {}` — the seam you `yield*`;
- the **impl** is a separate `const make: Effect<<Name>Shape, …>` (or a function returning one);
- the **layer(s)** hang off the tag: `static readonly layer` (prod default), plus `layerMemory` /
  `layerFromEnv` where a second adapter is real. **No `Live` suffix.**

The seams the loop depends on (its requirement channel,
[`sync-loop.ts:60-62`](../packages/live-collection/src/client/sync-loop.ts#L60-L62)):

| Seam | Shape | Tag + layers (cite) |
|------|-------|----|
| `CollectionRegistry` | `CollectionRegistryShape` — generic collection cache, owns teardown | [`collection-registry.ts:174-179`](../packages/live-collection/src/registry/collection-registry.ts#L174-L179) — `layer` |
| `SyncTransport` | `SyncTransportShape` — the one SSE `connect` stream | [`sync-transport.ts:67-83`](../packages/live-collection/src/client/sync-transport.ts#L67-L83) — `layer({url, keepAlive})`, `layerMemory` |
| `CatchupClient` | `CatchupClientShape` — `fetch(CatchupRequest) → CatchupResponse` | [`catchup-client.ts:41-48`](../packages/live-collection/src/client/catchup-client.ts#L41-L48) — `layer({url})`, `layerMemory` |
| `LastSyncIdStore` | `LastSyncIdStoreShape` — the durable global watermark/cursor | [`last-sync-id-store.ts:55-62`](../packages/live-collection/src/client/last-sync-id-store.ts#L55-L62) — `layer` (localStorage), `layerMemory` |
| `EventLogStore` | `EventLogStoreShape` — the durable per-model event log for replay-on-mount | [`event-log-store.ts:270-274`](../packages/live-collection/src/client/event-log-store.ts#L270-L274) — `layer({databaseName?})` (IndexedDB), `layerMemory` |

The app composes these into the single `loop` layer it passes to `makeLiveRuntime`. The transport and
catchup `layer`s carry a `HttpClient.HttpClient` requirement; provide the platform HTTP layer at the edge.
Tests provide `layerMemory` adapters — **no `vi.mock`**, drive the seam through its in-memory layer.

> **Note on persistence (DEC-R3, supersedes DEC-6 for this one seam).** `persistence` is **not** a tag —
> it is a plain `PersistedCollectionPersistence` **value** the app builds and hands to `makeLiveRuntime`
> ([`live-runtime.ts:23-24`](../packages/live-collection/src/runtime/live-runtime.ts#L23-L24)). The old
> `PersistenceBase` tag is retired. This is exactly what makes the mount path synchronous (the value is
> closed over, not resolved from context). DESIGN.md §2/§3 still describe the tag and an
> `effectCollectionOptions` creator — both **dead**; the live creator is `liveCollectionOptions`
> ([`index.ts:43-44`](../packages/live-collection/src/index.ts#L43-L44)).

---

## The wiring DAG (how an app assembles it)

A collection needs only **infra** (registry + persistence) to exist; it does **not** depend on the
dispatcher — the loop *pushes into* the collection via `utils.writeSynced`. So `runtime` can be an input
to `defineCollection` with no cycle:

```
runtime  (infra: registry value + persistence value | async transport+cursor+catchup)
   ↑       built first, knows no collections
collections = defineCollection({ runtime, … })   →  native LiveCollection<T>, carries _meta
   ↑       registry-backed callable handle
SyncModels [webhookCollection, …]  →  syncLoop / useLiveSync
           assembled last; references the handles by _meta (wire name = _meta.entity)
```

`defineCollection` has **two overloads** ([`define-collection.ts:103-104`](../packages/live-collection/src/registry/define-collection.ts#L103-L104)):
`scopeOf` present ⇒ scoped `(scope: string) => LiveCollection<T>`; absent ⇒ global `() => LiveCollection<T>`.
It returns a **native** `LiveCollection<T>` — pass it straight to `useLiveQuery`, no wrapper hook (DEC-R1).
Collection identity is the structured `CollectionKey { entity, scope: Option<string> }`
([`collection-key.ts:15-19`](../packages/live-collection/src/registry/collection-key.ts#L15-L19)) — there
is no string-id grammar; the registry never parses an id, and `serializeKey` is an injective map key only,
never parsed back ([`collection-key.ts:41-42`](../packages/live-collection/src/registry/collection-key.ts#L41-L42)).

`SyncModels` is a literal **array of handles** — `[webhookCollection, …]` — and the wire model name
is each handle's `_meta.entity`, written once in `defineCollection` (DEC-R5 as amended: the earlier
record shape duplicated the name, and a typo'd key silently dropped a model's events). Each handle
carries `_meta: ModelMeta<T>` (`entity`, `schema`, `getKey`, `scopeOf`, `listFn`) that the loop reads
to decode, route, and snapshot
([`define-collection.ts:25-31`](../packages/live-collection/src/registry/define-collection.ts#L25-L31)) —
the loop never *calls* the handle, it reaches instances through the registry.

### Worked example (from `examples/playground/`)

Real OPFS persistence + cross-tab backend; the full optimistic write path runs through the synced store
([`playground.ts:40-76`](../examples/playground/src/live/playground.ts#L40-L76)):

```ts
// 1. Build the persistence VALUE once at startup (async, off the render path).
const database    = await openBrowserWASQLiteOPFSDatabase({ databaseName: dbName })
const persistence = createBrowserWASQLitePersistence({ database })

// 2. Compose the loop layer (transport + catchup + cursor + the durable event log) and the runtime.
const loop    = Layer.merge(backend.loop, EventLogStore.layer({ databaseName: `${dbName}-eventlog` }))
const runtime = makeLiveRuntime({ persistence, loop, onResync: reloadWindow })

// 3. Declare one collection per model. scopeOf present ⇒ scoped by orgId.
const webhooks = defineCollection({
  runtime,
  services: backend.services,                 // the app-services ManagedRuntime that discharges listFn/handler R
  entity: "Webhook",
  schema: Webhook,
  getKey: webhookKey,
  scopeOf: (w) => w.orgId,
  listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)),  // cold/resync snapshot source
  onInsert: ({ transaction }) =>  // optimistic write path (A.10) — call the server, return the confirmed row
    Effect.flatMap(WebhookApi, (api) => api.create(transaction.mutations[0]!.modified)), // library reconciles (Model B)
})

// 4. The models array is a literal; start the loop once near the app root.
const models: SyncModels = [webhooks]
// React:  useLiveSync(runtime, models)
// Reads:  const { data } = useLiveQuery(() => webhooks(orgId), [orgId])   // native, stable
```

The optimistic mutation handlers return a **pure Effect** with the app's `R`; `services` (an
app-owned `ManagedRuntime`) discharges that `R` at define time
([`define-collection.ts:64-71`](../packages/live-collection/src/registry/define-collection.ts#L64-L71),
[`define-collection.ts:116-126`](../packages/live-collection/src/registry/define-collection.ts#L116-L126)).
`LiveRuntime` stays infra-only (non-generic) — the app's `R` lives only on the collection.

---

## Codebase conventions (any example must honor these)

These are repo-wide rules from [`../CLAUDE.md`](../CLAUDE.md); an example that breaks one misleads.

- **No `throw`, no `new Error` across boundaries.** Failures are `Schema.TaggedError` — a tagged error
  *is* an Effect, you return it. See `CatchupFailed`
  ([`catchup-client.ts:11`](../packages/live-collection/src/client/catchup-client.ts#L11)) and
  `SyncConnectionLost` ([`sync-transport.ts:10-12`](../packages/live-collection/src/client/sync-transport.ts#L10-L12)).
  Infrastructure failures (network, DB driver) are **defects** — `Effect.orDie` them; keep the error
  channel limited to modeled domain failures. Recover with `Effect.catchTag(s)` / `mapError`, **never**
  `Effect.catchAllCause` (it swallows defects).
- **`Option` over `null`/`undefined`** for modeled absence (`CollectionKey.scope`, `ModelMeta.scopeOf`).
  Decode wire `T | null` to `Option<T>` at the boundary — `HydratedSyncEvent.data` is nullable *on the
  wire* by contract; never let `null` leak inward.
- **Validation at boundaries only.** Decode SSE / catchup payloads against the protocol schemas — never
  cast a wire shape. The transport decodes against `HydratedSyncEventEnvelope`
  ([`sync-transport.ts:26`](../packages/live-collection/src/client/sync-transport.ts#L26)); catchup decodes
  against `CatchupResponse` ([`catchup-client.ts:33-34`](../packages/live-collection/src/client/catchup-client.ts#L33-L34)).
  **No `as` casts on IO results**, no `any` (use `unknown` and narrow). Branded ids are minted only at
  mappers/input handlers, never cast inside the app.
- **Object args when a function has more than one of its own params** — `fn({ key, make })`, not
  `fn(key, make)`. The leading data-last arg of a `dual` combinator is exempt. See `getOrCreate({ key, make })`
  ([`collection-registry.ts:28-31`](../packages/live-collection/src/registry/collection-registry.ts#L28-L31)).
- **Reuse before invent; design the interface before implementing.** The design is the deliverable first
  (the `design-first` skill drives it); seams get a locked `Context.Tag` + `Shape` spec, then red tests,
  then `make` + `Layer` to green. Tests verify behavior through the public interface, not implementation.

---

## Not built (and why)

These are deferred or rejected — do not document them as present:

- **Offline-durable writes.** A.10 ships *online* optimistic writes (Model B: confirm via `writeSynced`
  before resolving). Queueing mutations while offline is deferred.
- **Unmounted-workspace policy (A.11), throttled watermark flush, a registry eviction backstop, and
  per-target resync** are deferred. Resync today is whole-app `reloadWindow` (Model A);
  `onResync` is the seam where a finer model would slot in
  ([`live-runtime.ts:62-63`](../packages/live-collection/src/runtime/live-runtime.ts#L62-L63)).
- **Echo suppression / `clientId`** is removed (protocol DEC-11): no `clientId` on events, `SyncContext`,
  or the HTTP contract. Reconciliation relies on TanStack's optimistic-mutation confirm, not a server filter.

---

## See also

- [`./protocol.md`](./protocol.md) — the wire contract: event schemas, sync-group grammar, the squasher, `/catchup` shapes.
- [`./backend.md`](./backend.md) — implementing the per-app backend against the protocol kit (your responsibility).
- [`../packages/live-collection/DESIGN.md`](../packages/live-collection/DESIGN.md) — full design history (later sections supersede earlier ones).
