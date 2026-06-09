# React bindings (`@triargos/live-collection-react`)

**What this is.** The core library is already React-friendly: `defineCollection(...)` returns a
**native** TanStack collection, so you read it with `@tanstack/react-db`'s `useLiveQuery` directly —
this package does not wrap or re-export it. The only genuinely React-specific piece is *lifecycle*:
the package exports exactly one hook, `useLiveSync`, which forks the sync loop on mount and interrupts
it on unmount. The whole binding is one file
([`packages/react/src/index.ts`](../packages/react/src/index.ts)).

**How you use it.** Mount it once, near your app root, to start sync. Everything else — defining
collections, reading them, writing them — happens through the main `@triargos/live-collection`
package and `@tanstack/react-db`. If you reach for a custom subscription hook, stop: that was
deliberately rejected (DEC-R1 — it would shadow `useLiveQuery` and fail the deletion test).

This doc is frontend-only. The wire contract and the server you sync against are the reader's
responsibility — see [./protocol.md](./protocol.md) and [./backend.md](./backend.md). For the
runtime/registry/two-surface mechanics behind the hook, see [./architecture.md](./architecture.md).

---

## The two pieces you import

| Symbol | From | Role |
|---|---|---|
| `useLiveSync(runtime, models)` | `@triargos/live-collection-react` | Fork the sync loop for the app lifetime |
| `useLiveQuery(() => coll)` | `@tanstack/react-db` | Read a collection reactively (native, not wrapped) |

You also import `defineCollection`, `makeLiveRuntime`, and `reloadWindow` from the main
`@triargos/live-collection` package to build the runtime and your collections — those are documented
in [./architecture.md](./architecture.md); this page covers how they meet React.

> **Versions.** `react >= 18` (the playground runs React 19); `@tanstack/react-db` is on `^0.1.85`
> ([`packages/react/package.json`](../packages/react/package.json)). The underlying `@tanstack/db`
> is pinned exactly at `0.6.7` (alpha) and the SQLite persistence surface shifts between releases —
> **verify signatures against your installed version** before copying a snippet.

---

## `useLiveSync(runtime, models)`

```ts
export function useLiveSync(runtime: LiveRuntime, models: SyncModels): void
```

[`packages/react/src/index.ts`](../packages/react/src/index.ts) — DEC-R8.

Forks `runtime.forkLoop(models)` on mount and `Fiber.interrupt`s the returned fiber on unmount. That
loop is the async transport tier: SSE tail + catchup + cursor/watermark advance + resync. It runs
**off the render path**, in an effect.

Three things to internalize:

1. **Mount it once, near the root.** One loop serves the whole app. The playground mounts it in the
   app shell ([`examples/playground/src/routes/App.tsx:15`](../examples/playground/src/routes/App.tsx)).
   `forkLoop` is last-call-wins (a second fork interrupts the previous loop), so two mounts won't
   split the registry's mount-signal queue — but one mount point is still the intended shape.

2. **Unmount stops the loop, but does *not* dispose collections.** Registry lifetime is the **app's**,
   not the loop fiber's — a remount reuses the warm local store (no re-hydrate, no re-list). The hook's
   cleanup runs `Effect.runFork(Fiber.interrupt(fiber))` and nothing else. Disposal is a separate,
   explicit act: `runtime.dispose()` at app teardown / logout, or `disposeScope` on a workspace switch
   (see [./architecture.md](./architecture.md)).

3. **`models` is snapshotted at mount.** The loop reads it once at start; the hook deliberately omits
   `models` from the effect deps and re-forks **only when `runtime` changes**. Passing a fresh `[ ... ]`
   literal every render is fine for identity but pointless — **keep `models` a stable, module-level
   (or memoized) array.** Changing its contents after mount has no effect.

### What `runtime` and `models` are

`runtime: LiveRuntime` is the two-surface infra value from `makeLiveRuntime(...)`. The hook only
touches its `forkLoop` member
([`packages/live-collection/src/runtime/live-runtime.ts`](../packages/live-collection/src/runtime/live-runtime.ts)):

```ts
readonly forkLoop: (models: SyncModels) => Fiber.RuntimeFiber<void>
```

`models: SyncModels` is the **explicit** wiring (DEC-R5 as amended) — there is no auto-registration.
It is an array of collection handles from `defineCollection`; only each handle's `_meta` is read (the
loop reaches live instances through the registry, never by calling the handle)
([`packages/live-collection/src/registry/define-collection.ts`](../packages/live-collection/src/registry/define-collection.ts)):

```ts
export type SyncModels = ReadonlyArray<{ readonly _meta: ModelMeta<any> }>
```

So `models` is a literal `[webhookCollection]` — the wire model name is each handle's `_meta.entity`,
written once in `defineCollection`, and the rest of the metadata (`schema`/`scopeOf`/`listFn`) rides
on the handle. (The earlier record shape, `{ Webhook: webhookCollection }`, duplicated the name — a
typo'd key silently dropped every event for that model.)

---

## Reading a collection: `useLiveQuery`

There is no `useLiveCollection`. You read the native collection with `@tanstack/react-db`'s
`useLiveQuery` directly:

```ts
import { useLiveQuery } from "@tanstack/react-db"

const coll = webhooks(orgId)                       // mount/get the per-scope instance (sync, cached)
const { data } = useLiveQuery(() => coll, [orgId]) // reactive read
```

[`examples/playground/src/routes/WebhooksPage.tsx:21`](../examples/playground/src/routes/WebhooksPage.tsx).

Two read forms are supported and both stay native (DEC-R9):

- direct: `useLiveQuery(() => webhooks(orgId), [orgId])`
- join/filter: `useLiveQuery((q) => q.from({ w: coll }))`

Calling the handle (`webhooks(orgId)`) **during render** is the mount: it's an `Effect.runSync`
against the registry, cached by `(entity, scope)`, so after first mount it's a `Map.get` —
referentially stable, no rebuild, no churn. First mount of a scope is what seeds that workspace's
local store.

---

## Worked example (from the playground)

The whole wiring, end to end. Build the runtime + collections once at startup, hand the `Playground`
value to a plain React context, and let `useLiveSync` start the loop in the shell.

**1. Build the runtime and collections once** —
[`examples/playground/src/live/playground.ts:40`](../examples/playground/src/live/playground.ts):

```ts
const database    = await openBrowserWASQLiteOPFSDatabase({ databaseName: dbName }) // async, once
const persistence = createBrowserWASQLitePersistence({ database })                  // app value, off render path

const backend = makeSharedBackend({ bus, tabId })
const loop    = Layer.merge(backend.loop, EventLogStore.layer({ databaseName: `${dbName}-eventlog` }))
const runtime = makeLiveRuntime({ persistence, loop, onResync: reloadWindow })

const webhooks = defineCollection({
  runtime,
  services: backend.services,           // discharges the R of listFn + the optimistic handlers
  entity: "Webhook",
  schema: Webhook,
  getKey: webhookKey,
  scopeOf: (w) => w.orgId,              // present ⇒ scoped handle: webhooks(orgId)
  listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)),
  // Handler only calls the server and returns the confirmed row — the library reconciles (Model B).
  onInsert: ({ transaction }) =>
    Effect.flatMap(WebhookApi, (api) => api.create(transaction.mutations[0]!.modified)),
})

return { runtime, models: [webhooks], webhooks, /* … */ }
```

Note `models` is the literal `[webhooks]`. `reloadWindow` is the default prod resync
action exported from the main package
([`packages/live-collection/src/runtime/live-runtime.ts:63`](../packages/live-collection/src/runtime/live-runtime.ts)).

**2. Carry the runtime via a plain React context** —
[`examples/playground/src/live/context.tsx`](../examples/playground/src/live/context.tsx). There is no
library-provided provider; this package ships a hook, not a context. The playground rolls its own:

```tsx
const PlaygroundContext = createContext<Playground | null>(null)

export function PlaygroundProvider({ value, children }: { value: Playground; children: ReactNode }) {
  return <PlaygroundContext.Provider value={value}>{children}</PlaygroundContext.Provider>
}

export function usePlayground(): Playground {
  const pg = useContext(PlaygroundContext)
  if (pg === null) throw new Error("usePlayground must be used within <PlaygroundProvider>")
  return pg
}
```

> This `throw` lives in **app/UI code at a React boundary**, not across a library or Effect seam — it
> guards a misuse-in-development invariant (provider missing). It is not a domain failure and must not
> be the pattern inside the library: there, failures are `Schema.TaggedError` values in an Effect's
> error channel, never thrown.

**3. Start the loop once in the shell** —
[`examples/playground/src/routes/App.tsx:13`](../examples/playground/src/routes/App.tsx):

```tsx
export function App() {
  const pg = usePlayground()
  useLiveSync(pg.runtime, pg.models) // forks the loop for the app's lifetime
  return (/* … reads via useLiveQuery deeper in the tree … */)
}
```

**4. Read (and write) deeper in the tree** —
[`examples/playground/src/routes/WebhooksPage.tsx`](../examples/playground/src/routes/WebhooksPage.tsx):

```tsx
const coll = pg.webhooks(orgId)
const { data } = useLiveQuery(() => coll, [orgId])
// optimistic write via the native path — appears instantly, handler confirms, SSE echoes idempotently:
coll.insert({ id: crypto.randomUUID(), orgId, url }) // client-minted id (DEC-8)
```

Writes go through TanStack's native optimistic mutation path; the `onInsert`/`onDelete` handlers you
passed to `defineCollection` call your server and return the confirmed row (insert) / void (delete),
and the **library** reconciles via `writeSynced` / `deleteSynced` before resolving (Model B) — apps
never touch `collection.utils`. Client-minted ids keep the self-echo idempotent (DEC-8).

---

## Mental model: two surfaces, two lifetimes

```
mount  (sync, on render)  ── webhooks(orgId) ⇒ Effect.runSync(registry.getOrCreate) ⇒ Map.get
loop   (async, in effect) ── useLiveSync ⇒ runFork(syncLoop) ⇒ SSE tail + catchup + cursor
```

The registry is shared into the loop (`Layer.succeed`), so dispatch writes to exactly the instances
the UI mounted ([./architecture.md](./architecture.md) has the full picture). The split is why
unmounting the hook is cheap and safe: it stops the fiber, leaves the warm store, and a remount picks
up where it left off.

---

## Not built (and why)

These are deferred — do not wire them expecting them to exist:

- **A provider component / runtime context in this package.** The hook is the whole binding by design;
  apps own their context (the playground uses a plain `createContext`). No `LiveProvider`.
- **A `useLiveCollection` subscription hook.** Rejected (DEC-R1): collections are native, `useLiveQuery`
  already subscribes.
- **Disposing collections on `useLiveSync` unmount.** Intentional (DEC-R8) — registry lifetime is the
  app's. Workspace-switch / logout disposal is a separate explicit call.
- **Re-forking the loop when `map` changes.** Intentional — `map` is snapshotted; keep it stable.
- Throttled watermark flush, registry eviction backstop, offline-durable writes, the A.11
  unmounted-workspace policy, and per-target resync are all deferred at the runtime level — see
  [./architecture.md](./architecture.md).

---

## See also

- [./architecture.md](./architecture.md) — `makeLiveRuntime`, `defineCollection`, registry, scope, the two surfaces.
- [./protocol.md](./protocol.md) — wire schemas, the squasher, sync-group grammar.
- [./backend.md](./backend.md) — the `/catchup` + SSE server contract you sync against.
