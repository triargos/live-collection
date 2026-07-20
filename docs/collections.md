# Collections

`defineCollection` is the typed entry point. It returns a native TanStack `LiveCollection<T>` handle bound to one `LiveRuntime`.

## Scoped and global handles

Presence of `scopeOf` selects the overload:

```ts
const webhooks = defineCollection({
  runtime,
  entity: "Webhook",
  schema: Webhook,
  getKey: (row) => row.id,
  scopeOf: (row) => row.orgId,
  listFn: (orgId) => WebhookApi.pipe(Effect.flatMap((api) => api.list(orgId))),
})

const orgWebhooks = webhooks(orgId)
```

Without `scopeOf`, the handle is global:

```ts
const currentUser = defineCollection({
  runtime,
  entity: "User",
  schema: User,
  getKey: (row) => row.id,
  listFn: UserApi.pipe(Effect.flatMap((api) => api.current)),
})

const users = currentUser()
```

Handles mount synchronously. Calling the same handle with the same scope returns the same object until it is disposed.

## Configuration

- `runtime`: the shared `LiveRuntime`.
- `entity`: wire model name and persisted table identity.
- `schema`: decodes replay/live upserts.
- `getKey`: extracts the branded `ModelId`.
- `scopeOf`: optional scope extractor; its presence makes the collection scoped.
- `listFn`: current server truth used for cold start and resync snapshots.
- `services`: optional app-owned `ManagedRuntime` that supplies Effect dependencies required by `listFn` and mutation handlers.
- `onInsert`, `onUpdate`, `onDelete`: optional optimistic server handlers.

The app's workspace concept appears only through the opaque scope string. The package has no workspace-specific types.

## Self-owned sync drain

On first mount, `defineCollection` creates the persisted TanStack collection and forks a broker drain in the registry child scope.

The drain applies:

- `Snapshot`: `listFn → replaceSynced`
- `Upsert`: schema decode → scope filter → `writeSynced`
- `Delete`: `deleteSynced`

Then it calls `broker.markApplied`. Model decoding and scope filtering live here, not in the broker.

A malformed upsert is warned and skipped without stopping the stream. A valid upsert for another scope is also skipped. Both still advance this subscriber's last-applied syncId because the event was handled.

## Optimistic writes

Handlers only perform the server call. For insert/update they return the server-confirmed row:

```ts
const webhooks = defineCollection({
  // ...read configuration
  onInsert: ({ transaction }) =>
    WebhookApi.pipe(
      Effect.flatMap((api) => api.create(transaction.mutations[0]!.modified)),
    ),
  onUpdate: ({ transaction }) =>
    WebhookApi.pipe(
      Effect.flatMap((api) => api.update(transaction.mutations[0]!.modified)),
    ),
  onDelete: ({ transaction }) =>
    WebhookApi.pipe(
      Effect.flatMap((api) => api.remove(transaction.mutations[0]!.key)),
    ),
})
```

The library reconciles the confirmed row into the synced baseline before the optimistic transaction resolves. The eventual SSE echo is an idempotent rewrite. Batched mutations are rejected because reconciliation supports exactly one mutation per transaction.

## Collection identity

```ts
interface CollectionKey<A> {
  readonly entity: string
  readonly scope: Option<string>
}
```

Keys are structured and never parsed. Globals use `scope: None`; scoped collections use `Some(scope)`. The phantom `A` ties a key to its collection type inside the lifetime table.

## Registry lifecycle

The registry deliberately exposes only lifetime operations:

| Method | Behavior |
|---|---|
| `getOrCreate` | Return the canonical instance for a key, creating it in a child scope on a miss. Used internally by handles. |
| `dispose` | Close and evict one key. |
| `disposeScope` | Close every collection with the matching scope; globals survive. |
| `disposeAllScoped` | Close all scoped collections. |
| `disposeAll` | Close everything. |

Closing a child scope interrupts that collection's broker drain before native collection cleanup. The registry has no `getById`, `getByEntity`, or mount event stream; routing belongs to subscriptions.

```ts
yield* runtime.registry.disposeScope(orgId) // workspace exit
yield* runtime.registry.disposeAll()        // logout
runtime.dispose()                           // application teardown
```

Stopping `runtime.forkSync()` does not dispose collections. Conversely, disposing a collection does not stop global broker ingest.

## Reading

React uses native TanStack React DB:

```tsx
const collection = webhooks(orgId)
const { data } = useLiveQuery((query) => query.from({ webhook: collection }))
```

Non-React callers use the native TanStack collection directly.

See also:

- [Read path](./read-path.md)
- [Replay on mount](./replay-on-mount.md)
- [Optimistic writes](./optimistic-writes.md)
