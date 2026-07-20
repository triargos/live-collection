# Client architecture

`@triargos/live-collection` is a frontend-only sync engine. The backend contract remains one multiplexed SSE stream and one global `/catchup?from=` endpoint. Client ownership is inverted: collections subscribe themselves to a shared broker instead of a central loop routing into a registry.

## Main modules

### `SyncBroker`

One app-wide broker owns:

- the SSE connection (`SyncTransport`)
- catchup (`CatchupClient`)
- the global durable cursor (`LastSyncIdStore`)
- the durable sync journal (`SyncJournal`)
- pruning and resync handling
- an in-memory `PubSub` for active subscribers

Ingest is single-fibered. Each entity event is appended to the log, published, and advances the cursor. Connection loss retries the cycle, running catchup before reconnecting.

```ts
interface SyncBrokerShape {
  readonly subscribe: (args: {
    readonly modelName: ModelName
    readonly scope: Option<string>
  }) => Stream<SyncSignal>

  readonly markApplied: (args: {
    readonly modelName: ModelName
    readonly scope: Option<string>
    readonly through: SyncId
  }) => Effect<void>

  readonly start: Effect<void>
}
```

`SyncSignal` has three arms:

- `Snapshot { at }`: the local base cannot be trusted; run `listFn`, replace the synced store, then acknowledge `at`
- `Upsert { syncId, modelId, data }`
- `Delete { syncId, modelId }`

### Collections

`defineCollection` returns a native TanStack collection handle. On first mount it:

1. creates the persisted collection synchronously;
2. forks a drain in that collection's registry child scope;
3. subscribes to the broker by model and scope;
4. sequentially applies every signal;
5. calls `markApplied` only after application.

Sequential draining preserves order. A snapshot finishes before buffered tail events are applied.

The broker is model-blind. The collection drain decodes `Upsert.data` with its schema and performs scope filtering after decode. Deletes carry no scope, so every mounted scope for that model receives them; deleting an absent key is harmless.

### `CollectionRegistry`

The registry is only a lifetime table. Its public surface is:

```ts
interface CollectionRegistryShape {
  readonly getOrCreate: ...
  readonly dispose: ...
  readonly disposeScope: ...
  readonly disposeAllScoped: ...
  readonly disposeAll: ...
}
```

It guarantees one instance per `(entity, scope)` and owns a child `Scope` per collection. Closing that scope interrupts the drain and cleans up the native collection. It has no routing, lookup, or mount-stream API.

### `LiveRuntime`

```ts
const runtime = makeLiveRuntime({ persistence, sync })

runtime.forkSync()             // non-React startup
runtime.registry.disposeScope(orgId)
runtime.dispose()
```

`sync` is a layer containing `SyncTransport`, `CatchupClient`, `LastSyncIdStore`, and `SyncJournal`. Internally the runtime builds `SyncBroker.layer`, exposes synchronous collection mounting, and executes broker/drain fibers on a `ManagedRuntime`.

React apps call `useLiveSync(runtime)` once near the root. No model array is needed: mounted collections subscribe themselves.

## Replay and subscription switchover

A subscription first attaches to PubSub so new events begin buffering. It then reads the collection's last-applied syncId, global cursor, prune floor, last resync, and replay slice. The resulting stream is:

```text
Snapshot? → durable replay rows → filtered live tail
```

The tail drops signals at or below the last emitted `syncId`. This prevents a buffered older duplicate from landing after a newer replay row. Upserts remain idempotent, so catchup/SSE overlap is safe.

Last-applied marks are updated in memory per applied signal and flushed durably in batches (100 ms by default and on broker scope close). A remount reads the maximum of pending and durable state.

## Resync

Live and catchup resync use the same path: record `lastResync`, publish `Snapshot`, and continue ingesting. Active collections refetch in place; unmounted collections snapshot when they next subscribe because their last-applied syncId predates `lastResync`.

All resync targets are currently treated globally. Target-aware subscriber selection is intentionally deferred.

## Production graph

```text
useLiveSync(runtime)
  → runtime.forkSync()
    → broker.start
      → catchup + SSE
        → event log + cursor + PubSub

collectionHandle(scope)
  → registry.getOrCreate
    → create native persisted collection
    → fork broker subscription drain
      → Snapshot / Upsert / Delete
        → synced-store write
        → broker.markApplied
```
