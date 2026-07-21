# Read path

The read path combines one global network feed with self-owned collection drains.

## Startup

Build the runtime with the three sync services:

```ts
const sync = Layer.mergeAll(
  SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" }),
  CatchupClient.layer({ url: "/api/catchup" }),
  SyncJournal.layer({ databaseName: "app-eventlog" }),
).pipe(Layer.provide(FetchHttpClient.layer))

const runtime = makeLiveRuntime({ persistence, sync })
```

Start ingest once:

```ts
runtime.forkSync()
// React: useLiveSync(runtime)
```

No collection list is passed. Collections subscribe themselves when their handles mount.

## Broker ingest cycle

`SyncBroker.start` repeatedly performs:

1. read the global cursor, defaulting to `0`;
2. request `/catchup?from=<cursor>`;
3. ingest the response or log a recoverable catchup failure;
4. tail the multiplexed SSE stream;
5. on connection loss, wait and restart at step 1.

For each entity event:

```text
append opaque event to SyncJournal
→ publish it to active subscriptions
→ advance the journal's cursor
→ occasionally prune the log (squash per entity, drop rows every collection applied, then count caps)
```

The broker deliberately does not know model schemas or mounted collections. It logs all models, including models with no active subscriber. That durable history is what makes a later mount replay locally.

## Collection subscription

Each collection drains:

```ts
Stream.runForEach(
  broker.subscribe({ modelName, scope }),
  applySignal,
)
```

`subscribe` hides the replay/live switchover. It subscribes to PubSub first, then examines:

- the collection's last-applied syncId;
- the global cursor;
- the model's prune floor;
- the latest resync marker.

It chooses one of three outcomes:

- **Skip:** the local base is already complete; emit no replay.
- **Replay:** the log covers the missing range; emit model rows after the last-applied syncId.
- **Snapshot:** no base exists, pruning removed part of the gap, or a resync invalidated the base.

These outcomes all feed one stream:

```text
Snapshot? → replay → live tail
```

A PubSub subscription is established before metadata and log reads, so live events cannot fall into a replay/tail gap. Tail events at or below the replay head are dropped to prevent reordering.

## Applying signals

The collection owns its write path:

- `Snapshot`: run `listFn(scope)`, then `replaceSynced(rows)`.
- `Upsert`: decode `data` with the model schema, scope-filter it, then `writeSynced(row)`.
- `Delete`: call `deleteSynced(modelId)` for every subscriber of that model.

After each signal is fully handled, the drain calls `markApplied({ through: syncId })`. Scope-mismatched and undecodable upserts are still acknowledged because they have been deliberately handled. Decode failure is logged and does not kill the drain.

## Last-applied syncIds

A collection's last-applied syncId means: “this collection has handled every relevant signal through this sync id.”

`markApplied` updates an in-memory monotonic map. The broker flushes pending values to `SyncJournal` about every 100 ms and once more when its scope closes. This avoids a durable write per event. A crash can only cause a small idempotent replay on the next mount.

## Deletes and scoping

Upserts carry data, so the collection can derive scope with `scopeOf(decodedRow)`. Deletes carry only model and id. Therefore a delete is delivered to every active scope for that model. Removing a key that is absent is a no-op.

The durable event log stores model-level rows, not scope. Scoped replay decodes and filters its bounded per-model slice on the subscriber side.

## Resync

A live resync:

```text
record lastResync
→ publish Snapshot(at = resync.syncId) globally
→ advance cursor
→ keep tailing
```

A catchup response containing resync publishes `Snapshot(at = response.lastSyncId)` and advances the cursor to the response head. Active subscribers refetch in place. Unmounted collections detect that `lastResync` is newer than their last-applied syncId and snapshot when remounted.

There is no reload hook. Resync target-specific routing is deferred; all current resync arms invalidate all subscribers.
