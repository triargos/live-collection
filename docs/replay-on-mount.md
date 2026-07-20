# Replay on mount

A collection can unmount while the app-wide broker keeps receiving events. Those events are stored in `SyncJournal`, so the collection can later converge from local history instead of always fetching a fresh snapshot.

## Freshness state

Each collection key `(modelName, scope)` has one last-applied record: the newest `syncId` through which that collection has handled all relevant signals, stamped with the schema version of the saved rows it describes. A write under a different schema version supersedes the record outright (the saved table was dumped, so the old mark describes rows that no longer exist), and a read under any other version finds nothing — the mount then snapshots instead of trusting a mark for a dead table.

The event log also stores:

- a per-model prune floor: the highest deleted event id;
- the latest resync id.

The global cursor comes from `SyncCursor`.

## Subscription decision

When a collection subscribes, the broker compares those positions:

1. no last-applied syncId → `Snapshot`;
2. resync newer than the last-applied syncId → `Snapshot`;
3. last-applied syncId at or ahead of cursor → no replay;
4. prune floor above the last-applied syncId → `Snapshot`;
5. otherwise → replay model events after the last-applied syncId.

“Skip” is simply an empty replay. Callers do not branch between separate bootstrap, replay, and live APIs.

## One stream

The broker returns:

```text
Snapshot? → durable rows → live PubSub tail
```

It subscribes to PubSub before reading metadata and the log. Events arriving during setup are buffered. The tail is filtered above the last replayed id, preventing an older buffered duplicate from applying after newer replay.

For a snapshot, the collection runs `listFn(scope)`, atomically replaces its synced store, acknowledges the snapshot id, and then continues draining buffered tail events.

## Scope filtering

The broker stores opaque model-level rows because it does not know model schemas. A scoped drain decodes replayed upserts and compares `scopeOf(row)` with its own scope. Rows for other scopes are skipped but acknowledged.

Deletes contain no row data and therefore no derivable scope. They are delivered to every subscriber for that model.

## Pruning

Every `trimEveryEvents` ingested events the broker flushes pending last-applied marks and prunes the journal in three stages:

1. **Squash** — keep only the newest event per `(modelName, modelId)`. Replay applies upserts and deletes idempotently, so an entity's intermediate history converges to the same state from every possible last-applied position. Delete tombstones are kept: a collection whose last-applied sits mid-run saw the insert and must still see the delete.
2. **Dead weight** — drop rows at or below the model's *minimum* last-applied syncId across its collections. Application is sequential and gapless, so every collection with a record already applied them. A model with no record at all drops entirely: any mount without a record decides `Snapshot` regardless.
3. **Count caps** — keep the newest `maxEventsPerModel` events per model, then at most `maxEventsTotal` overall:

```ts
makeLiveRuntime({
  persistence,
  sync,
  broker: {
    retention: {
      maxEventsPerModel: 1000,
      maxEventsTotal: 5000,
      trimEveryEvents: 100,
    },
  },
})
```

Stages 1–2 delete only history no replayer can ever need, so they never move the prune floor and can never force a `Snapshot`. Only the count caps move the floor: if they remove any part of a collection's missing range, replay is unsafe and the broker emits `Snapshot`. Because squash bounds the log by distinct churned entities rather than raw event count, the caps rarely bite in practice.

## Last-applied batching

`markApplied` updates pending last-applied marks in memory and flushes them durably on an interval (100 ms by default) and on broker shutdown. A stale durable mark may cause a few events to replay after a crash, but synced upserts and deletes are idempotent.

Configure the interval with `broker.pendingLastAppliedFlushInterval`.

## Workspace lifecycle

```ts
const collection = webhooks(orgId)                 // mount or reuse
yield* runtime.registry.disposeScope(orgId)         // interrupt drain and clean up
const remounted = webhooks(orgId)                   // new instance; replay or snapshot
```

The registry has no lookup or routing API. Apps that need debug bookkeeping should maintain their own app-level index rather than turning the lifetime table back into a central manager.
