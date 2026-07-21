# Architecture

How the client keeps collections live. You don't need this to use the library — the [getting started guide](./getting-started.md) covers the API — but it explains what happens between an event leaving your server and a row appearing in a query.

## The pieces

```text
useLiveSync(runtime) / runtime.forkSync()
  → SyncBroker.start                      one app-wide ingest loop
      catchup → SSE tail
        → SyncJournal                     durable event log + cursor (IndexedDB)
        → PubSub                          fan-out to mounted collections

todosCollection(scope)                    handle call = mount
  → CollectionRegistry.getOrCreate       one instance per (entity, scope)
      → native persisted collection      rows in local SQLite
      → drain fiber                      subscribes to the broker, applies signals
```

- **`SyncBroker`** — one per app. Owns the SSE connection, catchup, the journal, and pruning. It is model-blind: it logs and publishes opaque events without decoding entity data.
- **`SyncJournal`** — a durable local log of recent events plus sync positions: the global cursor (newest syncId ingested), each collection's last-applied syncId, prune boundaries, and the last resync. This is what lets a collection that mounts later catch up locally.
- **`CollectionRegistry`** — a lifetime table. It guarantees one collection instance per `(entity, scope)` key and owns a child scope per instance; disposing the scope interrupts the drain and cleans up the collection. It does no routing — collections subscribe themselves.
- **The drain** — per collection. Decodes events with the model schema, filters to its scope, and writes to the collection's synced store.

## The ingest loop

`SyncBroker.start` runs forever:

1. Read the journal's cursor (the newest syncId this client has ingested).
2. `GET /catchup?from=<cursor>` and ingest the response. A failed catchup is logged, not fatal — the next reconnect retries it.
3. Tail the SSE stream.
4. On disconnect or keepalive silence, go to 1.

Each ingested event is appended to the journal, published to subscribers, and advances the cursor. All models are logged — including models with no mounted collection — which is what makes later mounts cheap.

Catchup is the source of truth; the SSE tail is best-effort. Any event the tail misses is picked up by the catchup that runs on the next reconnect.

## Signals

Collections receive three kinds of signal, applied strictly in order:

| Signal | Meaning | The drain's response |
|---|---|---|
| `Snapshot` | the local base can't be trusted | run `listFn(scope)`, atomically replace the synced store |
| `Upsert` | an entity changed | decode with the schema, filter by scope, `writeSynced` |
| `Delete` | an entity is gone | `deleteSynced(modelId)` |

The broker invokes `apply` sequentially and records the collection's last-applied syncId after each one returns — a subscriber cannot ack early, skip an ack, or apply out of order. An upsert that fails to decode, or belongs to another scope, is logged (or silently skipped) and still counts as applied: returning from `apply` means the signal was handled.

Deletes carry no entity data, so no scope can be derived from them; every mounted scope of the model receives the delete, and removing an absent key is a no-op.

## Replay on mount

When a collection subscribes — first mount, or a remount after `disposeScope` — the broker decides how to bring it current by comparing positions in the journal:

1. **No last-applied record** (or a schema-version change dumped the table, or a resync happened since) → `Snapshot`.
2. **Pruning removed part of the missing range** → `Snapshot`; replay would have gaps.
3. **Already at the head** → nothing to do.
4. **Otherwise** → replay the model's logged events after the last-applied syncId.

All outcomes feed one ordered stream:

```text
Snapshot? → replayed journal rows → live tail
```

The broker subscribes to the live feed *before* reading the journal, so events arriving mid-setup buffer instead of falling into a gap; buffered events at or below the replay head are dropped so an older duplicate can never land after newer replayed state. Upserts and deletes are idempotent, so overlap is always safe.

Last-applied marks are acked in memory per signal and flushed durably in batches (every 100 ms and on shutdown). A crash between flushes just replays a few idempotent events on the next mount.

## Journal pruning

The journal is bounded. Every `trimEveryEvents` ingested events, the broker prunes in three stages:

1. **Squash** — keep only the newest event per entity. Replay is idempotent, so intermediate history converges to the same state from any starting position. Delete tombstones survive: a collection that saw the insert must still see the delete.
2. **Dead weight** — drop events every mounted collection of that model has already applied.
3. **Count caps** — enforce `maxEventsPerModel` and `maxEventsTotal`, newest first.

Stages 1–2 only remove history no replayer can need. Only the count caps can cut into a collection's missing range — when they do, that collection gets a `Snapshot` on its next mount instead of a broken replay. Because squashing bounds the log by distinct changed entities rather than raw events, the caps rarely bite.

```ts
makeLiveRuntime({
  persistence,
  sync,
  broker: {
    retention: { maxEventsPerModel: 1000, maxEventsTotal: 5000, trimEveryEvents: 100 },
  },
})
```

## Resync

A `Resync` event from the server means "deltas can't express what changed — refetch." The broker records it and publishes a `Snapshot`: active collections re-run `listFn` in place; unmounted collections notice on their next mount that a resync postdates their last-applied mark and snapshot then. There is no page reload and no callback to wire.

Resync targets (`All` / `Group` / `Model`) are currently all treated as global on the client — every active collection refetches.

## Timeline resets and the epoch

Sync cursors are only meaningful within one server log timeline. If the server's log history is destroyed — an in-memory store restarting, a truncation, a backup restore — it reports a new `epoch` on catchup. The client detects the mismatch, wipes its local sync state (journal, cursor, last-applied marks), and re-bootstraps every collection with a `Snapshot`. Without this, a client holding a cursor from the old timeline would silently discard every new event as "already seen."

## Scoping and memory

Persistence bounds what survives a reload; **scope** bounds what's in memory. A mounted collection holds its working set in memory, so per-workspace scoped collections (`scopeOf`) plus `disposeScope` on exit are the lever for large datasets — not the persistence layer.

## See also

- [Persistence](./persistence.md) — the local SQLite layer collections write through.
- [Backend contract](./backend.md) — the server half of the loop.
- [Protocol reference](./protocol.md) — the event schemas and the squasher both ends share.
