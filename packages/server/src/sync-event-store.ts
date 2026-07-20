import { Context, DateTime, Effect, Layer, Option, Random, Ref, Schema } from "effect"
import {
  compareSyncId,
  Epoch,
  PendingSyncEvent,
  SyncEvent,
  SyncId
} from "@triargos/live-collection-protocol"

/**
 * The requested cursor predates what this store retains: the events between the
 * cursor and the retention floor have been pruned, so the delta cannot be
 * honored. `SyncFeed.catchup` catches this and answers with a synthetic
 * `Resync(All)` — it never surfaces to the route.
 */
export class CursorOutOfRetentionError extends Schema.TaggedErrorClass<CursorOutOfRetentionError>()(
  "CursorOutOfRetentionError",
  { cursor: SyncId }
) {}

/**
 * The event-log port the app implements (the kernel ships {@link SyncEventStore.layerMemory}
 * for single-node/dev/test use). One append-only log of at-rest `SyncEvent`s,
 * ordered by `syncId`.
 */
export interface SyncEventStoreShape {
  /** Persist a pending event, assigning its `syncId` and `createdAt`. */
  readonly appendEvent: (pending: PendingSyncEvent) => Effect.Effect<SyncEvent>
  /**
   * The events with `syncId` strictly greater than `cursor` (exclusive), in
   * `syncId` order. Fails with {@link CursorOutOfRetentionError} when `cursor`
   * predates what the store retains.
   */
  readonly listEvents: (args: {
    readonly cursor: SyncId
  }) => Effect.Effect<ReadonlyArray<SyncEvent>, CursorOutOfRetentionError>
  /** The log's current head — what a client stores as its durable cursor. */
  readonly getLatestSyncId: Effect.Effect<SyncId>
  /**
   * This log's timeline identity. Must return the same value for the server's
   * lifetime; it changes only when the log's history is destroyed or replaced.
   * `None` ⇒ the log is durable for the server's lifetime and clients skip
   * epoch checking.
   */
  readonly getCurrentEpoch: Effect.Effect<Option.Option<Epoch>>
}

interface MemoryState {
  readonly counter: bigint
  readonly events: ReadonlyArray<SyncEvent>
}

// Memory logs reset on every boot, so each construction starts a new timeline:
// a fresh epoch lets clients holding durable old-timeline cursors detect the
// reset and self-heal instead of silently dropping every new event below their
// stale head. The store never prunes, so listEvents never fails.
const makeMemory: Effect.Effect<SyncEventStoreShape> = Effect.gen(function* () {
  const state = yield* Ref.make<MemoryState>({ counter: 0n, events: [] })
  // Minted through Effect's Random service (not crypto.randomUUID) so tests can
  // inject determinism. The epoch is a collision-resistant timeline identity,
  // not a security token — four ~53-bit draws are ample for that, and a store
  // needing cryptographic strength would be a durable adapter with a
  // stored-once epoch anyway.
  const epochParts = yield* Effect.all([
    Random.nextInt,
    Random.nextInt,
    Random.nextInt,
    Random.nextInt
  ])
  const epoch = Option.some(Epoch.make(epochParts.join(":")))

  return {
    appendEvent: (pending) =>
      Effect.gen(function* () {
        // Clock read happens before the atomic modify, so under concurrent
        // appends createdAt order may not match syncId order. That is fine:
        // the protocol orders exclusively by syncId; createdAt is informational.
        const createdAt = yield* DateTime.nowAsDate
        return yield* Ref.modify(state, ({ counter, events }) => {
          const next = counter + 1n
          const assigned = {
            syncId: SyncId.make(String(next)),
            createdAt
          }
          const event = PendingSyncEvent.match<SyncEvent>(pending, {
            Insert: (p) => SyncEvent.cases.Insert.make({ ...p, ...assigned }),
            Update: (p) => SyncEvent.cases.Update.make({ ...p, ...assigned }),
            Delete: (p) => SyncEvent.cases.Delete.make({ ...p, ...assigned }),
            Resync: (p) => SyncEvent.cases.Resync.make({ ...p, ...assigned })
          })
          return [event, { counter: next, events: [...events, event] }]
        })
      }),
    listEvents: ({ cursor }) =>
      Ref.get(state).pipe(
        Effect.map(({ events }) => events.filter((event) => compareSyncId(event.syncId, cursor) > 0))
      ),
    getLatestSyncId: Ref.get(state).pipe(Effect.map(({ counter }) => SyncId.make(String(counter)))),
    getCurrentEpoch: Effect.succeed(epoch)
  }
})

export class SyncEventStore extends Context.Service<SyncEventStore, SyncEventStoreShape>()(
  "live-collection-server/SyncEventStore"
) {
  static readonly layerMemory: Layer.Layer<SyncEventStore> = Layer.effect(SyncEventStore, makeMemory)
}
