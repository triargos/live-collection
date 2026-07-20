import { Context, Effect, Layer, Ref } from "effect"
import {
  compareSyncId,
  Epoch,
  PendingSyncEvent,
  SyncId,
  SyncEvent,
} from "@triargos/live-collection-protocol"

export interface SyncEventStoreShape {
  readonly append: (pending: PendingSyncEvent) => Effect.Effect<SyncEvent>
  readonly since: (from: SyncId) => Effect.Effect<ReadonlyArray<SyncEvent>>
  readonly currentSyncId: Effect.Effect<SyncId>
  /**
   * This log's timeline identity, minted fresh at construction. The store is memory-only,
   * so every server boot starts a new timeline at syncId 0 — sending the epoch on catchup
   * lets clients holding durable old-timeline cursors detect the reset and self-heal
   * instead of silently dropping every new event below their stale head.
   */
  readonly epoch: Epoch
}

interface State {
  readonly counter: bigint
  readonly events: ReadonlyArray<SyncEvent>
}

const makeMemory: Effect.Effect<SyncEventStoreShape> = Effect.gen(function* () {
  const state = yield* Ref.make<State>({ counter: 0n, events: [] })
  const epoch = Epoch.make(crypto.randomUUID())

  return {
    epoch,
    append: (pending) =>
      Ref.modify(state, ({ counter, events }) => {
        const next = counter + 1n
        const assigned = {
          syncId: SyncId.make(String(next)),
          createdAt: new Date(),
        }
        const event = PendingSyncEvent.match<SyncEvent>(pending, {
          Insert: (p) => SyncEvent.cases.Insert.make({ ...p, ...assigned }),
          Update: (p) => SyncEvent.cases.Update.make({ ...p, ...assigned }),
          Delete: (p) => SyncEvent.cases.Delete.make({ ...p, ...assigned }),
          Resync: (p) => SyncEvent.cases.Resync.make({ ...p, ...assigned }),
        })
        return [event, { counter: next, events: [...events, event] }]
      }),
    since: (from) =>
      Ref.get(state).pipe(
        Effect.map(({ events }) =>
          events.filter((event) => compareSyncId(event.syncId, from) > 0),
        ),
      ),
    currentSyncId: Ref.get(state).pipe(
      Effect.map(({ counter }) => SyncId.make(String(counter))),
    ),
  }
})

export class SyncEventStore extends Context.Service<SyncEventStore, SyncEventStoreShape>()("pi-demo/SyncEventStore") {
  static readonly layerMemory: Layer.Layer<SyncEventStore> = Layer.effect(
    SyncEventStore,
    makeMemory,
  )
}
