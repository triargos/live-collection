import { Context, Effect, Layer, Ref } from "effect"
import {
  compareSyncId,
  DeleteEvent,
  InsertEvent,
  type PendingSyncEvent,
  ResyncEvent,
  SyncId,
  type SyncEvent,
  UpdateEvent,
} from "@triargos/live-collection-protocol"

export interface SyncEventStoreShape {
  readonly append: (pending: PendingSyncEvent) => Effect.Effect<SyncEvent>
  readonly since: (from: SyncId) => Effect.Effect<ReadonlyArray<SyncEvent>>
  readonly currentSyncId: Effect.Effect<SyncId>
}

interface State {
  readonly counter: bigint
  readonly events: ReadonlyArray<SyncEvent>
}

const makeMemory: Effect.Effect<SyncEventStoreShape> = Effect.gen(function* () {
  const state = yield* Ref.make<State>({ counter: 0n, events: [] })

  return {
    append: (pending) =>
      Ref.modify(state, ({ counter, events }) => {
        const next = counter + 1n
        const assigned = {
          syncId: SyncId.make(String(next)),
          createdAt: new Date(),
        }
        const event = pending._tag === "Insert"
          ? InsertEvent.make({ ...pending, ...assigned })
          : pending._tag === "Update"
            ? UpdateEvent.make({ ...pending, ...assigned })
            : pending._tag === "Delete"
              ? DeleteEvent.make({ ...pending, ...assigned })
              : ResyncEvent.make({ ...pending, ...assigned })
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

export class SyncEventStore extends Context.Tag("pi-demo/SyncEventStore")<
  SyncEventStore,
  SyncEventStoreShape
>() {
  static readonly layerMemory: Layer.Layer<SyncEventStore> = Layer.effect(
    SyncEventStore,
    makeMemory,
  )
}
