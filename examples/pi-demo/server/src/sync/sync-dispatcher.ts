import { Context, Effect, Layer } from "effect"
import type { PendingSyncEvent, SyncEvent } from "@triargos/live-collection-protocol"
import { SyncEventBus } from "./sync-event-bus.js"
import { SyncEventStore } from "./sync-event-store.js"

export interface SyncDispatcherShape {
  readonly dispatch: (pending: PendingSyncEvent) => Effect.Effect<SyncEvent>
}

const make: Effect.Effect<SyncDispatcherShape, never, SyncEventStore | SyncEventBus> =
  Effect.gen(function* () {
    const store = yield* SyncEventStore
    const bus = yield* SyncEventBus
    return {
      dispatch: Effect.fn("SyncDispatcher.dispatch")(function* (pending) {
        const persisted = yield* store.append(pending)
        yield* bus.publish(persisted).pipe(
          Effect.catchAll((cause) =>
            Effect.logWarning("Sync bus publish failed", cause),
          ),
        )
        return persisted
      }),
    }
  })

export class SyncDispatcher extends Context.Tag("pi-demo/SyncDispatcher")<
  SyncDispatcher,
  SyncDispatcherShape
>() {
  static readonly layer: Layer.Layer<SyncDispatcher, never, SyncEventStore | SyncEventBus> =
    Layer.effect(SyncDispatcher, make)
}
