import { Context, Effect, Layer } from "effect"
import type { PendingSyncEvent, SyncEvent } from "@triargos/live-collection-protocol"
import { SyncEventBus } from "./sync-event-bus.js"
import { SyncEventStore } from "./sync-event-store.js"

/**
 * The write-side entry: app handlers hand it a `PendingSyncEvent` after every
 * authoritative write.
 */
export interface SyncDispatcherShape {
  /**
   * Persist, then best-effort publish: a publish failure logs a warning and
   * never fails the write — catchup is the source of truth, so live delivery
   * may miss events. No echo suppression: originating clients receive their own
   * events back through normal sync (TanStack DB's optimistic-mutation
   * reconciliation depends on it).
   */
  readonly dispatch: (pending: PendingSyncEvent) => Effect.Effect<SyncEvent>
}

const make: Effect.Effect<SyncDispatcherShape, never, SyncEventStore | SyncEventBus> = Effect.gen(
  function* () {
    const store = yield* SyncEventStore
    const bus = yield* SyncEventBus
    return {
      dispatch: Effect.fn("SyncDispatcher.dispatch")(function* (pending) {
        const persisted = yield* store.appendEvent(pending)
        // The bus shape is infallible in the error channel, so adapter failures
        // arrive as defects — catch the full cause so a broken bus can never
        // fail (or kill) the write.
        yield* bus.publish(persisted).pipe(
          Effect.catchCause((cause) => Effect.logWarning("Sync bus publish failed", cause))
        )
        return persisted
      })
    }
  }
)

export class SyncDispatcher extends Context.Service<SyncDispatcher, SyncDispatcherShape>()(
  "live-collection-server/SyncDispatcher"
) {
  static readonly layer: Layer.Layer<SyncDispatcher, never, SyncEventStore | SyncEventBus> =
    Layer.effect(SyncDispatcher, make)
}
