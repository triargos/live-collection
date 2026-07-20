import { Context, Effect, Layer, PubSub, type Scope } from "effect"
import type { SyncEvent } from "@triargos/live-collection-protocol"

/**
 * The in-process fan-out from writers ({@link SyncDispatcher}) to live
 * subscribers (`SyncFeed.streamEvents`). Swappable: the shipped
 * {@link SyncEventBus.layerMemory} is correct for a single node; multi-node
 * deployments supply their own adapter (Redis pub/sub, Postgres NOTIFY, …) —
 * catchup remains the source of truth either way, so a lost publish heals on
 * the next reconnect.
 */
export interface SyncEventBusShape {
  readonly publish: (event: SyncEvent) => Effect.Effect<void>
  readonly subscribe: Effect.Effect<PubSub.Subscription<SyncEvent>, never, Scope.Scope>
}

const makeMemory: Effect.Effect<SyncEventBusShape> = Effect.gen(function* () {
  const pubSub = yield* PubSub.unbounded<SyncEvent>()
  return {
    publish: (event) => PubSub.publish(pubSub, event).pipe(Effect.asVoid),
    subscribe: PubSub.subscribe(pubSub)
  }
})

export class SyncEventBus extends Context.Service<SyncEventBus, SyncEventBusShape>()(
  "live-collection-server/SyncEventBus"
) {
  static readonly layerMemory: Layer.Layer<SyncEventBus> = Layer.effect(SyncEventBus, makeMemory)
}
