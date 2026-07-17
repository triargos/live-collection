import { Context, Effect, Layer, PubSub, type Scope } from "effect"
import type { SyncEvent } from "@triargos/live-collection-protocol"

export interface SyncEventBusShape {
  readonly publish: (event: SyncEvent) => Effect.Effect<void>
  readonly subscribe: Effect.Effect<PubSub.Subscription<SyncEvent>, never, Scope.Scope>
}

const make: Effect.Effect<SyncEventBusShape> = Effect.gen(function* () {
  const pubSub = yield* PubSub.unbounded<SyncEvent>()
  return {
    publish: (event) => PubSub.publish(pubSub, event).pipe(Effect.asVoid),
    subscribe: PubSub.subscribe(pubSub),
  }
})

export class SyncEventBus extends Context.Service<SyncEventBus, SyncEventBusShape>()("pi-demo/SyncEventBus") {
  static readonly layer: Layer.Layer<SyncEventBus> = Layer.effect(SyncEventBus, make)
}
