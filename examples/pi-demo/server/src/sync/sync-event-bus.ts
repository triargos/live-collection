import { Context, Effect, Layer, PubSub, type Queue, type Scope } from "effect"
import type { SyncEvent } from "@triargos/live-collection-protocol"

export interface SyncEventBusShape {
  readonly publish: (event: SyncEvent) => Effect.Effect<void>
  readonly subscribe: Effect.Effect<Queue.Dequeue<SyncEvent>, never, Scope.Scope>
}

const make: Effect.Effect<SyncEventBusShape> = Effect.gen(function* () {
  const pubSub = yield* PubSub.unbounded<SyncEvent>()
  return {
    publish: (event) => PubSub.publish(pubSub, event).pipe(Effect.asVoid),
    subscribe: PubSub.subscribe(pubSub),
  }
})

export class SyncEventBus extends Context.Tag("pi-demo/SyncEventBus")<
  SyncEventBus,
  SyncEventBusShape
>() {
  static readonly layer: Layer.Layer<SyncEventBus> = Layer.effect(SyncEventBus, make)
}
