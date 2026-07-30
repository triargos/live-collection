import { Context, Effect, Layer, PubSub, Stream } from "effect"
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
  /**
   * The live tail. Each run is one independent subscriber: it registers on first
   * pull and unregisters when the stream ends or is interrupted, so an adapter must
   * not hand out a shared, already-subscribed stream — a dropped SSE connection has
   * to release its subscriber, or the bus keeps feeding a queue nobody drains.
   *
   * Events published before a run's first pull are not delivered to it. That is safe
   * because catchup, not the bus, is the source of truth: the client's durable cursor
   * recovers anything the tail missed.
   */
  readonly events: Stream.Stream<SyncEvent>
}

const makeMemory: Effect.Effect<SyncEventBusShape> = Effect.gen(function* () {
  const pubSub = yield* PubSub.unbounded<SyncEvent>()
  return {
    publish: (event) => PubSub.publish(pubSub, event).pipe(Effect.asVoid),
    events: Stream.fromPubSub(pubSub)
  }
})

export class SyncEventBus extends Context.Tag("live-collection-server/SyncEventBus")<
  SyncEventBus,
  SyncEventBusShape
>() {
  static readonly layerMemory: Layer.Layer<SyncEventBus> = Layer.effect(SyncEventBus, makeMemory)
}
