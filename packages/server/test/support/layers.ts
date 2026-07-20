import { type Effect, Layer } from "effect"
import type { ModelDescriptor } from "@triargos/live-collection-protocol"
import { ModelRegistry } from "../../src/model-registry.js"
import { SyncDispatcher } from "../../src/sync-dispatcher.js"
import { SyncEventBus } from "../../src/sync-event-bus.js"
import { SyncEventStore } from "../../src/sync-event-store.js"
import { SyncFeed } from "../../src/sync-feed.js"
import { NoteRepo, testRegistry } from "./test-registry.js"

/**
 * The full kernel wired exactly as a production app wires it: memory adapters
 * behind the ports, the registry build effect lifted with ModelRegistry.layer,
 * repos provided where layers compose. Exposes every service so tests drive
 * real seams.
 */
export const makeKernelLayer = (
  registry: Effect.Effect<
    Record<string, ModelDescriptor<string, any, never>>,
    never,
    NoteRepo
  > = testRegistry
) => {
  const infrastructure = Layer.mergeAll(
    SyncEventStore.layerMemory,
    SyncEventBus.layerMemory,
    NoteRepo.layerMemory
  )
  return Layer.mergeAll(
    SyncDispatcher.layer,
    SyncFeed.layer.pipe(Layer.provide(ModelRegistry.layer(registry)))
  ).pipe(Layer.provideMerge(infrastructure))
}
