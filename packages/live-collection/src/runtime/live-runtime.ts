import { Effect, Exit, Fiber, Layer, ManagedRuntime, Scope } from "effect"
import type { PersistedCollectionPersistence } from "@tanstack/db-sqlite-persistence-core"
import { CatchupClient } from "../client/catchup-client.js"
import { EventLogStore } from "../client/event-log-store.js"
import { LastSyncIdStore } from "../client/last-sync-id-store.js"
import { SyncBroker, type SyncBrokerOptions } from "../client/sync-broker.js"
import { SyncTransport } from "../client/sync-transport.js"
import { type CollectionRegistryShape, makeRegistry } from "../registry/collection-registry.js"

/** Services used by the shared broker ingest path. */
export type SyncDeps = SyncTransport | CatchupClient | LastSyncIdStore | EventLogStore

/** App-wide lifetime and sync runtime shared by every defined collection. */
export interface LiveRuntime {
  readonly registry: CollectionRegistryShape
  readonly persistence: PersistedCollectionPersistence
  /** Fork the broker ingest loop. Last call wins. */
  readonly forkSync: () => Fiber.RuntimeFiber<void>
  /** Internal collection-side drain executor. */
  readonly forkDrain: (drain: Effect.Effect<void, never, SyncBroker>) => Fiber.RuntimeFiber<void>
  readonly dispose: () => void
}

export const makeLiveRuntime = (config: {
  readonly persistence: PersistedCollectionPersistence
  readonly sync: Layer.Layer<SyncDeps>
  readonly broker?: SyncBrokerOptions
}): LiveRuntime => {
  const scope = Effect.runSync(Scope.make())
  const registry = Effect.runSync(Scope.extend(makeRegistry, scope))
  const runtime = ManagedRuntime.make(SyncBroker.layer(config.broker).pipe(Layer.provide(config.sync)))
  let syncFiber: Fiber.RuntimeFiber<void> | undefined

  return {
    registry,
    persistence: config.persistence,
    forkSync: () => {
      if (syncFiber !== undefined && syncFiber.unsafePoll() === null) {
        Effect.runFork(
          Effect.logWarning("[liveRuntime] forkSync while sync is already running — interrupting the previous fiber").pipe(
            Effect.zipRight(Fiber.interrupt(syncFiber)),
          ),
        )
      }
      syncFiber = runtime.runFork(
        SyncBroker.pipe(
          Effect.flatMap((broker) => broker.start),
          Effect.tapDefect((cause) => Effect.logError("[liveRuntime] SyncBroker died unexpectedly", cause)),
        ),
      )
      return syncFiber
    },
    forkDrain: (drain) => runtime.runFork(drain),
    dispose: () => {
      void runtime.dispose()
      Effect.runFork(Scope.close(scope, Exit.void))
    },
  }
}
