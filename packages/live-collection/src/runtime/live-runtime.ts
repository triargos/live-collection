import { Effect, Exit, Fiber, Layer, ManagedRuntime, Scope } from "effect"
import type { PersistedCollectionPersistence } from "@tanstack/db-sqlite-persistence-core"
import { CollectionRegistry, type CollectionRegistryShape, makeRegistry } from "../registry/collection-registry.js"
import type { SyncMap } from "../registry/define-collection.js"
import { CatchupClient } from "../client/catchup-client.js"
import { LastSyncIdStore } from "../client/last-sync-id-store.js"
import { SyncTransport } from "../client/sync-transport.js"
import { EventLogStore } from "../client/event-log-store.js"
import { syncLoop } from "../client/sync-loop.js"

/** The deps the async sync loop needs; the app supplies them as one merged layer (`loop`). */
export type LoopDeps = SyncTransport | CatchupClient | LastSyncIdStore | EventLogStore

/**
 * The two-surface live runtime (DEC-R8). The **mount** surface (`registry` + `persistence`) is a sync
 * value the collection handles `runSync` against during render. The **loop** surface (`forkLoop`) runs
 * the async catchup/cursor/tail fiber off the render path. The registry is shared into the loop, so
 * dispatch writes to exactly the instances the UI mounted.
 */
export interface LiveRuntime {
  /** Built once in a long-lived scope; the handle mounts against it (sync). */
  readonly registry: CollectionRegistryShape
  /** App-owned persistence value, threaded into each collection's `make`. */
  readonly persistence: PersistedCollectionPersistence
  /** Fork the forever sync loop for `map`. Interrupt the returned fiber to stop it (does NOT dispose
   *  collections — registry lifetime is the app's, DEC-R8). Called by `useLiveSync` on mount. */
  readonly forkLoop: (map: SyncMap) => Fiber.RuntimeFiber<void>
  /** Tear down the loop runtime and the registry's long-lived scope (app teardown / logout). */
  readonly dispose: () => void
}

/**
 * Build a {@link LiveRuntime}. `persistence` is the app value (prod: `createBrowserWASQLitePersistence({
 * database })` from `@tanstack/browser-db-sqlite-persistence`, where `database` is opened once at startup
 * via `await openBrowserWASQLiteOPFSDatabase({ databaseName })`);
 * `loop` is the transport/catchup/cursor layer; `onResync` is the live-resync action (prod:
 * {@link reloadWindow}). The registry is created synchronously in a long-lived scope and also handed
 * to the loop's ManagedRuntime via `Layer.succeed`, so both surfaces share one instance.
 */
export const makeLiveRuntime = (config: {
  readonly persistence: PersistedCollectionPersistence
  readonly loop: Layer.Layer<LoopDeps>
  readonly onResync: Effect.Effect<void>
}): LiveRuntime => {
  const scope = Effect.runSync(Scope.make())
  const registry = Effect.runSync(Scope.extend(makeRegistry, scope))
  const loopRuntime = ManagedRuntime.make(
    Layer.merge(config.loop, Layer.succeed(CollectionRegistry, registry)),
  )

  return {
    registry,
    persistence: config.persistence,
    // tapDefect: the loop's error channel is `never`, so the only way it dies is a defect — and a
    // forked fiber dies silently (sync just stops). Surface it; interruption (unmount) is not a defect.
    forkLoop: (map) =>
      loopRuntime.runFork(
        syncLoop(map, config.onResync).pipe(
          Effect.tapDefect((cause) => Effect.logError("[liveRuntime] sync loop died unexpectedly", cause)),
        ),
      ),
    dispose: () => {
      void loopRuntime.dispose()
      Effect.runFork(Scope.close(scope, Exit.void))
    },
  }
}

/** The default prod resync action: reload the whole app (Model A, DEC-T6/T7). */
export const reloadWindow: Effect.Effect<void> = Effect.sync(() => window.location.reload())
