import { Effect, Exit, Fiber, Layer, ManagedRuntime, Scope } from "effect"
import type { PersistedCollectionPersistence } from "@tanstack/db-sqlite-persistence-core"
import { CollectionRegistry, type CollectionRegistryShape, makeRegistry } from "../registry/collection-registry.js"
import type { SyncModels } from "../registry/define-collection.js"
import { CatchupClient } from "../client/catchup-client.js"
import { LastSyncIdStore } from "../client/last-sync-id-store.js"
import { SyncTransport } from "../client/sync-transport.js"
import { EventLogStore } from "../client/event-log-store.js"
import { syncLoop } from "../client/sync-loop.js"

/**
 * The services the sync loop needs: the live stream, the catchup client, the durable
 * cursor, and the durable event log. The app supplies them as one merged layer — the
 * `loop` argument of {@link makeLiveRuntime}.
 */
export type LoopDeps = SyncTransport | CatchupClient | LastSyncIdStore | EventLogStore

/**
 * The app-wide live-sync runtime, built once at startup by {@link makeLiveRuntime} and
 * passed to every `defineCollection`. It has two surfaces: the **mount** surface
 * (`registry` + `persistence`) is synchronous, so collection handles can mount during
 * render; the **loop** surface (`forkLoop`) runs the async catchup/tail fiber off the
 * render path. Both share one registry, so the loop writes to exactly the instances the
 * UI mounted.
 */
export interface LiveRuntime {
  /**
   * The collection registry — the canonical instance cache. Use it for lifecycle calls:
   * `disposeScope(orgId)` when leaving a workspace, `disposeAll()` on logout.
   */
  readonly registry: CollectionRegistryShape
  /** The app-owned persistence value (local SQLite), threaded into each collection it builds. */
  readonly persistence: PersistedCollectionPersistence
  /**
   * Fork the forever sync loop for `models`. Interrupt the returned fiber to stop it —
   * this stops syncing but does NOT dispose collections (registry lifetime is the
   * app's). `useLiveSync` calls this on mount. Last call wins: forking while a previous
   * loop is still running interrupts the previous one (two concurrent loops would fight
   * over the registry's single-consumer mount queue).
   */
  readonly forkLoop: (models: SyncModels) => Fiber.RuntimeFiber<void>
  /** Tear down the loop runtime and the registry's long-lived scope (app teardown / logout). */
  readonly dispose: () => void
}

/**
 * Build the app-wide {@link LiveRuntime} — once, at startup, before defining collections.
 *
 * - `persistence`: the local-SQLite persistence value. In the browser, open the database
 *   once and wrap it (see the example).
 * - `loop`: one merged layer providing {@link LoopDeps} — the transport, catchup client,
 *   cursor store, and event log.
 * - `onResync`: what to do when the server declares local state unsalvageable via a live
 *   `Resync` event. {@link reloadWindow} (the recommended default) reloads the app; the
 *   next start then re-fetches from scratch.
 *
 * @example
 * ```ts
 * import { openBrowserWASQLiteOPFSDatabase, createBrowserWASQLitePersistence } from "@tanstack/browser-db-sqlite-persistence"
 * import { FetchHttpClient } from "@effect/platform"
 * import { Layer } from "effect"
 *
 * const database = await openBrowserWASQLiteOPFSDatabase({ databaseName: "myapp" })
 *
 * export const runtime = makeLiveRuntime({
 *   persistence: createBrowserWASQLitePersistence({ database }),
 *   loop: Layer.mergeAll(
 *     SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" }),
 *     CatchupClient.layer({ url: "/api/catchup" }),
 *     LastSyncIdStore.layer,
 *     EventLogStore.layer(),
 *   ).pipe(Layer.provide(FetchHttpClient.layer)),
 *   onResync: reloadWindow,
 * })
 * ```
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
  let loopFiber: Fiber.RuntimeFiber<void> | undefined

  return {
    registry,
    persistence: config.persistence,
    // tapDefect: the loop's error channel is `never`, so the only way it dies is a defect — and a
    // forked fiber dies silently (sync just stops). Surface it; interruption (unmount) is not a defect.
    forkLoop: (models) => {
      if (loopFiber !== undefined && loopFiber.unsafePoll() === null) {
        Effect.runFork(
          Effect.logWarning("[liveRuntime] forkLoop while a loop is already running — interrupting the previous loop").pipe(
            Effect.zipRight(Fiber.interrupt(loopFiber)),
          ),
        )
      }
      loopFiber = loopRuntime.runFork(
        syncLoop(models, config.onResync).pipe(
          Effect.tapDefect((cause) => Effect.logError("[liveRuntime] sync loop died unexpectedly", cause)),
        ),
      )
      return loopFiber
    },
    dispose: () => {
      void loopRuntime.dispose()
      Effect.runFork(Scope.close(scope, Exit.void))
    },
  }
}

/**
 * The recommended `onResync` action: reload the whole app. The reloaded app starts with
 * a cleared cursor, catches up cold, and re-snapshots — the simplest way to guarantee
 * convergence after the server invalidates local state.
 */
export const reloadWindow: Effect.Effect<void> = Effect.sync(() => window.location.reload())
