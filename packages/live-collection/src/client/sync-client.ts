import { Context, Data, Effect, Layer, Option, Schedule, Stream } from "effect"
import {
  type CatchupResponse,
  type HydratedSyncEventEnvelope,
  type ModelId,
  SyncId,
} from "@triargos/live-collection-protocol"
import type { CollectionRegistry } from "../registry/collection-registry.js"
import type { MountRef } from "../registry/define-collection.js"
import type { LiveCollection } from "../persistence/live-collection.js"
import { SyncDispatcher } from "../dispatch/sync-dispatcher.js"
import { LastSyncIdStore } from "./last-sync-id-store.js"
import { CatchupClient } from "./catchup-client.js"
import { SyncTransport } from "./sync-transport.js"

/**
 * One collection the orchestrator should snapshot on a cold/too-old start: a mountable handle plus
 * how to fetch its authoritative current state. {@link bootstrapFn} returns **rows only** — the
 * cursor is the sync stream's job (DEC-T3) — and {@link getKey} lets the snapshot reconcile, removing
 * rows the server no longer has (delete-absent, DEC-T9). `R` is the app's API-client requirement.
 */
export interface BootstrapSpec<T extends object, R> {
  readonly mount: MountRef<LiveCollection<T>, R>
  readonly bootstrapFn: Effect.Effect<ReadonlyArray<T>, never, R>
  readonly getKey: (entity: T) => ModelId
}

/**
 * Builds a {@link BootstrapSpec}, erasing the entity type so a heterogeneous set can be passed to
 * {@link SyncClientShape.start} (mirrors `dispatchEntry`). It only constructs a value.
 */
export const bootstrapSpec = <T extends object, R>(spec: BootstrapSpec<T, R>): BootstrapSpec<object, R> =>
  spec as unknown as BootstrapSpec<object, R>

export interface SyncClientShape {
  /**
   * The read path. Runs forever (fork it). Each cycle: catchup from the stored cursor (`"0"` when
   * cold), then tail the live SSE stream; reconnect re-runs catchup so the disconnect gap heals.
   *
   * - A catchup response carrying a `Resync` (cursor too old / log resync) ⇒ **snapshot** every spec
   *   via `bootstrapFn` and reconcile; otherwise dispatch the returned deltas. The cursor is then set
   *   from `lastSyncId`, always (DEC-T5).
   * - A **live** `Resync` ⇒ `cursor.clear` then `onResync` (prod: reload), and the loop stops — the
   *   reboot re-bootstraps cold (DEC-T6). Entity events are routed to the dispatcher.
   */
  readonly start: <R>(
    specs: ReadonlyArray<BootstrapSpec<object, R>>,
  ) => Effect.Effect<void, never, CollectionRegistry | SyncDispatcher | R>
}

/** Internal control signal: a live resync was seen, so stop the tail (it must not be retried). */
class ResyncStop extends Data.TaggedError("ResyncStop")<{}> {}

const make = (config: {
  readonly onResync: Effect.Effect<void>
}): Effect.Effect<SyncClientShape, never, LastSyncIdStore | CatchupClient | SyncTransport> =>
  Effect.gen(function* () {
    const store = yield* LastSyncIdStore
    const catchup = yield* CatchupClient
    const transport = yield* SyncTransport

    const start = <R>(specs: ReadonlyArray<BootstrapSpec<object, R>>) =>
      Effect.gen(function* () {
        const dispatcher = yield* SyncDispatcher

        // Mount every spec once so live dispatch (which only routes to mounted collections) has targets.
        yield* Effect.forEach(specs, (spec) => spec.mount, { discard: true })

        // Snapshot one collection: replace its contents with the server's current rows.
        const snapshot = (spec: BootstrapSpec<object, R>) =>
          Effect.gen(function* () {
            const collection = yield* spec.mount
            const rows = yield* spec.bootstrapFn
            const fetched = new Set(rows.map(spec.getKey))
            const absent = Array.from(collection.keys()).filter((key) => !fetched.has(key))
            yield* Effect.forEach(rows, (row) => collection.utils.writeSynced(row), { discard: true })
            yield* Effect.forEach(absent, (key) => collection.utils.deleteSynced(key), { discard: true })
          })

        const applyCatchup = (response: CatchupResponse) =>
          Effect.gen(function* () {
            if (response.events.some((event) => event._tag === "Resync")) {
              yield* Effect.forEach(specs, snapshot, { discard: true })
            } else {
              yield* Effect.forEach(
                response.events,
                (event) => (event._tag === "Resync" ? Effect.void : dispatcher.dispatch(event)),
                { discard: true },
              )
            }
            yield* store.set(response.lastSyncId)
          })

        const route = (event: HydratedSyncEventEnvelope) =>
          event._tag === "Resync"
            ? store.clear.pipe(Effect.zipRight(config.onResync), Effect.zipRight(Effect.fail(new ResyncStop())))
            : dispatcher.dispatch(event).pipe(Effect.zipRight(store.set(event.syncId)))

        const cycle = Effect.gen(function* () {
          const from = Option.getOrElse(yield* store.get, () => SyncId.make("0"))
          const response = yield* catchup.fetch({ from }).pipe(
            Effect.map(Option.some),
            Effect.catchTag("CatchupFailed", (error) =>
              Effect.logWarning(`[SyncClient] catchup failed, tailing anyway: ${error.reason}`).pipe(
                Effect.as(Option.none<CatchupResponse>()),
              ),
            ),
          )
          yield* Option.match(response, { onNone: () => Effect.void, onSome: applyCatchup })
          yield* Stream.runForEach(transport.connect, route)
        })

        yield* cycle.pipe(
          Effect.retry({
            while: (error) => error._tag === "SyncConnectionLost",
            schedule: Schedule.spaced("3 seconds"),
          }),
          Effect.catchTags({
            // A live resync stopped the tail — done (prod has already reloaded).
            ResyncStop: () => Effect.void,
            // Unreachable: the infinite `spaced` schedule retries every drop. Present only so the
            // public error channel is `never` — a drop never escapes `start`.
            SyncConnectionLost: () => Effect.void,
          }),
        )
      })

    return { start }
  })

/** The seam: `yield* SyncClient`. */
export class SyncClient extends Context.Tag("SyncClient")<SyncClient, SyncClientShape>() {
  static readonly layer = (config: {
    readonly onResync: Effect.Effect<void>
  }): Layer.Layer<SyncClient, never, LastSyncIdStore | CatchupClient | SyncTransport> =>
    Layer.effect(SyncClient, make(config))
}

/** The default prod resync action: reload the whole app (Model A, DEC-T7). Apps opt in via config. */
export const reloadWindow: Effect.Effect<void> = Effect.sync(() => window.location.reload())
