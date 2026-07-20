import {
    type CatchupResponse,
    HydratedSyncEventEnvelope,
    intersects,
    ResyncTarget,
    squash,
    type SyncGroup,
    type SyncId
} from "@triargos/live-collection-protocol";
import { Context, DateTime, Duration, Effect, Layer, Schema, type Scope, Stream } from "effect";
import * as Arr from "effect/Array";
import { makeHydrator } from "./hydrator.js";
import { ModelRegistry } from "./model-registry.js";
import { SyncEventBus } from "./sync-event-bus.js";
import { SyncEventStore } from "./sync-event-store.js";

/**
 * The read-side entry — the two surfaces the client contract observes. The
 * app's routes own auth and resolve the caller's sync groups server-side; the
 * feed owns everything the client's correctness depends on.
 */
export interface SyncFeedShape {
  /**
   * One catchup page: `listEvents` → `intersects` visibility filter → `squash`
   * → hydrate (batched, `Option.none` → synthetic `Delete`, unknown model →
   * log + drop) → `{ events, lastSyncId, epoch }`. A cursor that predates
   * retention (`CursorOutOfRetentionError`) becomes a single synthetic
   * `Resync(All)` — synthesized inline, never written to the log, never an
   * error to the route.
   */
  readonly catchup: (args: {
    readonly fromSyncId: SyncId
    readonly syncGroups: ReadonlyArray<SyncGroup>
  }) => Effect.Effect<CatchupResponse>

  /**
   * Ready-to-send SSE frame strings: bus subscription → `intersects` filter →
   * hydrate → `data: <json>\n\n`, merged with `:ka\n\n` keepalive comments.
   * A hydration or encode failure is logged and skipped, never fatal. The
   * default 15s keepalive must undercut the client transport's configured
   * silence window.
   */
  readonly streamEvents: (args: {
    readonly syncGroups: ReadonlyArray<SyncGroup>
    readonly keepAlive?: Duration.Input
  }) => Stream.Stream<string, never, Scope.Scope>
}

const encodeEnvelope = Schema.encodeEffect(Schema.fromJsonString(HydratedSyncEventEnvelope))

const make: Effect.Effect<SyncFeedShape, never, SyncEventStore | SyncEventBus | ModelRegistry> =
  Effect.gen(function* () {
    const store = yield* SyncEventStore
    const bus = yield* SyncEventBus
    const hydrator = makeHydrator(yield* ModelRegistry)

    const catchup: SyncFeedShape["catchup"] = Effect.fn("SyncFeed.catchup")(function* (args) {
      // Head first, then the slice: events appended in between simply arrive
      // with syncIds above lastSyncId, and the client's cursor advances past
      // them per event. The reverse order could hand out a head beyond events
      // the slice never contained.
      const lastSyncId = yield* store.getLatestSyncId
      const epoch = yield* store.getCurrentEpoch
      return yield* store.listEvents({ cursor: args.fromSyncId }).pipe(
        Effect.flatMap((listed) =>
          Effect.gen(function* () {
            const visible = listed.filter((event) =>
              intersects(event.syncGroups, args.syncGroups)
            )
            const events = yield* hydrator.hydrateEvents({
              events: squash(visible),
              syncGroups: args.syncGroups
            })
            return { events, lastSyncId, epoch }
          })
        ),
        Effect.catchTag("CursorOutOfRetentionError", () =>
          Effect.gen(function* () {
            yield* Effect.logInfo(
              `Catchup cursor ${args.fromSyncId} predates retention; answering with Resync(All)`
            )
            // A caller without any sync group holds no visible data to reset.
            const createdAt = yield* DateTime.nowAsDate
            const events = Arr.isReadonlyArrayNonEmpty(args.syncGroups)
              ? [
                  HydratedSyncEventEnvelope.cases.Resync.make({
                    target: ResyncTarget.cases.All.make({}),
                    syncGroups: args.syncGroups,
                    syncId: lastSyncId,
                    createdAt
                  })
                ]
              : []
            return { events, lastSyncId, epoch }
          })
        )
      )
    })

    const streamEvents: SyncFeedShape["streamEvents"] = ({ keepAlive = Duration.seconds(15), syncGroups }) => {
      const frames = Stream.unwrap(
        Effect.map(bus.subscribe, (subscription) =>
          Stream.fromSubscription(subscription).pipe(
            Stream.filter((event) => intersects(event.syncGroups, syncGroups)),
            Stream.mapEffect((event) => hydrator.hydrateEvents({ events: [event], syncGroups })),
            Stream.flatMap(Stream.fromIterable),
            Stream.mapEffect((envelope) =>
              encodeEnvelope(envelope).pipe(
                Effect.map((json) => [`data: ${json}\n\n`]),
                Effect.catch((error) =>
                  Effect.logWarning("Skipping SSE frame: envelope failed to encode", error).pipe(
                    Effect.as<ReadonlyArray<string>>([])
                  )
                )
              )
            ),
            Stream.flatMap(Stream.fromIterable)
          )
        )
      )
      const keepAliveFrames = Stream.tick(keepAlive).pipe(Stream.map(() => ":ka\n\n"))
      return Stream.merge(frames, keepAliveFrames).pipe(Stream.withSpan("SyncFeed.streamEvents"))
    }

    return { catchup, streamEvents }
  })

export class SyncFeed extends Context.Service<SyncFeed, SyncFeedShape>()(
  "live-collection-server/SyncFeed"
) {
  /**
   * A plain constant: the registry arrives as the `ModelRegistry` service,
   * built with `ModelRegistry.layer(registry)` — which is where the
   * descriptors' repo requirements are inferred and closed.
   */
  static readonly layer: Layer.Layer<SyncFeed, never, SyncEventStore | SyncEventBus | ModelRegistry> =
    Layer.effect(SyncFeed, make)
}
