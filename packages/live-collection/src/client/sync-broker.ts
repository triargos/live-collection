import type { ModelName, SyncId } from "@triargos/live-collection-protocol";
import { Context, Duration, Effect, Layer, Option, PubSub, type Scope, Stream } from "effect";
import type { SchemaVersion } from "../core/schema-version.js";
import { CatchupClient } from "./catchup-client.js";
import { makeIngest, PublishedItem, type RetentionOptions } from "./ingest.js";
import { makeLastAppliedTracker } from "./last-applied-tracker.js";
import { SyncCursor } from "./sync-cursor.js";
import type { SyncSignal } from "./sync-signal.js";
import { keyFor, makeSubscribe } from "./subscribe.js";
import { SyncJournal } from "./sync-journal.js";
import { SyncTransport } from "./sync-transport.js";

export { SyncSignal } from "./sync-signal.js";

export interface SyncBrokerShape {
  /**
   * Replay this collection's missing history, then continue with its live tail.
   * `schemaVersion` identifies the saved rows the subscriber hydrates from — the
   * collection's last-applied syncId is read under `(key, schemaVersion)`, so a schema
   * change (which dumps the saved table) finds no record and decides `Snapshot`.
   */
  readonly subscribe: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
  }) => Stream.Stream<SyncSignal>

  /** Record that a subscriber has applied every relevant signal through this sync id. */
  readonly markApplied: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
    readonly through: SyncId
  }) => Effect.Effect<void>

  /** Run the single catchup and live-event ingest fiber. Fork exactly once. */
  readonly start: Effect.Effect<void>
}

export interface SyncBrokerOptions {
  readonly retention?: RetentionOptions
  readonly pendingLastAppliedFlushInterval?: Duration.Input
}

const defaultOptions = {
  retention: { maxEventsPerModel: 1000, maxEventsTotal: 5000, trimEveryEvents: 100 },
  pendingLastAppliedFlushInterval: Duration.millis(100)
}

/**
 * Wiring only. Three internal machines, three narrow channels:
 *
 * - INGEST (`makeIngest`) — network → journal + fanout. Owns the cycle, epoch
 *   handling, and retention.
 * - SERVE (`makeSubscribe`) — journal + fanout → one stream per subscriber mount.
 * - ACK (`makeLastAppliedTracker`) — subscriber acks → batched durable last-applied
 *   marks; the single authority for reading them back.
 *
 * They touch each other only through the journal, the `PublishedItem` PubSub, and the
 * tracker's `current`/`clear` — handed over explicitly here, never shared via closure.
 */
const make = (options: SyncBrokerOptions = {}): Effect.Effect<
  SyncBrokerShape,
  never,
  SyncTransport | CatchupClient | SyncCursor | SyncJournal | Scope.Scope
> =>
  Effect.gen(function* () {
    const transport = yield* SyncTransport
    const catchup = yield* CatchupClient
    const cursorStore = yield* SyncCursor
    const journal = yield* SyncJournal
    const published = yield* PubSub.unbounded<PublishedItem>()

    const tracker = yield* makeLastAppliedTracker({
      journal,
      flushEvery: options.pendingLastAppliedFlushInterval ?? defaultOptions.pendingLastAppliedFlushInterval,
    })

    const subscribe = makeSubscribe({ journal, cursorStore, published, current: tracker.current })

    const start = makeIngest({
      transport,
      catchup,
      journal,
      cursorStore,
      publish: (item) => PubSub.publish(published, item).pipe(Effect.asVoid),
      onEpochReset: tracker.clear,
      flushLastApplied: tracker.flush,
      retention: options.retention ?? defaultOptions.retention,
    })

    const markApplied: SyncBrokerShape["markApplied"] = ({ modelName, scope, schemaVersion, through }) =>
      tracker.markApplied({ key: keyFor(modelName, scope), schemaVersion, through })

    return { subscribe, markApplied, start }
  })

export class SyncBroker extends Context.Service<SyncBroker, SyncBrokerShape>()("SyncBroker") {
  static readonly layer = (options?: SyncBrokerOptions): Layer.Layer<
    SyncBroker,
    never,
    SyncTransport | CatchupClient | SyncCursor | SyncJournal
  > => Layer.effect(SyncBroker, make(options))
}
