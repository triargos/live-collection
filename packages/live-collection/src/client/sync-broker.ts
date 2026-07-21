import type { ModelName } from "@triargos/live-collection-protocol";
import { Context, Duration, Effect, Layer, Option, PubSub, type Scope, Stream } from "effect";
import type { SchemaVersion } from "../core/schema-version.js";
import { CatchupClient } from "./catchup-client.js";
import { makeIngest, PublishedItem, type RetentionOptions } from "./ingest.js";
import { makeLastAppliedTracker } from "./last-applied-tracker.js";
import { SyncSignal } from "./sync-signal.js";
import { keyFor, makeSubscribe } from "./subscribe.js";
import { SyncJournal } from "./sync-journal.js";
import { SyncTransport } from "./sync-transport.js";

export { SyncSignal } from "./sync-signal.js";

export interface SyncBrokerShape {
  /**
   * Attach a subscriber: replay its missing history, then continue with its live
   * tail, invoking `apply` sequentially per signal. The broker acks each signal
   * itself after `apply` returns — a subscriber cannot ack early, skip an ack, or
   * apply out of order. `apply` is infallible by contract: handle-or-log is the
   * subscriber's job (a defect kills the attachment fiber). Never completes;
   * interrupt to detach.
   *
   * `schemaVersion` identifies the saved rows the subscriber hydrates from — the
   * collection's last-applied syncId is read under `(key, schemaVersion)`, so a schema
   * change (which dumps the saved table) finds no record and decides `Snapshot`.
   */
  readonly attachSubscriber: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
    readonly apply: (signal: SyncSignal) => Effect.Effect<void>
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
  SyncTransport | CatchupClient | SyncJournal | Scope.Scope
> =>
  Effect.gen(function* () {
    const transport = yield* SyncTransport
    const catchup = yield* CatchupClient
    const journal = yield* SyncJournal
    const published = yield* PubSub.unbounded<PublishedItem>()

    const tracker = yield* makeLastAppliedTracker({
      journal,
      flushEvery: options.pendingLastAppliedFlushInterval ?? defaultOptions.pendingLastAppliedFlushInterval,
    })

    const subscribe = makeSubscribe({ journal, published, current: tracker.current })

    const start = makeIngest({
      transport,
      catchup,
      journal,
      publish: (item) => PubSub.publish(published, item).pipe(Effect.asVoid),
      onEpochReset: tracker.clear,
      flushLastApplied: tracker.flush,
      retention: options.retention ?? defaultOptions.retention,
    })

    // The syncId a fully-handled signal acks: a Snapshot covers everything through `at`.
    const syncIdOf = SyncSignal.$match({
      Snapshot: ({ at }) => at,
      Upsert: ({ syncId }) => syncId,
      Delete: ({ syncId }) => syncId,
    })

    const attachSubscriber: SyncBrokerShape["attachSubscriber"] = ({ modelName, scope, schemaVersion, apply }) =>
      Stream.runForEach(subscribe({ modelName, scope, schemaVersion }), (signal) =>
        apply(signal).pipe(
          Effect.andThen(
            tracker.markApplied({ key: keyFor(modelName, scope), schemaVersion, through: syncIdOf(signal) }),
          ),
        ),
      )

    return { attachSubscriber, start }
  })

export class SyncBroker extends Context.Service<SyncBroker, SyncBrokerShape>()("SyncBroker") {
  static readonly layer = (options?: SyncBrokerOptions): Layer.Layer<
    SyncBroker,
    never,
    SyncTransport | CatchupClient | SyncJournal
  > => Layer.effect(SyncBroker, make(options))
}
