import { maxSyncId, ModelName, type SyncId } from "@triargos/live-collection-protocol";
import { Context, Duration, Effect, Layer, Option, PubSub, Schema, type Scope, Stream } from "effect";
import type { SchemaVersion } from "../core/schema-version.js";
import { subsetKey, type SubsetKey } from "../core/collection-key.js";
import { type BatchLoader, makeBatchLoader } from "./batch-loader.js";

import { CatchupClient } from "./catchup-client.js";
import { HydrateClient, HydrateFailed } from "./hydrate-client.js";
import { makeIngest, PublishedItem, type RetentionOptions } from "./ingest.js";
import { makeLastAppliedTracker } from "./last-applied-tracker.js";
import { MountDecision, planMount, signalFromRow } from "./mount-plan.js";
import { SyncSignal } from "./sync-signal.js";
import { keyFor, makeSubscribe } from "./subscribe.js";
import { SyncJournal } from "./sync-journal.js";
import { SyncTransport } from "./sync-transport.js";

export { SyncSignal } from "./sync-signal.js";

/**
 * The server refused visibility of one subset (`Forbidden` on the wire) — deliberately
 * distinct from empty membership. The ensure that requested it fails with this, writes
 * no coverage mark, and the next ensure asks again.
 */
export class SubsetForbidden extends Schema.TaggedError<SubsetForbidden>()("SubsetForbidden", {
  modelName: ModelName,
  indexKey: Schema.String,
  keyValue: Schema.String,
}) {}

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
    /**
     * `apply` may return subset keys whose coverage marks advance through this
     * signal's syncId — a partial drain returns every covered subset (an event that
     * didn't touch a subset still proves it current through that id). `void` ⇒ none;
     * existing subscribers compile unchanged.
     */
    readonly apply: (signal: SyncSignal) => Effect.Effect<ReadonlyArray<SubsetKey> | void>
  }) => Effect.Effect<void>

  /**
   * The idempotent ensure behind every `loadBy*` call: resolve when the subset's
   * local rows are current. The broker drives (plan → fetch → replay-read → mark),
   * the collection applies (callbacks). Tiers via `planMount` on the subset key:
   *
   * - **Skip** — the mark says nothing happened since the last load ⇒ resolve now.
   * - **Replay** — the journal still holds every event past the mark ⇒ `apply` the
   *   slice locally (the collection filters by its extractor), no network.
   * - **Snapshot** — rows can't be trusted (first load, pruned gap, resync) ⇒ one
   *   coalesced batch fetch: `Forbidden` ⇒ fail, no mark; `Members` + stamp S ⇒
   *   `replaceSlice(rows, S)` → journal slice since S → `apply` each → mark through
   *   max(S, replayed).
   */
  readonly ensureSubset: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
    readonly subset: SubsetKey
    /** Activate coverage at the stamp and land the slice in one synced transaction. */
    readonly replaceSlice: (rows: ReadonlyArray<unknown>, at: SyncId) => Effect.Effect<void>
    readonly apply: (signal: SyncSignal) => Effect.Effect<void>
  }) => Effect.Effect<void, SubsetForbidden | HydrateFailed>

  /** Durable subset coverage marks for one partial collection — coverage-map hydration. */
  readonly coveredSubsets: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
  }) => Effect.Effect<ReadonlyArray<{ readonly subset: SubsetKey; readonly at: SyncId }>>

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

    // Optional by design: apps without partial collections provide no HydrateClient
    // and nothing changes. A Snapshot-tier ensure with no client is a config defect.
    const hydrate = yield* Effect.serviceOption(HydrateClient)
    const batchLoader = Option.isSome(hydrate)
      ? Option.some(yield* makeBatchLoader({ client: hydrate.value }))
      : Option.none<BatchLoader>()

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
          Effect.flatMap((covered) =>
            Effect.forEach(
              covered ?? [],
              (subset) =>
                tracker.markApplied({
                  key: subsetKey({ entity: modelName, scope, subset }),
                  schemaVersion,
                  through: syncIdOf(signal),
                }),
              { discard: true },
            ),
          ),
          Effect.andThen(
            tracker.markApplied({ key: keyFor(modelName, scope), schemaVersion, through: syncIdOf(signal) }),
          ),
        ),
      )

    const ensureSubset: SyncBrokerShape["ensureSubset"] = ({
      modelName,
      scope,
      schemaVersion,
      subset,
      replaceSlice,
      apply,
    }) =>
      Effect.gen(function* () {
        const key = subsetKey({ entity: modelName, scope, subset })
        const plan = planMount({
          collectionLastApplied: yield* tracker.current({ key, schemaVersion }),
          lastIngested: yield* journal.getLastIngestedSyncId,
          highestPruned: yield* journal.highestPrunedSyncId(modelName),
          lastResyncAt: yield* journal.getLastResync,
        })

        // Replay a journal slice after `since` and advance the mark: an event ≤ the
        // fold seed is other models' — the subset is still proven current through it.
        const replayFrom = (since: SyncId, seed: SyncId): Effect.Effect<void> =>
          journal.read({ modelName, since }).pipe(
            Effect.flatMap((rows) =>
              Effect.forEach(rows, (row) => apply(signalFromRow(row)), { discard: true }).pipe(
                Effect.andThen(
                  tracker.markApplied({
                    key,
                    schemaVersion,
                    through: rows.reduce((guard, row) => maxSyncId(guard, row.syncId), seed),
                  }),
                ),
              ),
            ),
          )

        yield* MountDecision.$match(plan.decision, {
          Skip: () => Effect.void,
          Replay: () => replayFrom(plan.since, plan.tailGuardSeed),
          Snapshot: () =>
            Effect.gen(function* () {
              if (Option.isNone(batchLoader)) {
                return yield* Effect.die(
                  "[SyncBroker] a partial collection needs HydrateClient — add HydrateClient.layer({ url }) to the sync layer",
                )
              }
              const { result, lastSyncId, epoch } = yield* batchLoader.value.load({
                modelName,
                indexKey: subset.indexKey,
                keyValue: subset.keyValue,
              })
              // Epoch guard: ingest owns the reset — this ensure just refuses to mark
              // a dead-timeline coordinate; the next ensure lands on the new timeline.
              const stored = yield* journal.getEpoch
              if (Option.isSome(epoch) && Option.isSome(stored) && epoch.value !== stored.value) {
                return yield* new HydrateFailed({ reason: "epoch mismatch" })
              }
              if (result._tag === "Forbidden") {
                return yield* new SubsetForbidden({ modelName, indexKey: subset.indexKey, keyValue: subset.keyValue })
              }
              yield* replaceSlice(result.rows, lastSyncId)
              yield* replayFrom(lastSyncId, lastSyncId)
            }),
        })
      })

    const coveredSubsets: SyncBrokerShape["coveredSubsets"] = ({ modelName, scope, schemaVersion }) =>
      // Flush first so marks still batched in the tracker are visible — the read is
      // rare (one per partial-collection mount), the write cost negligible.
      tracker.flush.pipe(
        Effect.andThen(journal.subsetMarks({ entity: modelName, scope, schemaVersion })),
      )

    return { attachSubscriber, ensureSubset, coveredSubsets, start }
  })

export class SyncBroker extends Context.Service<SyncBroker, SyncBrokerShape>()("SyncBroker") {
  static readonly layer = (options?: SyncBrokerOptions): Layer.Layer<
    SyncBroker,
    never,
    SyncTransport | CatchupClient | SyncJournal
  > => Layer.effect(SyncBroker, make(options))
}
