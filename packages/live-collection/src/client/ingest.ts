import { Data, Effect, Option, Schedule, Stream } from "effect"
import { type CatchupResponse, type Epoch, type HydratedSyncEventEnvelope, type SyncId, zeroSyncId } from "@triargos/live-collection-protocol"
import type { CatchupClientShape } from "./catchup-client.js"
import type { SyncCursorShape } from "./sync-cursor.js"
import type { SyncJournalShape, JournalEvent } from "./sync-journal.js"
import type { SyncTransportShape } from "./sync-transport.js"

/**
 * What the ingest side publishes to subscriber tails. `Event`/`Resync` respect each
 * subscriber's monotonic tail guard; `EpochReset` bypasses it — after a server timeline
 * reset every mounted subscriber's guard is an old-epoch (large) syncId, so a guarded
 * item at the new-epoch (small) cursor would be silently dropped.
 */
export type PublishedItem = Data.TaggedEnum<{
  Event: { readonly row: JournalEvent }
  Resync: { readonly at: SyncId }
  EpochReset: { readonly at: SyncId }
}>
export const PublishedItem = Data.taggedEnum<PublishedItem>()

export interface RetentionOptions {
  readonly maxEventsPerModel: number
  readonly maxEventsTotal: number
  readonly trimEveryEvents: number
}

type EntityEvent = Exclude<HydratedSyncEventEnvelope, { readonly _tag: "Resync" }>

const rowFromEvent = (event: EntityEvent): JournalEvent =>
  event._tag === "Delete"
    ? { syncId: event.syncId, modelName: event.modelName, tag: "Delete", modelId: event.modelId, data: Option.none() }
    : {
        syncId: event.syncId,
        modelName: event.modelName,
        tag: event._tag,
        modelId: event.modelId,
        data: Option.some(event.data),
      }

/**
 * The INGEST machine — the single fiber that owns the network→journal path.
 *
 * One cycle = catchup (heal the gap since the cursor) then tail (SSE live events).
 * `SyncConnectionLost` retries the whole cycle every 3s, re-running catchup so the
 * disconnection gap is healed before tailing again. A failed catchup is non-fatal:
 * stale is better than dead.
 *
 * Ordering invariants owned here:
 * - Live tail, per event: `append` → publish → cursor. The cursor only advances after
 *   the row is durable, so a crash mid-ingest re-fetches rather than skips.
 * - Catchup, per batch: full `append` (one tx) → ordered publishes → one cursor set.
 * - Epoch reset: `onEpochReset` (drop pending last-applied marks) strictly before
 *   `journal.reset`; `cursorStore.clear` before `set` (monotonic set would keep the
 *   old-epoch cursor); `EpochReset` fanout last.
 */
export const makeIngest = (deps: {
  readonly transport: SyncTransportShape
  readonly catchup: CatchupClientShape
  readonly journal: SyncJournalShape
  readonly cursorStore: SyncCursorShape
  readonly publish: (item: PublishedItem) => Effect.Effect<void>
  readonly onEpochReset: Effect.Effect<void>
  /** Flush pending last-applied marks — run before each prune so stage-2 sees fresh marks. */
  readonly flushLastApplied: Effect.Effect<void>
  readonly retention: RetentionOptions
}): Effect.Effect<void> => {
  const { transport, catchup, journal, cursorStore, publish, onEpochReset, flushLastApplied, retention } = deps

  // Amortized retention: prune once per `trimEveryEvents` ingested events.
  // Single-fiber by construction (only the ingest fiber touches it).
  let ingestsSinceTrim = 0
  const trimIfNeeded = (ingested: number): Effect.Effect<void> => {
    ingestsSinceTrim += ingested
    if (ingestsSinceTrim < retention.trimEveryEvents) return Effect.void
    ingestsSinceTrim = 0
    // Flush first: prune's dead-weight stage reads the durable marks, and a pending mark
    // held back by the timer would make it retain rows every collection already applied.
    return flushLastApplied.pipe(
      Effect.andThen(
        journal.prune({ maxEventsPerModel: retention.maxEventsPerModel, maxEventsTotal: retention.maxEventsTotal }),
      ),
    )
  }

  const ingestEntity = (event: EntityEvent): Effect.Effect<void> => {
    const row = rowFromEvent(event)
    return journal.append([row]).pipe(
      Effect.andThen(publish(PublishedItem.Event({ row }))),
      Effect.andThen(cursorStore.set(event.syncId)),
      Effect.andThen(trimIfNeeded(1)),
      Effect.asVoid,
    )
  }

  const ingestLive = (event: HydratedSyncEventEnvelope): Effect.Effect<void> =>
    event._tag === "Resync"
      ? journal.setLastResync(event.syncId).pipe(
          Effect.andThen(cursorStore.set(event.syncId)),
          Effect.andThen(publish(PublishedItem.Resync({ at: event.syncId }))),
          Effect.asVoid,
        )
      : ingestEntity(event)

  const applyCatchup = (response: CatchupResponse): Effect.Effect<void> => {
    const resyncs = response.events.filter((event) => event._tag === "Resync")
    // Any resync in the batch ⇒ everyone snapshots anyway; journaling the entities would be wasted work.
    if (resyncs.length > 0) {
      return Effect.forEach(resyncs, (event) => journal.setLastResync(event.syncId), { discard: true }).pipe(
        Effect.andThen(cursorStore.set(response.lastSyncId)),
        Effect.andThen(publish(PublishedItem.Resync({ at: response.lastSyncId }))),
        Effect.asVoid,
      )
    }
    // One consistent chunk: all rows durable in one append, then fanout in order, then one cursor advance.
    const rows = response.events.filter((event): event is EntityEvent => event._tag !== "Resync").map(rowFromEvent)
    return journal.append(rows).pipe(
      Effect.andThen(Effect.forEach(rows, (row) => publish(PublishedItem.Event({ row })), { discard: true })),
      Effect.andThen(cursorStore.set(response.lastSyncId)),
      Effect.andThen(trimIfNeeded(rows.length)),
      Effect.asVoid,
    )
  }

  /**
   * The server's timeline changed identity — every locally remembered syncId is a
   * coordinate in a timeline that no longer exists. Wipe it all, adopt the new epoch,
   * and snapshot every subscriber at the new cursor.
   */
  const resetForEpoch = (epoch: Epoch, at: SyncId): Effect.Effect<void> =>
    onEpochReset.pipe(
      Effect.andThen(journal.reset),
      Effect.andThen(journal.setEpoch(epoch)),
      Effect.andThen(cursorStore.clear),
      Effect.andThen(cursorStore.set(at)),
      Effect.andThen(publish(PublishedItem.EpochReset({ at }))),
      Effect.asVoid,
    )

  // No epoch on the wire ⇒ no checking (the backend guarantees one everlasting timeline).
  // First epoch seen ⇒ adopt it. Same ⇒ proceed. Different ⇒ the timeline reset: heal.
  const applyEpochChecked = (response: CatchupResponse): Effect.Effect<void> =>
    Option.match(response.epoch, {
      onNone: () => applyCatchup(response),
      onSome: (epoch) =>
        journal.getEpoch.pipe(
          Effect.flatMap(
            Option.match({
              onNone: () => journal.setEpoch(epoch).pipe(Effect.andThen(applyCatchup(response))),
              onSome: (stored) =>
                stored === epoch ? applyCatchup(response) : resetForEpoch(epoch, response.lastSyncId),
            }),
          ),
        ),
    })

  const cycle = Effect.gen(function* () {
    const from = Option.getOrElse(yield* cursorStore.get, () => zeroSyncId)
    const response = yield* catchup.fetch({ from }).pipe(
      Effect.map(Option.some),
      Effect.catchTag("CatchupFailed", (error) =>
        Effect.logWarning(`[SyncBroker] catchup failed, tailing anyway: ${error.reason}`).pipe(
          Effect.as(Option.none()),
        ),
      ),
    )
    yield* Option.match(response, { onNone: () => Effect.void, onSome: applyEpochChecked })
    yield* Stream.runForEach(transport.connect, ingestLive)
  })

  return cycle.pipe(
    Effect.retry({
      while: (error) => error._tag === "SyncConnectionLost",
      schedule: Schedule.spaced("3 seconds"),
    }),
    Effect.catchTag("SyncConnectionLost", () => Effect.void),
  )
}
