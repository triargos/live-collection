import { Data, Option, Order } from "effect"
import { compareSyncId, type ModelId, type ModelName, SyncId } from "@triargos/live-collection-protocol"
import type { JournalEvent } from "./sync-journal.js"
import { PublishedItem } from "./ingest.js"

/** Replay + live tail as one stream. Snapshot means the subscriber's local base is untrusted. */
export type SyncSignal = Data.TaggedEnum<{
  Snapshot: { readonly at: SyncId }
  Upsert: { readonly syncId: SyncId; readonly modelId: ModelId; readonly data: unknown }
  Delete: { readonly syncId: SyncId; readonly modelId: ModelId }
}>
export const SyncSignal = Data.taggedEnum<SyncSignal>()

/**
 * The on-mount verdict. `Snapshot` carries the syncId the subscriber must snapshot at —
 * the point replay becomes trustworthy again (max of cursor and last resync).
 */
export type MountDecision = Data.TaggedEnum<{
  /** Local rows are complete through the cursor — nothing to replay. */
  Skip: {}
  /** The journal still holds every event the collection is missing — replay the slice. */
  Replay: {}
  /** The local rows are untrusted (no last-applied record, resync crossed it, or pruning ate the gap). */
  Snapshot: { readonly at: SyncId }
}>
export const MountDecision = Data.taggedEnum<MountDecision>()

/**
 * Everything `subscribe` needs to assemble one mount stream, computed purely:
 * the decision, where the journal replay slice starts, and the syncId that seeds
 * the live tail's monotonic guard (items at or below it are already covered by
 * the replay slice or the snapshot and must be dropped).
 */
export interface MountPlan {
  readonly decision: MountDecision
  /** `journal.read` start (exclusive). */
  readonly since: SyncId
  /** Seed for the tail guard, before folding in the replay rows actually read. */
  readonly tailGuardSeed: SyncId
}

const zero = SyncId.make("0")
export const maxSyncId: (a: SyncId, b: SyncId) => SyncId = Order.max(compareSyncId)

/**
 * Decide what a mounting collection must do, from journal metadata alone.
 *
 * - No last-applied record for `(key, schemaVersion)` ⇒ `Snapshot` (fresh install or schema bump).
 * - A resync newer than the last-applied ⇒ `Snapshot` (replay across a resync is invalid).
 * - Last-applied ≥ cursor ⇒ `Skip` (nothing missed).
 * - Pruning deleted events above the last-applied (`maxDeletedSyncId`) ⇒ `Snapshot` (the gap is gone).
 * - Otherwise ⇒ `Replay` from the last-applied.
 */
export const planMount = (input: {
  /** max(durable, pending) last-applied syncId for this `(key, schemaVersion)`. */
  readonly collectionLastApplied: Option.Option<SyncId>
  /** The client's global ingest cursor. */
  readonly cursor: Option.Option<SyncId>
  /** Highest syncId pruning ever deleted for this model — replay below it is impossible. */
  readonly maxDeletedSyncId: Option.Option<SyncId>
  /** The newest resync the client has ingested — replay across it is invalid. */
  readonly lastResyncAt: Option.Option<SyncId>
}): MountPlan => {
  const cursorAt = Option.getOrElse(input.cursor, () => zero)
  const snapshotPoint = Option.match(input.lastResyncAt, {
    onNone: () => cursorAt,
    onSome: (resyncAt) => maxSyncId(cursorAt, resyncAt),
  })
  const snapshot: MountPlan = {
    decision: MountDecision.Snapshot({ at: snapshotPoint }),
    since: snapshotPoint,
    tailGuardSeed: snapshotPoint,
  }
  if (Option.isNone(input.collectionLastApplied)) return snapshot
  const lastApplied = input.collectionLastApplied.value
  if (Option.exists(input.lastResyncAt, (at) => compareSyncId(at, lastApplied) > 0)) return snapshot
  const continuation = (decision: MountDecision): MountPlan => ({
    decision,
    since: lastApplied,
    tailGuardSeed: maxSyncId(snapshotPoint, lastApplied),
  })
  if (compareSyncId(lastApplied, cursorAt) >= 0) return continuation(MountDecision.Skip())
  if (Option.exists(input.maxDeletedSyncId, (deleted) => compareSyncId(deleted, lastApplied) > 0)) return snapshot
  return continuation(MountDecision.Replay())
}

/** Journal row → subscriber signal. Insert/Update collapse into one `Upsert`. */
export const signalFromRow = (row: JournalEvent): SyncSignal =>
  row.tag === "Delete"
    ? SyncSignal.Delete({ syncId: row.syncId, modelId: row.modelId })
    : SyncSignal.Upsert({
        syncId: row.syncId,
        modelId: row.modelId,
        data: Option.getOrElse(row.data, () => null),
      })

/**
 * Relevance (stateless): does this fanout item concern the given model's subscriber?
 * Foreign models' events don't; `Resync`/`EpochReset` concern everyone.
 */
export const concernsModel =
  (modelName: ModelName) =>
  (item: PublishedItem): boolean =>
    item._tag !== "Event" || item.row.modelName === modelName

/**
 * Staleness (stateful) — one `Stream.mapAccum` step. The accumulator is the highest
 * syncId already emitted; an item at or below it is stale (covered by the replay slice,
 * or a redelivery) and is dropped. `EpochReset` is the one exception: a server timeline
 * reset means "stale" itself is measured in dead coordinates, so it always emits a
 * `Snapshot` and rebases the accumulator to the new epoch.
 */
export const dropStale = (
  lastEmitted: SyncId,
  item: PublishedItem,
): readonly [SyncId, ReadonlyArray<SyncSignal>] =>
  PublishedItem.$match(item, {
    EpochReset: ({ at }): readonly [SyncId, ReadonlyArray<SyncSignal>] => [at, [SyncSignal.Snapshot({ at })]],
    Resync: ({ at }): readonly [SyncId, ReadonlyArray<SyncSignal>] =>
      compareSyncId(at, lastEmitted) <= 0 ? [lastEmitted, []] : [at, [SyncSignal.Snapshot({ at })]],
    Event: ({ row }): readonly [SyncId, ReadonlyArray<SyncSignal>] =>
      compareSyncId(row.syncId, lastEmitted) <= 0 ? [lastEmitted, []] : [row.syncId, [signalFromRow(row)]],
  })
