import { compareSyncId, entityKey, maxSyncId, type SyncId } from "@triargos/live-collection-protocol"
import type { JournalEvent } from "./sync-journal.js"

/**
 * The outcome of pruning, computed purely so any storage adapter (in-memory, IndexedDB)
 * shares one retention policy. Three stages, in order:
 *
 * 1. **Squash** — keep only the newest event per `(modelName, modelId)`. Client replay is
 *    binary (Upsert/Delete applied idempotently), so intermediate history converges to the
 *    same state from *every* possible last-applied position. Delete tombstones are always
 *    kept — never the protocol squasher's cancel-out: a collection whose last-applied sits
 *    mid-run saw the Insert and must still see the Delete.
 * 2. **Dead weight** — drop rows with `syncId ≤ minLastApplied[model]`. Application is
 *    sequential and gapless, so every collection with a record already applied them; a
 *    model with *no* record drops entirely (any mount decides Snapshot regardless).
 * 3. **Count caps** — keep the newest `maxEventsPerModel` events of every model, then
 *    globally keep at most `maxEventsTotal`.
 *
 * Stages 1–2 delete only history no replayer can ever need, so they never move the floor.
 * `maxDeletedSyncId` — the highest syncId deleted per model, which the adapter merges
 * (monotonically) into its `floor` — is computed from **stage-3 deletions only**.
 */
export interface PrunePlan {
  readonly keep: ReadonlyArray<JournalEvent>
  /** From stage 3 (count caps) only — stages 1–2 never move the floor. */
  readonly maxDeletedSyncId: ReadonlyMap<string, SyncId>
}

const newestFirst = (a: JournalEvent, b: JournalEvent) => compareSyncId(b.syncId, a.syncId)

export const prunePlan = (args: {
  readonly rows: ReadonlyArray<JournalEvent>
  /**
   * Per model: the minimum collection last-applied syncId across its collections'
   * records. A model absent here has no record at all — all its rows are dead weight
   * (any mount decides Snapshot regardless).
   */
  readonly minLastApplied: ReadonlyMap<string, SyncId>
  /** Stage-3 cap: keep at most this many (newest) events per model. */
  readonly maxEventsPerModel: number
  /** Stage-3 cap: keep at most this many (newest) events across all models. */
  readonly maxEventsTotal: number
}): PrunePlan => {
  // ── Stage 1: squash — newest event per (modelName, modelId); floor-neutral ──
  const newestPerEntity = new Map<string, JournalEvent>()
  for (const r of args.rows) {
    const key = entityKey(r.modelName, r.modelId)
    const current = newestPerEntity.get(key)
    if (current === undefined || compareSyncId(r.syncId, current.syncId) > 0) newestPerEntity.set(key, r)
  }

  // ── Stage 2: dead weight — rows at or below the model's minimum last-applied; floor-neutral ──
  // A model absent from `minLastApplied` has no collection record ⇒ all its rows drop.
  const live = [...newestPerEntity.values()].filter((r) => {
    const min = args.minLastApplied.get(r.modelName)
    return min !== undefined && compareSyncId(r.syncId, min) > 0
  })

  // ── Stage 3: count caps — the only stage that moves the floor ──
  const byModel = new Map<string, Array<JournalEvent>>()
  for (const r of live) {
    const arr = byModel.get(r.modelName) ?? []
    arr.push(r)
    byModel.set(r.modelName, arr)
  }

  const deleted: Array<JournalEvent> = []
  let keep: Array<JournalEvent> = []
  for (const events of byModel.values()) {
    const desc = [...events].sort(newestFirst)
    keep.push(...desc.slice(0, args.maxEventsPerModel))
    deleted.push(...desc.slice(args.maxEventsPerModel))
  }

  // Global backstop: if the per-model survivors still exceed `maxEventsTotal`, trim the oldest across all models.
  if (keep.length > args.maxEventsTotal) {
    const oldestFirst = [...keep].sort((a, b) => compareSyncId(a.syncId, b.syncId))
    const overflow = keep.length - args.maxEventsTotal
    deleted.push(...oldestFirst.slice(0, overflow))
    keep = oldestFirst.slice(overflow)
  }

  const maxDeletedSyncId = new Map<string, SyncId>()
  for (const d of deleted) {
    const current = maxDeletedSyncId.get(d.modelName)
    maxDeletedSyncId.set(d.modelName, current ? maxSyncId(current, d.syncId) : d.syncId)
  }
  return { keep, maxDeletedSyncId }
}
