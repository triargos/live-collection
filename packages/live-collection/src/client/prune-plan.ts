import { Order } from "effect"
import { compareSyncId, type SyncId } from "@triargos/live-collection-protocol"
import type { LoggedEvent } from "./event-log-store.js"

/**
 * The outcome of pruning, computed purely so any storage adapter (in-memory, IndexedDB) shares one
 * retention policy: keep the newest `perModel` events of every model, then globally keep at most `total`.
 * `deletedHighWater` is the highest syncId deleted per model — the model's new **prune boundary** (events
 * above it stay complete), which the adapter merges (monotonically) into its `floor`.
 */
export interface PrunePlan {
  readonly keep: ReadonlyArray<LoggedEvent>
  readonly deletedHighWater: ReadonlyMap<string, SyncId>
}

const newestFirst = (a: LoggedEvent, b: LoggedEvent) => compareSyncId(b.syncId, a.syncId)

export const prunePlan = (args: {
  readonly rows: ReadonlyArray<LoggedEvent>
  readonly perModel: number
  readonly total: number
}): PrunePlan => {
  const byModel = new Map<string, Array<LoggedEvent>>()
  for (const r of args.rows) {
    const arr = byModel.get(r.modelName) ?? []
    arr.push(r)
    byModel.set(r.modelName, arr)
  }

  const deleted: Array<LoggedEvent> = []
  let keep: Array<LoggedEvent> = []
  for (const events of byModel.values()) {
    const desc = [...events].sort(newestFirst)
    keep.push(...desc.slice(0, args.perModel))
    deleted.push(...desc.slice(args.perModel))
  }

  // Global backstop: if the per-model survivors still exceed `total`, trim the oldest across all models.
  if (keep.length > args.total) {
    const oldestFirst = [...keep].sort((a, b) => compareSyncId(a.syncId, b.syncId))
    const overflow = keep.length - args.total
    deleted.push(...oldestFirst.slice(0, overflow))
    keep = oldestFirst.slice(overflow)
  }

  const deletedHighWater = new Map<string, SyncId>()
  for (const d of deleted) {
    const current = deletedHighWater.get(d.modelName)
    deletedHighWater.set(d.modelName, current ? Order.max(compareSyncId)(current, d.syncId) : d.syncId)
  }
  return { keep, deletedHighWater }
}
