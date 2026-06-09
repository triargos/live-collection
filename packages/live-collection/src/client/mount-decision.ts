import { Option } from "effect"
import { compareSyncId, SyncId } from "@triargos/live-collection-protocol"

/** What the loop does for a collection on mount, decided from its freshness metadata alone. */
export enum MountDecision {
  /** Base is already complete to the cursor — do nothing. */
  Skip = "skip",
  /** Base is behind, but the local log covers the gap — re-apply the logged events. */
  Replay = "replay",
  /** Local state can't be trusted to cover the gap — refetch current truth via `listFn`. */
  Bootstrap = "bootstrap",
}

/**
 * Decide how to heal a collection when it mounts, from syncId positions only — no timestamps, no
 * arithmetic, only `compareSyncId` magnitude checks. `bootstrap` (network `listFn`) is the safe
 * fallback whenever the local log can't be trusted to cover the gap.
 *
 * `modelFloor` is the model's **prune boundary**, not its oldest event: `None` means nothing has
 * been pruned (the log is complete from the start, replay is safe); `Some(f)` means events below
 * `f` were deleted, so replay is safe only when `f <= baseWatermark`.
 */
export const decideOnMount = (i: {
  readonly baseWatermark: Option.Option<SyncId>
  readonly cursor: Option.Option<SyncId>
  readonly modelFloor: Option.Option<SyncId>
  readonly lastResyncAt: Option.Option<SyncId>
}): MountDecision => {
  if (Option.isNone(i.baseWatermark)) return MountDecision.Bootstrap // no base ever ⇒ fetch one
  const base = i.baseWatermark.value
  const cursor = Option.getOrElse(i.cursor, () => SyncId.make("0"))
  if (compareSyncId(base, cursor) >= 0) return MountDecision.Skip // base already complete to the cursor

  // base is behind the cursor — replay only if it's both safe and possible.
  const resyncAfter = Option.exists(i.lastResyncAt, (r) => compareSyncId(r, base) > 0)
  if (resyncAfter) return MountDecision.Bootstrap // a resync since the base invalidated it (D9)
  const floorAbove = Option.match(i.modelFloor, {
    onNone: () => false, // nothing pruned ⇒ log complete from the start ⇒ gap is covered
    onSome: (floor) => compareSyncId(floor, base) > 0, // pruned past the base ⇒ gap not covered
  })
  if (floorAbove) return MountDecision.Bootstrap
  return MountDecision.Replay // the log fully covers (base, cursor]
}
