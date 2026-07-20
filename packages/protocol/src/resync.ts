import { Schema } from "effect"
import { ModelName } from "./ids.js"
import { SyncGroup } from "./sync-group.js"

/**
 * Tells subscribers to discard part of their local state and re-fetch it — emitted by
 * a backend when deltas alone can't express a change, such as a permission change or a
 * bulk correction. The structure encodes how much to reset, from narrowest to widest:
 *
 * - `Model` — discard entities of the given model.
 * - `Group` — discard entities in the given sync group.
 * - `All` — discard everything and re-fetch from scratch.
 *
 * Build one through `cases`:
 *
 * @example
 * ```ts
 * const target: ResyncTarget =
 *   ResyncTarget.cases.Group.make({ group: deriveGroup(["organization", orgId]) })
 * ```
 */
export const ResyncTarget = Schema.TaggedUnion({
  All: {},
  Group: { group: SyncGroup },
  Model: { model: ModelName }
})
export type ResyncTarget = typeof ResyncTarget.Type
