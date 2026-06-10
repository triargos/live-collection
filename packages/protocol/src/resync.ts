import { Schema } from "effect"
import { ModelName } from "./ids.js"
import { SyncGroup } from "./sync-group.js"

/** Resync target: discard **everything** and re-fetch from scratch. `ResyncAll.make({})`. */
export const ResyncAll = Schema.TaggedStruct("All", {})
/** Resync target: discard the entities in one sync group. `ResyncGroup.make({ group })`. */
export const ResyncGroup = Schema.TaggedStruct("Group", { group: SyncGroup })
/** Resync target: discard the entities of one model. `ResyncModel.make({ model })`. */
export const ResyncModel = Schema.TaggedStruct("Model", { model: ModelName })

/**
 * Tells subscribers to discard part of their local state and re-fetch it — emitted by
 * a backend when deltas alone can't express a change, such as a permission change or a
 * bulk correction. The structure encodes how much to reset, from narrowest to widest:
 *
 * - {@link ResyncModel} — discard entities of the given model.
 * - {@link ResyncGroup} — discard entities in the given sync group.
 * - {@link ResyncAll} — discard everything and re-fetch from scratch.
 *
 * Build one with the smart constructors:
 *
 * @example
 * ```ts
 * const target: ResyncTarget = ResyncGroup.make({ group: deriveGroup(["organization", orgId]) })
 * ```
 */
export const ResyncTarget = Schema.Union(ResyncAll, ResyncGroup, ResyncModel)
export type ResyncTarget = typeof ResyncTarget.Type
