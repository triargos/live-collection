import { Schema } from "effect"
import { ModelName } from "./ids.js"
import { SyncGroup } from "./sync-group.js"

/**
 * Tells subscribers to discard part of their local state and re-fetch it — used when
 * deltas alone can't express a change, such as a permission change or a bulk
 * correction. The structure encodes how much to reset, from narrowest to widest:
 *
 * - `Model(model)` — discard entities of the given model.
 * - `Group(group)` — discard entities in the given sync group.
 * - `All` — discard everything and re-fetch from scratch.
 *
 * Build one with the smart constructors, e.g. `ResyncGroup.make({ group })`.
 */
export const ResyncAll = Schema.TaggedStruct("All", {}) // reset everything
export const ResyncGroup = Schema.TaggedStruct("Group", { group: SyncGroup }) // reset one group
export const ResyncModel = Schema.TaggedStruct("Model", { model: ModelName }) // reset one model

export const ResyncTarget = Schema.Union(ResyncAll, ResyncGroup, ResyncModel)
export type ResyncTarget = typeof ResyncTarget.Type
