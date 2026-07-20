import { compareSyncId, entityKey } from "./ids.js"
import { ResyncTarget } from "./resync.js"
import { SyncEvent } from "./sync-event.js"

type ResyncEvent = Extract<SyncEvent, { readonly _tag: "Resync" }>
type EntityEvent = Exclude<SyncEvent, { readonly _tag: "Resync" }>
type EntityTag = EntityEvent["_tag"]

/**
 * Folds an entity's previous terminal tag (rows) with the next event's tag (columns).
 * `"Drop"` means the two cancel out and the entity leaves the output — an insert
 * followed by a delete the subscriber never observed. A previous tag of `(none)`
 * (first event for the entity) keeps the next tag as-is.
 *
 * Cells for transitions that can't occur in a well-formed stream (e.g. Insert after
 * Insert) keep the prior tag.
 */
const foldTag: Record<EntityTag, Record<EntityTag, EntityTag | "Drop">> = {
  Insert: { Insert: "Insert", Update: "Insert", Delete: "Drop" },
  Update: { Insert: "Update", Update: "Update", Delete: "Delete" },
  Delete: { Insert: "Update", Update: "Delete", Delete: "Delete" }
}

// Re-emit an entity event under a possibly different tag, carrying the latest run
// metadata (syncId, syncGroups, createdAt) so a stored cursor advances past every
// folded-away event.
const retag = (e: EntityEvent, tag: EntityTag): EntityEvent => {
  const fields = {
    modelName: e.modelName,
    modelId: e.modelId,
    syncGroups: e.syncGroups,
    syncId: e.syncId,
    createdAt: e.createdAt
  }
  return SyncEvent.cases[tag].make(fields)
}

/**
 * Collapses a `syncId`-ordered list of at-rest events into the smallest equivalent
 * list, so a catching-up subscriber receives one event per entity instead of its full
 * history. It works only from event references (model, id, tag, groups, syncId) and
 * never reads entity data.
 *
 * Within an entity, a run of changes folds to a single terminal event — an insert
 * then update becomes one insert, and an insert then delete cancels out. A resync
 * event drops the earlier events it supersedes: `All` everything before it, `Group`
 * the earlier events in that group, `Model` the earlier events of that model.
 *
 * The result stays `syncId`-ordered, and the fold is idempotent:
 * `squash(squash(events))` equals `squash(events)`.
 *
 * Backends call this in their catchup handler before hydrating, so the response carries
 * the minimal set of entities to fetch.
 *
 * @example
 * ```ts
 * // events for one entity: Insert #1 → Update #2 → Update #5
 * squash(events) // ⇒ one Insert carrying syncId #5
 *
 * // Insert #1 → Delete #3 (the subscriber never saw the entity)
 * squash(events) // ⇒ [] — the pair cancels out
 * ```
 */
export const squash = (
  events: ReadonlyArray<SyncEvent>
): ReadonlyArray<SyncEvent> => {
  // Latest folded event per entity; a dropped key falls out of the output naturally.
  const entities = new Map<string, EntityEvent>()
  const resyncs: Array<SyncEvent> = []

  // Squashing proper: fold this event's tag with the entity's previous terminal tag.
  const foldEntity = (event: EntityEvent) => {
    const key = entityKey(event.modelName, event.modelId)
    const prev = entities.get(key)
    const folded = prev === undefined ? event._tag : foldTag[prev._tag][event._tag]
    if (folded === "Drop") entities.delete(key)
    else entities.set(key, retag(event, folded))
  }

  // Resync supersession: a resync makes the earlier events in its target redundant,
  // because subscribers re-fetch that slice anyway. The resync itself is kept.
  const applyResync = (event: ResyncEvent) => {
    const dropWhere = (superseded: (e: EntityEvent) => boolean) => {
      for (const [key, e] of entities) {
        if (superseded(e)) entities.delete(key)
      }
    }
    ResyncTarget.match(event.target, {
      All: () => {
        entities.clear()
        resyncs.length = 0
      },
      Group: ({ group }) =>
        dropWhere((e) => e.syncGroups.includes(group)),
      Model: ({ model }) => dropWhere((e) => e.modelName === model)
    })
    resyncs.push(event)
  }

  for (const event of events) {
    if (event._tag === "Resync") applyResync(event)
    else foldEntity(event)
  }

  return [...entities.values(), ...resyncs].sort((a, b) =>
    compareSyncId(a.syncId, b.syncId)
  )
}
