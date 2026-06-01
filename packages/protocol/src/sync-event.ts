import { Schema } from "effect"
import { ModelId, ModelName, SyncId } from "./ids.js"
import { SyncGroup } from "./sync-group.js"
import { ResyncTarget } from "./resync.js"

/**
 * Sync events come in three forms that share one tag vocabulary —
 * `Insert` / `Update` / `Delete` / `Resync`:
 *
 * - {@link PendingSyncEvent} — what a producer constructs, before persistence.
 * - {@link SyncEvent} — a persisted event "at rest": reference-only, with no entity data.
 * - {@link HydratedSyncEvent} — what a subscriber receives, with the entity `data` attached.
 *
 * Data presence is structural rather than optional: `Insert`/`Update` carry `data`,
 * `Delete` carries none, and `Resync` carries a `target`.
 */

// Fields a producer provides for an entity event. `syncId` and `createdAt` are
// assigned on persistence, so they're absent here.
const entityFields = {
  modelName: ModelName,
  modelId: ModelId,
  syncGroups: Schema.NonEmptyArray(SyncGroup)
} as const

const resyncFields = {
  target: ResyncTarget,
  syncGroups: Schema.NonEmptyArray(SyncGroup) // the groups this resync is delivered to
} as const

// Pending — constructed by a producer; accepted as input before persistence.
export const PendingInsert = Schema.TaggedStruct("Insert", entityFields)
export const PendingUpdate = Schema.TaggedStruct("Update", entityFields)
export const PendingDelete = Schema.TaggedStruct("Delete", entityFields)
export const PendingResync = Schema.TaggedStruct("Resync", resyncFields)
export const PendingSyncEvent = Schema.Union(
  PendingInsert,
  PendingUpdate,
  PendingDelete,
  PendingResync
)
export type PendingSyncEvent = typeof PendingSyncEvent.Type

// At rest — persisted events. Reference-only: no entity data on any arm.
const dbAssigned = { syncId: SyncId, createdAt: Schema.Date } as const
export const InsertEvent = Schema.TaggedStruct("Insert", { ...entityFields, ...dbAssigned })
export const UpdateEvent = Schema.TaggedStruct("Update", { ...entityFields, ...dbAssigned })
export const DeleteEvent = Schema.TaggedStruct("Delete", { ...entityFields, ...dbAssigned })
export const ResyncEvent = Schema.TaggedStruct("Resync", { ...resyncFields, ...dbAssigned })
export const SyncEvent = Schema.Union(InsertEvent, UpdateEvent, DeleteEvent, ResyncEvent)
export type SyncEvent = typeof SyncEvent.Type

// Hydrated — delivered to subscribers. `Insert`/`Update` gain typed `data`;
// `Delete` carries none; `Resync` carries its target.
const hydratedBase = {
  syncId: SyncId,
  modelName: ModelName,
  modelId: ModelId,
  syncGroups: Schema.NonEmptyArray(SyncGroup),
  createdAt: Schema.Date
} as const

export const HydratedInsert = <T, I, R>(entity: Schema.Schema<T, I, R>) =>
  Schema.TaggedStruct("Insert", { ...hydratedBase, data: entity })
export const HydratedUpdate = <T, I, R>(entity: Schema.Schema<T, I, R>) =>
  Schema.TaggedStruct("Update", { ...hydratedBase, data: entity })
export const HydratedDelete = Schema.TaggedStruct("Delete", hydratedBase) // no data
export const HydratedResync = Schema.TaggedStruct("Resync", {
  syncId: SyncId,
  target: ResyncTarget,
  syncGroups: Schema.NonEmptyArray(SyncGroup),
  createdAt: Schema.Date
})

/** The full set of events a subscriber decodes for one entity type: all entity arms plus resync. */
export const HydratedSyncEvent = <T, I, R>(entity: Schema.Schema<T, I, R>) =>
  Schema.Union(
    HydratedInsert(entity),
    HydratedUpdate(entity),
    HydratedDelete,
    HydratedResync
  )

/** The entity arms for one model, once resync events have been separated out. */
export const HydratedEntityEvent = <T, I, R>(entity: Schema.Schema<T, I, R>) =>
  Schema.Union(HydratedInsert(entity), HydratedUpdate(entity), HydratedDelete)

/**
 * Decodes a sync event without yet knowing its entity type: it validates the common
 * envelope and leaves `data` as opaque JSON, to be decoded later against the
 * matching model schema.
 */
export const HydratedSyncEventEnvelope = HydratedSyncEvent(Schema.Unknown)
export type HydratedSyncEventEnvelope = typeof HydratedSyncEventEnvelope.Type
