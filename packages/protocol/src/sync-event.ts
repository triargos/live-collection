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

/** A producer's `Insert`, before persistence — no `syncId`/`createdAt` yet. */
export const PendingInsert = Schema.TaggedStruct("Insert", entityFields)
/** A producer's `Update`, before persistence — no `syncId`/`createdAt` yet. */
export const PendingUpdate = Schema.TaggedStruct("Update", entityFields)
/** A producer's `Delete`, before persistence — no `syncId`/`createdAt` yet. */
export const PendingDelete = Schema.TaggedStruct("Delete", entityFields)
/** A producer's `Resync`, before persistence — carries the {@link ResyncTarget} to reset. */
export const PendingResync = Schema.TaggedStruct("Resync", resyncFields)
/**
 * What a producer hands the backend's event log for persistence: an entity event
 * (`Insert`/`Update`/`Delete`) or a `Resync`, without the database-assigned
 * `syncId`/`createdAt`. The persisted form is {@link SyncEvent}.
 */
export const PendingSyncEvent = Schema.Union([
  PendingInsert,
  PendingUpdate,
  PendingDelete,
  PendingResync
])
export type PendingSyncEvent = typeof PendingSyncEvent.Type

// Database-assigned fields, present once an event is persisted.
const dbAssigned = { syncId: SyncId, createdAt: Schema.DateFromString } as const

/** A persisted `Insert` — reference-only (model + id), no entity data. */
export const InsertEvent = Schema.TaggedStruct("Insert", { ...entityFields, ...dbAssigned })
/** A persisted `Update` — reference-only (model + id), no entity data. */
export const UpdateEvent = Schema.TaggedStruct("Update", { ...entityFields, ...dbAssigned })
/** A persisted `Delete` — reference-only (model + id). */
export const DeleteEvent = Schema.TaggedStruct("Delete", { ...entityFields, ...dbAssigned })
/** A persisted `Resync` — carries the {@link ResyncTarget} to reset. */
export const ResyncEvent = Schema.TaggedStruct("Resync", { ...resyncFields, ...dbAssigned })
/**
 * A persisted event "at rest" in the backend's event log: reference-only (model name +
 * id, never entity data), ordered by `syncId`. This is what `squash` folds and what a
 * backend hydrates into a {@link HydratedSyncEvent} before delivering it.
 */
export const SyncEvent = Schema.Union([InsertEvent, UpdateEvent, DeleteEvent, ResyncEvent])
export type SyncEvent = typeof SyncEvent.Type

// Fields shared by every hydrated entity arm.
const hydratedBase = {
  syncId: SyncId,
  modelName: ModelName,
  modelId: ModelId,
  syncGroups: Schema.NonEmptyArray(SyncGroup),
  createdAt: Schema.DateFromString
} as const

/** A delivered `Insert` carrying the entity `data`, decoded against the given schema. */
export const HydratedInsert = <T, I, R>(entity: Schema.Codec<T, I, R, R>) =>
  Schema.TaggedStruct("Insert", { ...hydratedBase, data: entity })
/** A delivered `Update` carrying the entity `data`, decoded against the given schema. */
export const HydratedUpdate = <T, I, R>(entity: Schema.Codec<T, I, R, R>) =>
  Schema.TaggedStruct("Update", { ...hydratedBase, data: entity })
/** A delivered `Delete` — structurally has no `data` field, only the entity reference. */
export const HydratedDelete = Schema.TaggedStruct("Delete", hydratedBase)
/** A delivered `Resync` — carries the {@link ResyncTarget} to reset, no entity reference. */
export const HydratedResync = Schema.TaggedStruct("Resync", {
  syncId: SyncId,
  target: ResyncTarget,
  syncGroups: Schema.NonEmptyArray(SyncGroup),
  createdAt: Schema.DateFromString
})

/**
 * The full set of events a subscriber decodes for one entity type: all entity arms plus
 * resync. Decode the wire payload with it at the client boundary — never cast.
 *
 * @example
 * ```ts
 * const WebhookEvent = HydratedSyncEvent(Webhook)
 * const event = yield* Schema.decodeUnknownEffect(WebhookEvent)(payload)
 * // event._tag: "Insert" | "Update" | "Delete" | "Resync"
 * // event.data is a decoded Webhook on the Insert/Update arms
 * ```
 */
export const HydratedSyncEvent = <T, I, R>(entity: Schema.Codec<T, I, R, R>) =>
  Schema.Union([
    HydratedInsert(entity),
    HydratedUpdate(entity),
    HydratedDelete,
    HydratedResync
  ])

/** The entity arms for one model, once resync events have been separated out. */
export const HydratedEntityEvent = <T, I, R>(entity: Schema.Codec<T, I, R, R>) =>
  Schema.Union([HydratedInsert(entity), HydratedUpdate(entity), HydratedDelete])

/**
 * Decodes a sync event without yet knowing its entity type: it validates the common
 * envelope and leaves `data` as opaque JSON, to be decoded later against the matching
 * model schema. This is what crosses the multiplexed wire (the SSE stream, the catchup
 * response), where events of many models interleave.
 */
export const HydratedSyncEventEnvelope = HydratedSyncEvent(Schema.Unknown)
export type HydratedSyncEventEnvelope = typeof HydratedSyncEventEnvelope.Type
