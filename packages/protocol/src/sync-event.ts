import { Schema } from "effect"
import { ModelId, ModelName, SyncId } from "./ids.js"
import { SyncGroup } from "./sync-group.js"
import { ResyncTarget } from "./resync.js"
import { taggedUnion } from "./tagged-union.js"

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
 *
 * Each form is a union of tagged structs; construct arms through `cases`
 * (`SyncEvent.cases.Insert.make(...)`) and branch on `_tag`.
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

// Database-assigned fields, present once an event is persisted.
const dbAssigned = { syncId: SyncId, createdAt: Schema.DateFromString } as const

/**
 * What a producer hands the backend's event log for persistence: an entity event
 * (`Insert`/`Update`/`Delete`) or a `Resync`, without the database-assigned
 * `syncId`/`createdAt`. The persisted form is {@link SyncEvent}.
 */
export const PendingSyncEvent = taggedUnion(
  Schema.TaggedStruct("Insert", entityFields),
  Schema.TaggedStruct("Update", entityFields),
  Schema.TaggedStruct("Delete", entityFields),
  Schema.TaggedStruct("Resync", resyncFields)
)
export type PendingSyncEvent = typeof PendingSyncEvent.Type

/**
 * A persisted event "at rest" in the backend's event log: reference-only (model name +
 * id, never entity data), ordered by `syncId`. This is what `squash` folds and what a
 * backend hydrates into a {@link HydratedSyncEvent} before delivering it.
 */
const restDelete = Schema.TaggedStruct("Delete", { ...entityFields, ...dbAssigned })
const restResync = Schema.TaggedStruct("Resync", { ...resyncFields, ...dbAssigned })

export const SyncEvent = taggedUnion(
  Schema.TaggedStruct("Insert", { ...entityFields, ...dbAssigned }),
  Schema.TaggedStruct("Update", { ...entityFields, ...dbAssigned }),
  restDelete,
  restResync
)
export type SyncEvent = typeof SyncEvent.Type

/**
 * The full set of events a subscriber decodes for one entity type: `Insert`/`Update`
 * carry the entity `data` decoded against the given schema, `Delete` carries only the
 * entity reference, and `Resync` carries the {@link ResyncTarget} to reset. Decode the
 * wire payload with it at the client boundary — never cast.
 *
 * @example
 * ```ts
 * const WebhookEvent = HydratedSyncEvent(Webhook)
 * const event = yield* Schema.decodeUnknown(WebhookEvent)(payload)
 * // event._tag: "Insert" | "Update" | "Delete" | "Resync"
 * // event.data is a decoded Webhook on the Insert/Update arms
 * ```
 */
export const HydratedSyncEvent = <T, I, R>(entity: Schema.Schema<T, I, R>) =>
  taggedUnion(
    Schema.TaggedStruct("Insert", { ...entityFields, ...dbAssigned, data: entity }),
    Schema.TaggedStruct("Update", { ...entityFields, ...dbAssigned, data: entity }),
    restDelete,
    restResync
  )

/**
 * Decodes a sync event without yet knowing its entity type: it validates the common
 * envelope and leaves `data` as opaque JSON, to be decoded later against the matching
 * model schema. This is what crosses the multiplexed wire (the SSE stream, the catchup
 * response), where events of many models interleave.
 */
export const HydratedSyncEventEnvelope = HydratedSyncEvent(Schema.Unknown)
export type HydratedSyncEventEnvelope = typeof HydratedSyncEventEnvelope.Type
