import { Schema } from "effect"
import { Epoch, ModelName, SyncId } from "./ids.js"

/**
 * Request and response schemas for partial-index loading — fetching one keyed subset
 * of a model (e.g. "all `SelectionTemplateValue` where `templateId = t1`") through the
 * backend's batch endpoint, without syncing the whole model.
 *
 * Like `catchup.ts`, these describe only what crosses the wire. The HTTP surface —
 * route, method, status codes, auth — belongs to the backend, which decodes
 * {@link HydrateBatchRequest} and encodes {@link HydrateBatchResponse} in its own
 * handler. The index vocabulary is closed and server-declared (the registry's
 * `indexes`): a request for an undeclared key is a malformed request (the kernel fails
 * the whole batch with `UnknownIndexError`), never an empty result.
 */

/** One subset address: "the `indexKey = keyValue` slice of `modelName`". */
export const IndexValue = Schema.Struct({
  modelName: ModelName,
  /** Must be declared in the server registry's `indexes` for this model. */
  indexKey: Schema.NonEmptyString,
  keyValue: Schema.NonEmptyString
})
export type IndexValue = typeof IndexValue.Type

/**
 * One batch of subset fetches — several `loadBy*` calls coalesced into a single
 * round trip. Never empty: a client with nothing to load sends nothing.
 */
export const HydrateBatchRequest = Schema.Struct({
  requests: Schema.NonEmptyArray(IndexValue)
})
export type HydrateBatchRequest = typeof HydrateBatchRequest.Type

/**
 * Per-request outcome. `Forbidden` is a visibility refusal — deliberately distinct
 * from `Members` with empty `rows`, which is valid, claimable membership ("this
 * subset exists and is empty"). Each row is the entity's wire form (the same shape a
 * snapshot or hydrated event carries), decoded later against the matching model schema.
 */
export const HydrateBatchResult = Schema.TaggedUnion({
  Members: { request: IndexValue, rows: Schema.Array(Schema.Unknown) },
  Forbidden: { request: IndexValue }
})
export type HydrateBatchResult = typeof HydrateBatchResult.Type

/**
 * The batch's results plus the coverage stamp. `lastSyncId` is the server log's head
 * read **before** the index queries ran — a safe floor: every row reflects at least
 * that position, so a client replaying its journal from `lastSyncId` re-applies at
 * worst a few already-reflected events (idempotent) and misses nothing.
 *
 * `epoch` has the same semantics as `CatchupResponse.epoch`: absent ⇒ the client does
 * no epoch checking.
 */
export const HydrateBatchResponse = Schema.Struct({
  results: Schema.Array(HydrateBatchResult),
  lastSyncId: SyncId,
  epoch: Schema.OptionFromOptionalKey(Epoch)
})
export type HydrateBatchResponse = typeof HydrateBatchResponse.Type
