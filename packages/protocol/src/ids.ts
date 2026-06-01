import { Order, Schema } from "effect"

/**
 * A sync cursor: an opaque, monotonically increasing position in the global event
 * log. Encoded as a canonical decimal string (no leading zeros, so string equality
 * matches numeric equality), which stays exact well beyond `Number.MAX_SAFE_INTEGER`
 * without bigints. Always order `SyncId`s with {@link compareSyncId} — never compare
 * them lexicographically or via `Number(...)`.
 */
export const SyncId = Schema.String.pipe(
  Schema.pattern(/^(0|[1-9][0-9]*)$/), // "0", or a non-zero digit followed by any digits
  Schema.brand("SyncId")
)
export type SyncId = typeof SyncId.Type

/**
 * Orders `SyncId`s by numeric magnitude. Parses to `bigint` so it stays exact for
 * cursors beyond `Number.MAX_SAFE_INTEGER`. Cursors need not be contiguous; advance a
 * stored cursor with `Order.max(compareSyncId)(previous, next)`.
 */
export const compareSyncId: Order.Order<SyncId> = Order.mapInput(Order.bigint, BigInt)

/**
 * The name of a synced model, such as `"Webhook"`. A non-empty string on the wire;
 * each app narrows it to a closed union through its model registry (see
 * `narrowModelName`).
 */
export const ModelName = Schema.NonEmptyString.pipe(Schema.brand("ModelName"))
export type ModelName = typeof ModelName.Type

/** The id of a single entity within a model — any non-empty string, typically a UUID. */
export const ModelId = Schema.NonEmptyString.pipe(Schema.brand("ModelId"))
export type ModelId = typeof ModelId.Type

/** The authenticated user a sync session belongs to. */
export const UserId = Schema.NonEmptyString.pipe(Schema.brand("UserId"))
export type UserId = typeof UserId.Type
