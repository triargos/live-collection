import { Option, Order, Schema } from "effect"

/**
 * A sync cursor: an opaque, monotonically increasing position in the global event
 * log. Encoded as a canonical decimal string (no leading zeros, so string equality
 * matches numeric equality), which stays exact well beyond `Number.MAX_SAFE_INTEGER`
 * without bigints. Always order `SyncId`s with {@link compareSyncId} — never compare
 * them lexicographically or via `Number(...)`.
 */
export const SyncId = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^(0|[1-9][0-9]*)$/)), // "0", or a non-zero digit followed by any digits
  Schema.brand("SyncId")
)
export type SyncId = typeof SyncId.Type

/**
 * Orders `SyncId`s by numeric magnitude. Parses to `bigint` so it stays exact for
 * cursors beyond `Number.MAX_SAFE_INTEGER`. Cursors need not be contiguous; advance a
 * stored cursor with `Order.max(compareSyncId)(previous, next)`.
 *
 * @example
 * ```ts
 * import { Order } from "effect"
 * import { compareSyncId, SyncId } from "@triargos/live-collection-protocol"
 *
 * compareSyncId(SyncId.make("9"), SyncId.make("10")) // -1 — numeric, not lexicographic
 *
 * // Advance a stored cursor monotonically:
 * const next = Order.max(compareSyncId)(current, incoming)
 * ```
 */
export const compareSyncId: Order.Order<SyncId> = Order.mapInput(Order.BigInt, BigInt)

/** The cold-start cursor — "before every event": catchup from it returns the whole visible log. */
export const zeroSyncId: SyncId = SyncId.make("0")

/** The numerically larger of two `SyncId`s — the monotonic-advance step for cursors and marks. */
export const maxSyncId: (a: SyncId, b: SyncId) => SyncId = Order.max(compareSyncId)

/**
 * Advance a possibly-absent cursor monotonically: absent adopts `next`, present keeps
 * the numerically larger. Every durable syncId the client stores (global cursor,
 * last-applied marks, prune floors, lastResync) advances through this, so a late,
 * out-of-order event can never pull a cursor backwards.
 */
export const advanceSyncId = (current: Option.Option<SyncId>, next: SyncId): SyncId =>
  Option.match(current, { onNone: () => next, onSome: (c) => maxSyncId(c, next) })

/**
 * The identity of the server event log's **timeline** — the epoch `SyncId`s belong to.
 * A `syncId` alone is not a complete coordinate: it is only comparable to cursors from
 * the *same* epoch. As long as the log lives continuously the epoch never changes; when
 * the log's history is destroyed or replaced (memory-backend restart, table truncation,
 * backup restore, database migration) a new epoch begins and every remembered `SyncId`
 * from the old one is meaningless.
 *
 * Opaque and server-minted (a UUID at boot for memory backends; a stored-once value for
 * durable ones). It is **not** a software version — a redeploy over a durable log must
 * not change it, and a backup restore under unchanged code must.
 *
 * Optional at the wire (see `CatchupResponse.epoch`): backends whose log is durable for
 * the server's whole lifetime never need to send one.
 */
export const Epoch = Schema.NonEmptyString.pipe(Schema.brand("Epoch"))
export type Epoch = typeof Epoch.Type

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

/**
 * The injective map key for one entity, `(modelName, modelId)` as a single string.
 * NUL-joined so distinct pairs can't collide — both parts are arbitrary strings, so any
 * printable delimiter could appear inside them. Never parsed back.
 */
export const entityKey = (modelName: ModelName, modelId: ModelId): string =>
  `${modelName}\u0000${modelId}`
