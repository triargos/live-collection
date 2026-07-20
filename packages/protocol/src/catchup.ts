import { Schema } from "effect"
import { Epoch, SyncId } from "./ids.js"
import { HydratedSyncEventEnvelope } from "./sync-event.js"

/**
 * Request and response schemas for catching up — fetching the events a subscriber
 * missed while disconnected.
 *
 * These describe only what crosses the wire. The HTTP surface around them — the
 * route, method, status codes, errors, and authentication — belongs to the backend,
 * which decodes {@link CatchupRequest} and encodes {@link CatchupResponse} in its own
 * handler.
 *
 * There is no group parameter: the server decides which groups the caller may see
 * from their permissions and returns everything visible since `from`.
 */

/** What a subscriber sends to catch up: the cursor to resume from. */
export const CatchupRequest = Schema.Struct({
  from: SyncId
})
export type CatchupRequest = typeof CatchupRequest.Type

/**
 * The events since the requested cursor, plus the new cursor to store. Each event's
 * `data` is opaque JSON here, decoded later against the matching model schema.
 *
 * `epoch` is the identity of the log timeline `lastSyncId` belongs to — **optional on
 * the wire** (absent key ⇒ `None` ⇒ the client does no epoch checking). The protocol's
 * invariant is that syncIds are durable and monotonic within one epoch; a backend whose
 * log can reset (memory store, truncation, restore) sends `epoch` so the client can
 * detect the mismatch and self-heal by wiping its local sync state and re-bootstrapping.
 * Backends with a genuinely durable log omit it and lose nothing.
 *
 * @example
 * ```ts
 * // Backend handler sketch — the route, auth, and errors are yours:
 * const { from } = yield* Schema.decodeUnknownEffect(CatchupRequest)(request.query)
 * const visible = yield* eventLog.since({ from, groups: yield* groupsFor({ userId }) })
 * const events = yield* hydrateAll(squash(visible))
 * return yield* Schema.encodeEffect(CatchupResponse)({ events, lastSyncId })
 * ```
 */
export const CatchupResponse = Schema.Struct({
  events: Schema.Array(HydratedSyncEventEnvelope),
  lastSyncId: SyncId,
  epoch: Schema.OptionFromOptionalKey(Epoch)
})
export type CatchupResponse = typeof CatchupResponse.Type
