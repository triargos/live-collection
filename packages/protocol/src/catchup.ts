import { Schema } from "effect"
import { SyncId } from "./ids.js"
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
 * @example
 * ```ts
 * // Backend handler sketch — the route, auth, and errors are yours:
 * const { from } = yield* Schema.decodeUnknown(CatchupRequest)(request.query)
 * const visible = yield* eventLog.since({ from, groups: yield* groupsFor({ userId }) })
 * const events = yield* hydrateAll(squash(visible))
 * return yield* Schema.encode(CatchupResponse)({ events, lastSyncId })
 * ```
 */
export const CatchupResponse = Schema.Struct({
  events: Schema.Array(HydratedSyncEventEnvelope),
  lastSyncId: SyncId
})
export type CatchupResponse = typeof CatchupResponse.Type
