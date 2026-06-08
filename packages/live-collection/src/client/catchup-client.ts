import { Context, Effect, Layer, Schema } from "effect"
import { HttpClient } from "@effect/platform"
import { type CatchupRequest, CatchupResponse, SyncId } from "@triargos/live-collection-protocol"

/**
 * A catchup request failed — the response wasn't 2xx, the connection broke, or the body didn't
 * decode against {@link CatchupResponse}. It is a **modeled, recoverable** failure on purpose: the
 * orchestrator logs it and tails the live stream anyway (a transient catchup miss is healed on the
 * next reconnect), so the read path degrades gracefully instead of crashing.
 */
export class CatchupFailed extends Schema.TaggedError<CatchupFailed>()("CatchupFailed", {
  from: SyncId,
  reason: Schema.String,
}) {}

/**
 * Fetches the events a subscriber missed since a cursor — `GET /catchup?from=`. One-shot. The server
 * resolves the caller's groups from their permissions (protocol DEC-12), squashes, hydrates, and
 * returns `{ events, lastSyncId }`; each event's `data` stays opaque here, decoded per-model later at
 * the dispatch seam.
 */
export interface CatchupClientShape {
  readonly fetch: (request: CatchupRequest) => Effect.Effect<CatchupResponse, CatchupFailed>
}

/** Decodes the wire body against {@link CatchupResponse} at the boundary — never casts the shape. */
const makeHttp = (config: { readonly url: string }): Effect.Effect<CatchupClientShape, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk)
    return {
      fetch: (request) =>
        client.get(`${config.url}?from=${request.from}`).pipe(
          Effect.flatMap((response) => response.json),
          Effect.flatMap(Schema.decodeUnknown(CatchupResponse)),
          Effect.mapError((cause) => new CatchupFailed({ from: request.from, reason: cause.message })),
        ),
    }
  })

/** The seam: `yield* CatchupClient`. */
export class CatchupClient extends Context.Tag("CatchupClient")<CatchupClient, CatchupClientShape>() {
  /** Prod: `GET {url}?from=` over the platform `HttpClient`. */
  static readonly layer = (config: { readonly url: string }): Layer.Layer<CatchupClient, never, HttpClient.HttpClient> =>
    Layer.effect(CatchupClient, makeHttp(config))
  /** Test: always returns the canned response. */
  static readonly layerMemory = (response: CatchupResponse): Layer.Layer<CatchupClient> =>
    Layer.succeed(CatchupClient, { fetch: () => Effect.succeed(response) })
}
