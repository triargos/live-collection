import { Context, Effect, Layer, Schema } from "effect"
import { HttpClient } from "@effect/platform"
import { type CatchupRequest, CatchupResponse, SyncId } from "@triargos/live-collection-protocol"

/**
 * A catchup request failed — the response wasn't 2xx, the connection broke, or the body didn't
 * decode against {@link CatchupResponse}. It is a **modeled, recoverable** failure on purpose: the
 * sync loop logs it and tails the live stream anyway (a transient catchup miss is healed on the
 * next reconnect), so the read path degrades gracefully instead of crashing.
 */
export class CatchupFailed extends Schema.TaggedError<CatchupFailed>()("CatchupFailed", {
  from: SyncId,
  reason: Schema.String,
}) {}

/**
 * Fetches the events this client missed since a cursor — `GET /catchup?from=`. One-shot.
 * The server resolves the caller's visible groups from their permissions, squashes,
 * hydrates, and returns `{ events, lastSyncId }`; each event's `data` stays opaque here
 * and is decoded against the matching model schema later, at the dispatch seam.
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

/**
 * The catchup service tag. Provide one of its layers as part of the `loop` layer handed
 * to `makeLiveRuntime`:
 *
 * @example
 * ```ts
 * CatchupClient.layer({ url: "/api/catchup" })
 * // requires an HttpClient, e.g.:  Layer.provide(FetchHttpClient.layer)
 * ```
 */
export class CatchupClient extends Context.Tag("CatchupClient")<CatchupClient, CatchupClientShape>() {
  /** HTTP default: `GET {url}?from=` over the platform `HttpClient` (provide e.g. `FetchHttpClient.layer`). */
  static readonly layer = (config: { readonly url: string }): Layer.Layer<CatchupClient, never, HttpClient.HttpClient> =>
    Layer.effect(CatchupClient, makeHttp(config))
  /** In-memory — always returns the given canned response; for tests. */
  static readonly layerMemory = (response: CatchupResponse): Layer.Layer<CatchupClient> =>
    Layer.succeed(CatchupClient, { fetch: () => Effect.succeed(response) })
}
