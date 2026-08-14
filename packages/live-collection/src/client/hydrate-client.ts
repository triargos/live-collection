import { Context, Effect, Layer, Schema } from "effect"
import { HttpBody, HttpClient } from "effect/unstable/http"
import {
  HydrateBatchRequest,
  HydrateBatchResponse,
} from "@triargos/live-collection-protocol"

/**
 * A batch fetch failed — the response wasn't 2xx, the connection broke, or the body
 * didn't decode against {@link HydrateBatchResponse}. Modeled and recoverable: the
 * ensure that triggered the batch fails with it, writes no coverage mark, and the next
 * ensure simply fetches again.
 */
export class HydrateFailed extends Schema.TaggedError<HydrateFailed>()("HydrateFailed", {
  reason: Schema.String,
}) {}

/**
 * Fetches partial-index subsets in one round trip — `POST {url}` with a
 * {@link HydrateBatchRequest} body. One-shot; the broker's batch loader owns
 * coalescing and dedupe, this service owns only the wire.
 */
export interface HydrateClientShape {
  readonly fetch: (
    request: HydrateBatchRequest,
  ) => Effect.Effect<HydrateBatchResponse, HydrateFailed>
}

/** Encodes the request and decodes the wire body at the boundary — never casts either shape. */
const makeHttp = (config: {
  readonly url: string
}): Effect.Effect<HydrateClientShape, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk)
    return {
      fetch: (request) =>
        Schema.encodeEffect(HydrateBatchRequest)(request).pipe(
          Effect.flatMap((encoded) => client.post(config.url, { body: HttpBody.jsonUnsafe(encoded) })),
          Effect.flatMap((response) => response.json),
          Effect.flatMap(Schema.decodeUnknownEffect(HydrateBatchResponse)),
          Effect.mapError((cause) => new HydrateFailed({ reason: cause.message })),
        ),
    }
  })

/**
 * The batch-fetch service tag. Provide its layer as part of the `sync` layer handed to
 * `makeLiveRuntime` — required exactly when any collection is `partial`; apps without
 * partial collections omit it and nothing changes.
 *
 * @example
 * ```ts
 * HydrateClient.layer({ url: "/api/sync/batch" })
 * // requires an HttpClient, e.g.:  Layer.provide(FetchHttpClient.layer)
 * ```
 */
export class HydrateClient extends Context.Service<HydrateClient, HydrateClientShape>()(
  "HydrateClient",
) {
  /** HTTP default: `POST {url}` over the platform `HttpClient` (provide e.g. `FetchHttpClient.layer`). */
  static readonly layer = (config: {
    readonly url: string
  }): Layer.Layer<HydrateClient, never, HttpClient.HttpClient> =>
    Layer.effect(HydrateClient, makeHttp(config))
  /** In-memory — answers each batch with the handler; for tests. */
  static readonly layerMemory = (
    handler: (request: HydrateBatchRequest) => Effect.Effect<HydrateBatchResponse, HydrateFailed>,
  ): Layer.Layer<HydrateClient> => Layer.succeed(HydrateClient, { fetch: handler })
}
