import { Context, type Duration, Effect, Layer, Option, type Queue, Schema, Stream } from "effect"
import { HttpClient, HttpClientResponse } from "@effect/platform"
import { HydratedSyncEventEnvelope } from "@triargos/live-collection-protocol"

/**
 * The live connection dropped — it ended, errored, or fell silent past the keep-alive window. It is
 * **expected**, not exceptional: the orchestrator's retry catches it, re-runs catchup to heal the
 * disconnect gap, and reconnects (DEC-T4). The `reason` carries why, for logs.
 */
export class SyncConnectionLost extends Schema.TaggedError<SyncConnectionLost>()("SyncConnectionLost", {
  reason: Schema.String,
}) {}

/**
 * The one app-wide SSE connection to `GET /sync`, decoded. {@link SyncTransportShape.connect} hides
 * SSE line-framing, the keep-alive timeout, text/JSON decoding, and {@link HydratedSyncEventEnvelope}
 * decoding (a malformed line is skipped and logged, never fatal — a newer server may emit shapes this
 * client can't parse). It carries every arm including `Resync`; the orchestrator splits them. The
 * stream **fails** with {@link SyncConnectionLost} on drop rather than retrying internally, so each
 * reconnect re-runs catchup.
 */
export interface SyncTransportShape {
  readonly connect: Stream.Stream<HydratedSyncEventEnvelope, SyncConnectionLost>
}

const decodeEvent = Schema.decode(Schema.parseJson(HydratedSyncEventEnvelope))

const makeHttp = (config: {
  readonly url: string
  readonly keepAlive: Duration.DurationInput
}): Effect.Effect<SyncTransportShape, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient
    // Every line resets the keep-alive timer: SSE servers emit `:` comment pings, so a gap longer
    // than `keepAlive` means the connection is dead even though no error surfaced.
    const lines = HttpClientResponse.stream(client.get(config.url)).pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.filter((line) => line.length > 0),
      Stream.timeoutFail(() => new SyncConnectionLost({ reason: "keep-alive timeout" }), config.keepAlive),
    )
    const connect = lines.pipe(
      Stream.filter((line) => line.startsWith("data:")),
      Stream.map((line) => line.slice("data:".length).trim()),
      Stream.filter((payload) => payload.length > 0),
      Stream.mapEffect((payload) =>
        decodeEvent(payload).pipe(
          Effect.map(Option.some),
          Effect.catchAll((error) =>
            Effect.logWarning(`[SyncTransport] dropping undecodable event: ${error.message}`).pipe(
              Effect.as(Option.none<HydratedSyncEventEnvelope>()),
            ),
          ),
        ),
      ),
      Stream.filterMap((option) => option),
      Stream.mapError((error) =>
        error instanceof SyncConnectionLost ? error : new SyncConnectionLost({ reason: error.message }),
      ),
      // A server-closed stream is still a drop — surface it so the orchestrator reconnects.
      Stream.concat(Stream.fail(new SyncConnectionLost({ reason: "stream ended" }))),
    )
    return { connect }
  })

/** The seam: `yield* SyncTransport`. */
export class SyncTransport extends Context.Tag("SyncTransport")<SyncTransport, SyncTransportShape>() {
  /** Prod: `GET {url}` as an SSE stream over the platform `HttpClient`. */
  static readonly layer = (config: {
    readonly url: string
    readonly keepAlive: Duration.DurationInput
  }): Layer.Layer<SyncTransport, never, HttpClient.HttpClient> => Layer.effect(SyncTransport, makeHttp(config))

  /** Test: events drained from a queue; shutting the queue surfaces {@link SyncConnectionLost}. */
  static readonly layerMemory = (
    events: Queue.Dequeue<HydratedSyncEventEnvelope>,
  ): Layer.Layer<SyncTransport> =>
    Layer.succeed(SyncTransport, {
      connect: Stream.fromQueue(events).pipe(
        Stream.concat(Stream.fail(new SyncConnectionLost({ reason: "transport closed" }))),
      ),
    })
}
