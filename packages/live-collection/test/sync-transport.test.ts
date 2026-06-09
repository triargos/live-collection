import { Chunk, Effect, Layer, Queue, Stream } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { HttpClient, HttpClientResponse } from "@effect/platform"
import {
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncGroup,
  SyncId,
} from "@triargos/live-collection-protocol"
import { SyncConnectionLost, SyncTransport } from "../src/client/sync-transport.js"

/** The HTTP transport over a canned web `Response` — the SSE wire is the only fake. */
const httpTransport = (respond: () => Response): Layer.Layer<SyncTransport> =>
  SyncTransport.layer({ url: "http://test/sync", keepAlive: "5 seconds" }).pipe(
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request) => Effect.succeed(HttpClientResponse.fromWeb(request, respond()))),
      ),
    ),
  )

const env = (id: string): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId: SyncId.make("1"),
  modelName: ModelName.make("Webhook"),
  modelId: ModelId.make(id),
  syncGroups: [SyncGroup.make("organization:o1")],
  createdAt: new Date(0),
  data: { id, orgId: "o1" },
})

describe("SyncTransport", () => {
  it.effect("layerMemory surfaces enqueued events on connect, in order", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      yield* Queue.offerAll(queue, [env("a"), env("b")])
      const taken = yield* Effect.flatMap(SyncTransport, (transport) =>
        transport.connect.pipe(Stream.take(2), Stream.runCollect),
      ).pipe(Effect.provide(SyncTransport.layerMemory(queue)))
      assert.deepStrictEqual(
        Chunk.toReadonlyArray(taken).map((e) => ("modelId" in e ? e.modelId : undefined)),
        [ModelId.make("a"), ModelId.make("b")],
      )
    }))

  it.effect("an SSE event split across multiple data: lines decodes as ONE event (spec framing)", () =>
    Effect.gen(function* () {
      // One envelope split at a JSON token boundary across two `data:` lines (SSE joins them with
      // \n — legal JSON whitespace), then a normal single-line event. Blank lines dispatch.
      const first =
        `{"_tag":"Insert","syncId":"1","modelName":"Webhook","modelId":"a",` +
        `"syncGroups":["organization:o1"],"createdAt":"1970-01-01T00:00:00.000Z","data":{"id":"a"}}`
      const second = first.replace(`"modelId":"a"`, `"modelId":"b"`).replace(`"id":"a"`, `"id":"b"`)
      const splitAt = first.indexOf(`"syncGroups"`)
      const body = [
        `data: ${first.slice(0, splitAt)}`,
        `data: ${first.slice(splitAt)}`,
        ``,
        `data: ${second}`,
        ``,
        ``, // join("\n") ⇒ the wire ends "…\n\n": the second event's dispatching blank line
      ].join("\n")
      const taken = yield* Effect.flatMap(SyncTransport, (transport) =>
        transport.connect.pipe(Stream.take(2), Stream.runCollect),
      ).pipe(Effect.provide(httpTransport(() => new Response(body, { status: 200 }))))
      assert.deepStrictEqual(
        Chunk.toReadonlyArray(taken).map((e) => ("modelId" in e ? e.modelId : undefined)),
        [ModelId.make("a"), ModelId.make("b")],
      )
    }))

  it.effect("a non-2xx response fails as SyncConnectionLost carrying the status — not a silent stream end", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.flatMap(SyncTransport, (transport) => Stream.runDrain(transport.connect)).pipe(
        Effect.provide(httpTransport(() => new Response("unauthorized", { status: 401 }))),
        Effect.exit,
      )
      if (exit._tag === "Failure" && exit.cause._tag === "Fail") {
        assert.instanceOf(exit.cause.error, SyncConnectionLost)
        assert.include(exit.cause.error.reason, "401") // the status, not "stream ended"
      } else {
        assert.fail("expected a SyncConnectionLost failure")
      }
    }))

  it.effect("a closed connection fails with SyncConnectionLost (the reconnect signal)", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
      yield* Queue.shutdown(queue)
      const exit = yield* Effect.flatMap(SyncTransport, (transport) =>
        Stream.runDrain(transport.connect),
      ).pipe(Effect.provide(SyncTransport.layerMemory(queue)), Effect.exit)
      assert.isTrue(exit._tag === "Failure")
      if (exit._tag === "Failure" && exit.cause._tag === "Fail") {
        assert.instanceOf(exit.cause.error, SyncConnectionLost)
      } else {
        assert.fail("expected a SyncConnectionLost failure")
      }
    }))
})
