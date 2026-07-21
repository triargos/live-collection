import { Cause, Effect, Layer, Option, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { HttpClient, HttpClientResponse } from "effect/unstable/http"
import { CatchupResponse, SyncId } from "@triargos/live-collection-protocol"
import { CatchupClient, CatchupFailed } from "../src/client/catchup-client.js"

const sid = (s: string) => SyncId.make(s)
const canned: CatchupResponse = { events: [], lastSyncId: sid("42"), epoch: Option.none() }

/** A real in-memory HttpClient adapter (DI, not a mock): each GET is answered by `respond(url)`,
 *  and every requested URL is recorded into `urls` so a test can assert what was fetched. */
const fakeHttp = (urls: Array<string>, respond: (url: URL) => Response): Layer.Layer<HttpClient.HttpClient> =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request, url) => {
      urls.push(url.toString())
      return Effect.succeed(HttpClientResponse.fromWeb(request, respond(url)))
    }),
  )

describe("CatchupClient", () => {
  it.effect("layerMemory returns the canned response", () =>
    Effect.gen(function* () {
      const client = yield* CatchupClient
      const resp = yield* client.fetch({ from: sid("0") })
      assert.strictEqual(resp.lastSyncId, sid("42"))
    }).pipe(Effect.provide(CatchupClient.layerMemory(canned))))

  it.effect("layer GETs /catchup?from=<cursor> and decodes the body at the boundary", () =>
    Effect.gen(function* () {
      const urls: Array<string> = []
      const body = yield* Schema.encodeEffect(Schema.fromJsonString(CatchupResponse))({
        events: [],
        lastSyncId: sid("99"),
        epoch: Option.none(),
      })
      const http = fakeHttp(urls, () => new Response(body, { status: 200 }))
      const resp = yield* Effect.provide(
        Effect.flatMap(CatchupClient, (c) => c.fetch({ from: sid("7") })),
        CatchupClient.layer({ url: "https://api.test/catchup" }).pipe(Layer.provide(http)),
      )
      assert.strictEqual(resp.lastSyncId, sid("99"))
      assert.include(urls[0], "from=7")
    }))

  it.effect("layer maps a non-2xx response to CatchupFailed", () =>
    Effect.gen(function* () {
      const http = fakeHttp([], () => new Response("nope", { status: 500 }))
      const exit = yield* Effect.provide(
        Effect.flatMap(CatchupClient, (c) => c.fetch({ from: sid("3") })),
        CatchupClient.layer({ url: "https://api.test/catchup" }).pipe(Layer.provide(http)),
      ).pipe(Effect.exit)
      assert.isTrue(exit._tag === "Failure")
      const error = exit._tag === "Failure" ? Cause.findErrorOption(exit.cause) : Option.none()
      if (Option.isSome(error)) {
        assert.instanceOf(error.value, CatchupFailed)
        assert.strictEqual(error.value.from, sid("3"))
      } else {
        assert.fail("expected a CatchupFailed failure")
      }
    }))
})
