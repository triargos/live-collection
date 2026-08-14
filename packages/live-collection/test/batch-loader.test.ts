import { Effect, Option, Ref } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  type HydrateBatchRequest,
  type HydrateBatchResponse,
  HydrateBatchResult,
  type IndexValue,
  ModelName,
  SyncId,
} from "@triargos/live-collection-protocol"
import { type BatchLoader, makeBatchLoader } from "../src/client/batch-loader.js"
import { HydrateFailed } from "../src/client/hydrate-client.js"

const value = (keyValue: string, indexKey = "templateId", modelName = "Value"): IndexValue => ({
  modelName: ModelName.make(modelName),
  indexKey,
  keyValue,
})

/** Answers every request with Members (rows = [keyValue]) except keyValue "secret" ⇒ Forbidden. */
const answering = (request: HydrateBatchRequest): HydrateBatchResponse => ({
  results: request.requests.map((r) =>
    r.keyValue === "secret"
      ? HydrateBatchResult.cases.Forbidden.make({ request: r })
      : HydrateBatchResult.cases.Members.make({ request: r, rows: [r.keyValue] }),
  ),
  lastSyncId: SyncId.make("42"),
  epoch: Option.none(),
})

const withLoader = <A, E>(
  handler: (request: HydrateBatchRequest) => Effect.Effect<HydrateBatchResponse, HydrateFailed>,
  use: (loader: BatchLoader) => Effect.Effect<A, E>,
) => Effect.scoped(Effect.flatMap(makeBatchLoader({ client: { fetch: handler } }), use))

describe("batch loader", () => {
  it.effect("coalesces every load in one microtask window into a single fetch", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      const results = yield* withLoader(
        (request) => Ref.update(calls, (n) => n + 1).pipe(Effect.as(answering(request))),
        (loader) =>
          Effect.all(
            [loader.load(value("t1")), loader.load(value("t2")), loader.load(value("t3", "channel", "Other"))],
            { concurrency: "unbounded" },
          ),
      )
      assert.strictEqual(yield* Ref.get(calls), 1)
      assert.deepStrictEqual(
        results.map((r) => (r.result._tag === "Members" ? [...r.result.rows] : "forbidden")),
        [["t1"], ["t2"], ["t3"]],
      )
      // The shared stamp reaches every caller.
      assert.deepStrictEqual(results.map((r) => r.lastSyncId), ["42", "42", "42"])
    }))

  it.effect("identical requests dedupe into one wire entry sharing one result", () =>
    Effect.gen(function* () {
      const seen = yield* Ref.make<ReadonlyArray<number>>([])
      const results = yield* withLoader(
        (request) => Ref.update(seen, (all) => [...all, request.requests.length]).pipe(Effect.as(answering(request))),
        (loader) =>
          Effect.all([loader.load(value("t1")), loader.load(value("t1")), loader.load(value("t1"))], {
            concurrency: "unbounded",
          }),
      )
      assert.deepStrictEqual(yield* Ref.get(seen), [1]) // one fetch, one wire entry
      for (const r of results) {
        assert.strictEqual(r.result._tag, "Members")
      }
    }))

  it.effect("a Forbidden result reaches only its own caller", () =>
    Effect.gen(function* () {
      const results = yield* withLoader(
        (request) => Effect.succeed(answering(request)),
        (loader) =>
          Effect.all(
            [Effect.exit(loader.load(value("secret"))), Effect.exit(loader.load(value("t1")))],
            { concurrency: "unbounded" },
          ),
      )
      const [secret, open] = results
      // Forbidden is a successful load carrying a Forbidden result — the ensure layers the failure.
      assert.isTrue(secret._tag === "Success" && secret.value.result._tag === "Forbidden")
      assert.isTrue(open._tag === "Success" && open.value.result._tag === "Members")
    }))

  it.effect("a failed fetch fails every caller in that window — and the next window fetches afresh", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0)
      const flaky = (request: HydrateBatchRequest) =>
        Ref.updateAndGet(calls, (n) => n + 1).pipe(
          Effect.flatMap((n) =>
            n === 1
              ? Effect.fail(new HydrateFailed({ reason: "boom" }))
              : Effect.succeed(answering(request)),
          ),
        )
      yield* withLoader(flaky, (loader) =>
        Effect.gen(function* () {
          const window1 = yield* Effect.all(
            [Effect.exit(loader.load(value("t1"))), Effect.exit(loader.load(value("t2")))],
            { concurrency: "unbounded" },
          )
          for (const outcome of window1) assert.strictEqual(outcome._tag, "Failure")
          const retry = yield* loader.load(value("t1"))
          assert.strictEqual(retry.result._tag, "Members")
        }),
      )
      assert.strictEqual(yield* Ref.get(calls), 2)
    }))
})
