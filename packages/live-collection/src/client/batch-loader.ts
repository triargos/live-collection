import { Deferred, Effect, Option, Queue, Ref, type Scope } from "effect"
import * as Arr from "effect/Array"
import type {
  Epoch,
  HydrateBatchResult,
  IndexValue,
  SyncId,
} from "@triargos/live-collection-protocol"
import type { HydrateFailed } from "./hydrate-client.js"
import type { HydrateClientShape } from "./hydrate-client.js"

/**
 * One caller's slice of a batch response: its own result plus the batch-wide stamp.
 * `lastSyncId` is the server head read before the index queries — the syncId the
 * caller's coverage mark starts from.
 */
export interface BatchResult {
  readonly result: HydrateBatchResult
  readonly lastSyncId: SyncId
  readonly epoch: Option.Option<Epoch>
}

/**
 * The BATCH machine — collects `IndexValue`s from all callers in one microtask
 * window, dedupes identical requests, issues ONE `HydrateClient.fetch`, and
 * distributes per-request results plus the shared stamp.
 *
 * The per-caller `Deferred` here is a batching latch — callers wait for the shared
 * round trip they joined — not a write-ack handshake: nothing downstream waits on a
 * subscriber applying anything.
 */
export interface BatchLoader {
  readonly load: (request: IndexValue) => Effect.Effect<BatchResult, HydrateFailed>
}

const requestKey = (request: IndexValue): string =>
  JSON.stringify([request.modelName, request.indexKey, request.keyValue])

type Waiter = {
  readonly request: IndexValue
  readonly deferred: Deferred.Deferred<BatchResult, HydrateFailed>
}

export const makeBatchLoader = (deps: {
  readonly client: HydrateClientShape
}): Effect.Effect<BatchLoader, never, Scope.Scope> =>
  Effect.gen(function* () {
    const pending = yield* Ref.make(new Map<string, Waiter>())
    const wake = yield* Queue.unbounded<void>()

    const flush = Effect.gen(function* () {
      const waiters = yield* Ref.getAndSet(pending, new Map<string, Waiter>())
      const requests = [...waiters.values()].map((w) => w.request)
      if (!Arr.isReadonlyArrayNonEmpty(requests)) return
      const outcome = yield* Effect.exit(deps.client.fetch({ requests }))
      if (outcome._tag === "Failure") {
        // One failed window fails every caller in it; the next window fetches afresh.
        yield* Effect.forEach(
          waiters.values(),
          (w) => Deferred.failCause(w.deferred, outcome.cause),
          { discard: true },
        )
        return
      }
      const response = outcome.value
      const byRequest = new Map(response.results.map((result) => [requestKey(result.request), result]))
      yield* Effect.forEach(
        waiters.entries(),
        ([key, w]) => {
          const result = byRequest.get(key)
          return result === undefined
            ? Deferred.die(w.deferred, `[BatchLoader] server response missing result for ${key}`)
            : Deferred.succeed(w.deferred, {
                result,
                lastSyncId: response.lastSyncId,
                epoch: response.epoch,
              })
        },
        { discard: true },
      )
    })

    // One background fiber owns every fetch: a caller's interruption can never kill a
    // round trip other callers are waiting on. `yieldNow` after the wake lets every
    // same-tick `load` join the window before it closes.
    yield* Queue.take(wake).pipe(
      Effect.andThen(Effect.yieldNow),
      Effect.andThen(flush),
      Effect.forever,
      Effect.forkScoped,
    )

    const load: BatchLoader["load"] = (request) =>
      Effect.gen(function* () {
        const key = requestKey(request)
        const existing = (yield* Ref.get(pending)).get(key)
        if (existing !== undefined) return yield* Deferred.await(existing.deferred)
        const deferred = yield* Deferred.make<BatchResult, HydrateFailed>()
        yield* Ref.update(pending, (current) => new Map(current).set(key, { request, deferred }))
        yield* Queue.offer(wake, undefined)
        return yield* Deferred.await(deferred)
      })

    return { load }
  })
