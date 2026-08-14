import { DateTime, Duration, Effect, Fiber, Layer, Option, Queue, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  deriveGroup,
  HydrateBatchResult,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  ResyncTarget,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CatchupClient } from "../src/client/catchup-client.js"
import { HydrateClient } from "../src/client/hydrate-client.js"
import { SyncJournal } from "../src/client/sync-journal.js"
import { SyncTransport } from "../src/client/sync-transport.js"
import { defineCollection, loadByMethodName } from "../src/define-collection.js"
import { makeLiveRuntime, type SyncDeps } from "../src/runtime/live-runtime.js"
import { makeNodeSqlitePersistence } from "./sqlite-persistence.js"

const Value = Schema.Struct({ id: Schema.String, templateId: Schema.String, label: Schema.String })
type Value = typeof Value.Type
const modelName = ModelName.make("Value")
const group = deriveGroup(["organization", "org-1"])
const sid = (value: string) => SyncId.make(value)
const key = (value: string) => ModelId.make(value)
const epoch = DateTime.makeUnsafe(0).pipe(DateTime.toDateUtc)

const value = (id: string, templateId: string, label: string): Value => ({ id, templateId, label })

const upsert = (syncId: string, row: Value, tag: "Insert" | "Update" = "Insert"): HydratedSyncEventEnvelope => ({
  _tag: tag,
  syncId: sid(syncId),
  modelName,
  modelId: key(row.id),
  syncGroups: [group],
  createdAt: epoch,
  data: row,
})

const resync = (syncId: string): HydratedSyncEventEnvelope => ({
  _tag: "Resync",
  syncId: sid(syncId),
  target: ResyncTarget.cases.All.make({}),
  syncGroups: [group],
  createdAt: epoch,
})

const waitUntil = (condition: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() =>
    condition() ? Effect.void : Effect.sleep(Duration.millis(5)).pipe(Effect.andThen(waitUntil(condition))),
  ).pipe(
    Effect.timeoutOrElse({ duration: Duration.seconds(2), orElse: () => Effect.die("condition not met") }),
  )

/** A mutable fake server: the batch endpoint answers from `rows` at `stamp`, counting calls. */
interface FakeServer {
  rows: ReadonlyArray<Value>
  stamp: string
  calls: number
}

const withRuntime = <A>(
  initial: Pick<FakeServer, "rows" | "stamp">,
  use: (args: {
    readonly runtime: ReturnType<typeof makeLiveRuntime>
    readonly events: Queue.Queue<HydratedSyncEventEnvelope>
    readonly server: FakeServer
  }) => Effect.Effect<A>,
): Effect.Effect<A> =>
  Effect.gen(function* () {
    const server: FakeServer = { ...initial, calls: 0 }
    const events = yield* Queue.unbounded<HydratedSyncEventEnvelope>()
    const sync: Layer.Layer<SyncDeps> = Layer.mergeAll(
      CatchupClient.layerMemory({ events: [], lastSyncId: sid("0"), epoch: Option.none() }),
      SyncTransport.layerMemory(events),
      SyncJournal.layerMemory,
      HydrateClient.layerMemory((request) =>
        Effect.sync(() => {
          server.calls += 1
          return {
            results: request.requests.map((r) =>
              HydrateBatchResult.cases.Members.make({
                request: r,
                rows: server.rows.filter((row) => row.templateId === r.keyValue),
              }),
            ),
            lastSyncId: sid(server.stamp),
            epoch: Option.none(),
          }
        }),
      ),
    )
    const runtime = makeLiveRuntime({ persistence: makeNodeSqlitePersistence(), sync })
    const fiber = runtime.forkSync()
    return yield* use({ runtime, events, server }).pipe(
      Effect.ensuring(
        Fiber.interrupt(fiber).pipe(
          Effect.andThen(Effect.sync(() => runtime.dispose())),
          Effect.asVoid,
        ),
      ),
    )
  })

const makeValues = (runtime: ReturnType<typeof makeLiveRuntime>) =>
  defineCollection({
    runtime,
    entity: "Value",
    schema: Value,
    getKey: (v) => key(v.id),
    partial: { by: { templateId: (v: Value) => v.templateId } },
  })

// Never executed — compile-time surface checks only.
const _typeChecks = (runtime: ReturnType<typeof makeLiveRuntime>) => {
  // @ts-expect-error — a partial collection has no listFn: the batch endpoint is the snapshot path
  defineCollection({ runtime, entity: "Value", schema: Value, getKey: (v: Value) => key(v.id), partial: { by: { templateId: (v: Value) => v.templateId } }, listFn: Effect.succeed([]) })
  // @ts-expect-error — partial and scopeOf are mutually exclusive
  defineCollection({ runtime, entity: "Value", schema: Value, getKey: (v: Value) => key(v.id), partial: { by: { templateId: (v: Value) => v.templateId } }, scopeOf: (v: Value) => v.templateId })
}
void _typeChecks

describe("partial collection", () => {
  it("loadByMethodName is exactly TS's Capitalize", () => {
    assert.strictEqual(loadByMethodName("templateId"), "loadByTemplateId")
    assert.strictEqual(loadByMethodName("x"), "loadByX")
  })

  it.live("loadByTemplateId populates exactly the ensured subset — and later ensures skip the wire", () =>
    withRuntime(
      { rows: [value("v1", "t1", "A"), value("v2", "t1", "B"), value("x1", "t2", "C")], stamp: "5" },
      ({ runtime, events, server }) =>
        Effect.gen(function* () {
          const collection = makeValues(runtime)()
          // Type surface: the generated method name and its param type.
          const _check: (value: string) => Promise<void> = collection.utils.loadByTemplateId
          void _check

          yield* Effect.promise(async () => {
            await collection.preload()
            await collection.utils.loadByTemplateId("t1")
          })
          assert.isTrue(collection.has(key("v1")))
          assert.isTrue(collection.has(key("v2")))
          assert.isFalse(collection.has(key("x1"))) // t2 was never ensured
          assert.strictEqual(server.calls, 1)

          // Marks advance while nobody reads: an uncovered same-model event is dropped
          // but still proves every covered subset current through its syncId…
          yield* Queue.offer(events, upsert("6", value("x2", "t2", "D")))
          yield* waitUntil(() => server.calls === 1 && !collection.has(key("x2")))
          yield* Effect.sleep(Duration.millis(20)) // let the drain ack land
          // …so the next ensure resolves locally — never a fetch.
          yield* Effect.promise(() => collection.utils.loadByTemplateId("t1"))
          assert.strictEqual(server.calls, 1)
          assert.isFalse(collection.has(key("x2")))
        }),
    ))

  it.live("live events respect coverage: covered land, uncovered drop, membership moves insert/update/delete", () =>
    withRuntime(
      { rows: [value("v1", "t1", "A"), value("x1", "t2", "C")], stamp: "2" },
      ({ runtime, events }) =>
        Effect.gen(function* () {
          const collection = makeValues(runtime)()
          yield* Effect.promise(async () => {
            await collection.preload()
            await collection.utils.loadByTemplateId("t1")
            await collection.utils.loadByTemplateId("t2")
          })

          // covered → covered (t1 → t2): updated in place, stays present.
          yield* Queue.offer(events, upsert("3", value("v1", "t2", "moved"), "Update"))
          yield* waitUntil(() => collection.get(key("v1"))?.templateId === "t2")

          // uncovered insert (t9) is dropped; covered insert (t1) lands live.
          yield* Queue.offer(events, upsert("4", value("z1", "t9", "nope")))
          yield* Queue.offer(events, upsert("5", value("v3", "t1", "new")))
          yield* waitUntil(() => collection.has(key("v3")))
          assert.isFalse(collection.has(key("z1")))

          // covered → uncovered (t2 → t9): the stale local row is deleted.
          yield* Queue.offer(events, upsert("6", value("v1", "t9", "gone"), "Update"))
          yield* waitUntil(() => !collection.has(key("v1")))

          // uncovered → covered (t9 → t2): inserted live.
          yield* Queue.offer(events, upsert("7", value("v1", "t2", "back"), "Update"))
          yield* waitUntil(() => collection.has(key("v1")))
        }),
    ))

  it.live("a stale event at or below the subset's mark never overwrites the fresher slice", () =>
    withRuntime(
      { rows: [value("v1", "t1", "fresh")], stamp: "5" },
      ({ runtime, events }) =>
        Effect.gen(function* () {
          const collection = makeValues(runtime)()
          yield* Effect.promise(async () => {
            await collection.preload()
            await collection.utils.loadByTemplateId("t1")
          })
          assert.strictEqual(collection.get(key("v1"))?.label, "fresh")

          // syncId 3 ≤ mark 5: the slice already reflects a newer server state — drop.
          yield* Queue.offer(events, upsert("3", value("v1", "t1", "stale"), "Update"))
          // syncId 6 > mark: applies — also proves the stale event above was seen and skipped.
          yield* Queue.offer(events, upsert("6", value("v1", "t1", "newer"), "Update"))
          yield* waitUntil(() => collection.get(key("v1"))?.label === "newer")
          assert.notStrictEqual(collection.get(key("v1"))?.label, "stale")
        }),
    ))

  it.live("a resync wipes rows and coverage; the next ensure refetches and drops server-deleted rows", () =>
    withRuntime(
      { rows: [value("v1", "t1", "A"), value("v2", "t1", "B")], stamp: "2" },
      ({ runtime, events, server }) =>
        Effect.gen(function* () {
          const collection = makeValues(runtime)()
          yield* Effect.promise(async () => {
            await collection.preload()
            await collection.utils.loadByTemplateId("t1")
          })
          yield* waitUntil(() => collection.has(key("v2")))

          // Server declares the timeline broken past 9 — all marks (2) go stale.
          yield* Queue.offer(events, resync("9"))
          yield* waitUntil(() => !collection.has(key("v1")) && !collection.has(key("v2")))

          // Meanwhile v2 vanished server-side; the refetch's slice must not resurrect it.
          server.rows = [value("v1", "t1", "A2")]
          server.stamp = "9"
          yield* Effect.promise(() => collection.utils.loadByTemplateId("t1"))
          assert.strictEqual(collection.get(key("v1"))?.label, "A2")
          assert.isFalse(collection.has(key("v2")))
          assert.strictEqual(server.calls, 2)
        }),
    ))
})
