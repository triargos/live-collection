import { Context, Duration, Effect, Layer, ManagedRuntime, Option, Queue, Schema } from "effect"
import { CatchupClient, SyncCursor, SyncTransport } from "@triargos/live-collection"
import {
  type CatchupResponse,
  compareSyncId,
  HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncGroup,
  SyncId,
} from "@triargos/live-collection-protocol"
import type { DebugBus } from "../debug/debug-bus.js"
import { Webhook } from "./schema.js"

/**
 * The app's server API as an Effect service — the thing a collection's `services` runtime discharges
 * (the app's `R`). The optimistic handlers `yield* WebhookApi`; in prod this would be an HTTP client.
 * `create`/`remove` can fail with {@link BackendRejected} when failure injection fires (the panel toggle),
 * which is what makes TanStack roll the optimistic write back.
 */
export class WebhookApi extends Context.Service<
  WebhookApi,
  {
    readonly create: (w: Webhook) => Effect.Effect<Webhook, BackendRejected>
    readonly remove: (id: ModelId) => Effect.Effect<void, BackendRejected>
    readonly list: (orgId: string) => Effect.Effect<ReadonlyArray<Webhook>>
  }
>()("WebhookApi") {}

/** The fake server refused a mutation (failure injection) — the modeled failure that triggers rollback. */
export class BackendRejected extends Schema.TaggedErrorClass<BackendRejected>()("BackendRejected", {
  operation: Schema.Literals(["create", "delete"]),
  id: Schema.String,
}) {}

/** The knobs the debug panel drives, plus read-only views of the shared "server" state. */
export interface BackendControls {
  readonly tabId: string
  /** Probability (0…1) that the next mutation is rejected and rolled back. */
  readonly getFailureRate: () => number
  readonly setFailureRate: (rate: number) => void
  /** Number of events in the shared (cross-tab) event log. */
  readonly serverLogSize: () => number
  /** The server's latest `syncId`. */
  readonly lastSyncId: () => string
  /** Broadcast a live `Resync(All)` to **other** tabs (they reload + re-catchup — DEC-T6). */
  readonly broadcastResync: () => void
  /**
   * Append an `Insert` for `orgId` to the shared log and echo it onto this tab's loop — the "a remote
   * client wrote into a scope you don't have mounted" signal. The loop **logs** it to the EventLog (so it
   * is replayable) even when no collection for that scope is mounted, which is what makes replay-on-mount
   * demonstrable: seed while unmounted, then mount and watch it heal from the local log with no `listFn`.
   */
  readonly seedRemote: (args: { readonly orgId: string; readonly url: string }) => void
  /** Wipe the shared event log (a fresh server). Does not touch any tab's local OPFS/cursor. */
  readonly resetServer: () => void
}

export interface SharedBackend {
  readonly services: ManagedRuntime.ManagedRuntime<WebhookApi, never>
  readonly sync: Layer.Layer<SyncTransport | CatchupClient | SyncCursor>
  readonly controls: BackendControls
  /** Close the BroadcastChannel (app teardown). */
  readonly dispose: () => void
}

// One sync group for the whole playground; one model.
const GROUP = SyncGroup.make("playground")
const MODEL = ModelName.make("Webhook")

// The shared "server": an append-only event log + a monotonic seq, both in localStorage (so every tab on
// the origin sees the same authority), and a BroadcastChannel that fans new events out to live tabs.
const LOG_KEY = "lc:playground:server-log"
const SEQ_KEY = "lc:playground:server-seq"
const BC_NAME = "lc:playground:sync"

// Boundary codecs — the localStorage log and the BroadcastChannel messages are the wire here, so they are
// decoded against the protocol envelope schema, never cast (CLAUDE.md: never cast the wire shape).
const LogSchema = Schema.Array(HydratedSyncEventEnvelope)
const decodeLog = Schema.decodeEffect(Schema.fromJsonString(LogSchema))
const encodeLog = Schema.encodeEffect(Schema.fromJsonString(LogSchema))
const decodeEnvelope = Schema.decodeEffect(Schema.fromJsonString(HydratedSyncEventEnvelope))
const encodeEnvelope = Schema.encodeEffect(Schema.fromJsonString(HydratedSyncEventEnvelope))

const insertEnvelope = (syncId: SyncId, w: Webhook): HydratedSyncEventEnvelope => ({
  _tag: "Insert",
  syncId,
  modelName: MODEL,
  modelId: ModelId.make(w.id),
  syncGroups: [GROUP],
  createdAt: new Date(),
  data: w,
})

const deleteEnvelope = (syncId: SyncId, id: ModelId): HydratedSyncEventEnvelope => ({
  _tag: "Delete",
  syncId,
  modelName: MODEL,
  modelId: id,
  syncGroups: [GROUP],
  createdAt: new Date(),
})

const resyncEnvelope = (syncId: SyncId): HydratedSyncEventEnvelope => ({
  _tag: "Resync",
  syncId,
  target: { _tag: "All" },
  syncGroups: [GROUP],
  createdAt: new Date(),
})

/**
 * A cross-tab fake backend with fake delays and optional failure injection — the single authority behind
 * every seam a real backend serves: the `WebhookApi` mutations the optimistic handlers call, plus the read
 * path (`/catchup`, the SSE tail, the `listFn` snapshot). A mutation appends to the shared log, mints a
 * `syncId`, echoes the event onto **this** tab's loop (self-echo, idempotent against the handler's
 * `writeSynced`), and broadcasts it to **other** tabs over a `BroadcastChannel`. Every step is tapped into
 * the {@link DebugBus} so the panel can show the traffic.
 */
export const makeSharedBackend = (config: {
  readonly bus: DebugBus
  readonly tabId: string
  readonly delays?: { readonly tail?: Duration.Input; readonly list?: Duration.Input }
}): SharedBackend => {
  const { bus, tabId } = config
  const tail = Duration.fromInputUnsafe(config.delays?.tail ?? Duration.millis(120))
  const listDelay = Duration.fromInputUnsafe(config.delays?.list ?? Duration.millis(80))

  const settings = { failureRate: 0 }
  const channel = new BroadcastChannel(BC_NAME)
  const queue = Effect.runSync(Queue.unbounded<HydratedSyncEventEnvelope>())

  // --- shared-log primitives (read-modify-write; demo-grade, racy only under simultaneous writes) ---
  const readLog: Effect.Effect<ReadonlyArray<HydratedSyncEventEnvelope>> = Effect.sync(
    () => localStorage.getItem(LOG_KEY) ?? "[]",
  ).pipe(Effect.flatMap(decodeLog), Effect.orDie)

  const writeLog = (log: ReadonlyArray<HydratedSyncEventEnvelope>): Effect.Effect<void> =>
    encodeLog(log).pipe(
      Effect.map((serialized) => localStorage.setItem(LOG_KEY, serialized)),
      Effect.orDie,
    )

  const rawSeq = (): number => Number(localStorage.getItem(SEQ_KEY) ?? "0")
  const nextSeq: Effect.Effect<number> = Effect.sync(() => {
    const next = rawSeq() + 1
    localStorage.setItem(SEQ_KEY, String(next))
    return next
  })

  // Append one event to the shared log, echo it onto this tab's loop, and fan it out to other tabs.
  const commit = (
    build: (syncId: SyncId) => HydratedSyncEventEnvelope,
  ): Effect.Effect<HydratedSyncEventEnvelope> =>
    Effect.gen(function* () {
      const seq = yield* nextSeq
      const env = build(SyncId.make(String(seq)))
      yield* writeLog([...(yield* readLog), env])
      yield* Queue.offer(queue, env) // self-echo: the originator's loop confirms its own write (idempotent)
      const wire = yield* encodeEnvelope(env).pipe(Effect.orDie)
      yield* Effect.sync(() => channel.postMessage(wire)) // cross-tab fan-out (BroadcastChannel ≠ self)
      return env
    })

  // Fold the log into current rows — the snapshot/resync source (`listFn`). The log is the only authority,
  // so deriving from it keeps cross-tab writes visible without a second shared structure.
  const deriveRows = (
    log: ReadonlyArray<HydratedSyncEventEnvelope>,
  ): Effect.Effect<ReadonlyMap<string, Webhook>> =>
    Effect.gen(function* () {
      const rows = new Map<string, Webhook>()
      for (const event of log) {
        if (event._tag === "Insert" || event._tag === "Update") {
          const w = yield* Schema.decodeUnknownEffect(Webhook)(event.data).pipe(Effect.orDie)
          rows.set(w.id, w)
        } else if (event._tag === "Delete") {
          rows.delete(event.modelId)
        }
      }
      return rows
    })

  const rejects = (): boolean => Math.random() < settings.failureRate

  const api: Context.Service.Shape<typeof WebhookApi> = {
    create: (w) =>
      Effect.gen(function* () {
        yield* bus.tap({ direction: "out", channel: "mutation", label: `create webhook → ${w.url}`, payload: w })
        yield* Effect.sleep(tail)
        if (rejects()) {
          yield* bus.tap({ direction: "error", channel: "mutation", label: "create rejected — rolling back", payload: w })
          return yield* Effect.fail(new BackendRejected({ operation: "create", id: w.id }))
        }
        const env = yield* commit((syncId) => insertEnvelope(syncId, w))
        yield* bus.tap({ direction: "echo", channel: "sync", label: `Insert #${env.syncId} accepted (self-echo)`, payload: w })
        return w
      }),
    remove: (id) =>
      Effect.gen(function* () {
        yield* bus.tap({ direction: "out", channel: "mutation", label: `delete webhook ${id.slice(0, 8)}` })
        yield* Effect.sleep(tail)
        if (rejects()) {
          yield* bus.tap({ direction: "error", channel: "mutation", label: "delete rejected — rolling back" })
          return yield* Effect.fail(new BackendRejected({ operation: "delete", id }))
        }
        const env = yield* commit((syncId) => deleteEnvelope(syncId, id))
        yield* bus.tap({ direction: "echo", channel: "sync", label: `Delete #${env.syncId} accepted (self-echo)` })
      }),
    list: (orgId) =>
      Effect.sleep(listDelay).pipe(
        Effect.andThen(readLog),
        Effect.flatMap(deriveRows),
        Effect.map((rows) => Array.from(rows.values()).filter((w) => w.orgId === orgId)),
        Effect.tap((rows) =>
          bus.tap({ direction: "in", channel: "listFn", label: `listFn(${orgId}) → ${rows.length} rows (snapshot source)` }),
        ),
      ),
  }

  const services = ManagedRuntime.make(Layer.succeed(WebhookApi, api))

  // /catchup?from= : every logged event newer than the cursor (cold "0" ⇒ the whole log).
  const catchup = Layer.succeed(CatchupClient, {
    fetch: ({ from }) =>
      Effect.sleep(tail).pipe(
        Effect.andThen(readLog),
        Effect.flatMap((log) => {
          // compareSyncId, never Number(): syncIds are exact beyond MAX_SAFE_INTEGER (protocol ids.ts).
          const events = log.filter((e) => compareSyncId(e.syncId, from) > 0)
          // No epoch: the shared localStorage log is durable across reloads/redeploys, so its
          // syncId timeline never resets — the epoch escape hatch is for backends that can't say that.
          const response: CatchupResponse = { events, lastSyncId: SyncId.make(String(rawSeq())), epoch: Option.none() }
          return bus
            .tap({ direction: "in", channel: "catchup", label: `catchup from #${from} → ${events.length} events`, payload: { from, count: events.length } })
            .pipe(Effect.as(response))
        }),
      ),
  })

  // Other tabs' broadcasts land here and feed this tab's SSE tail. Decode at the boundary; a bad message
  // is logged and dropped, never fatal.
  channel.onmessage = (event) => {
    const decoded = Effect.runSync(Effect.result(decodeEnvelope(String(event.data))))
    if (decoded._tag === "Failure") {
      bus.push({ direction: "error", channel: "broadcast", label: "dropped undecodable cross-tab message" })
      return
    }
    const env = decoded.success
    const id = env._tag === "Resync" ? "All" : env.modelId
    bus.push({ direction: "in", channel: "sync", label: `cross-tab ${env._tag} #${env.syncId} (${id.slice(0, 8)})`, payload: env })
    Effect.runSync(Queue.offer(queue, env))
  }

  const sync = Layer.mergeAll(SyncCursor.layerMemory, catchup, SyncTransport.layerMemory(queue))

  const controls: BackendControls = {
    tabId,
    getFailureRate: () => settings.failureRate,
    setFailureRate: (rate) => {
      settings.failureRate = Math.max(0, Math.min(1, rate))
    },
    serverLogSize: () => Effect.runSync(readLog).length,
    lastSyncId: () => String(rawSeq()),
    broadcastResync: () => {
      // No log append and no self-echo: a Resync is a live control signal to *other* tabs only.
      // It still CONSUMES a real seq — syncIds are unique-per-event (DEC-E12); reusing rawSeq()+1
      // would collide with the next real mutation's id.
      const env = resyncEnvelope(SyncId.make(String(Effect.runSync(nextSeq))))
      const wire = Effect.runSync(encodeEnvelope(env).pipe(Effect.orDie))
      channel.postMessage(wire)
      bus.push({ direction: "out", channel: "resync", label: "broadcast Resync(All) → other tabs reload" })
    },
    seedRemote: ({ orgId, url }) => {
      const w: Webhook = { id: crypto.randomUUID(), orgId, url }
      // `commit` is the same path a real mutation takes: append to the shared log, self-echo onto this
      // tab's loop (which logs it to the EventLog + advances the cursor), and fan out to other tabs.
      Effect.runSync(
        commit((syncId) => insertEnvelope(syncId, w)).pipe(
          Effect.flatMap((env) =>
            bus.tap({
              direction: "in",
              channel: "seed",
              label: `seeded Insert #${env.syncId} → ${orgId} (no collection mounted ⇒ logged, not applied)`,
              payload: w,
            }),
          ),
        ),
      )
    },
    resetServer: () => {
      localStorage.removeItem(LOG_KEY)
      localStorage.removeItem(SEQ_KEY)
      bus.push({ direction: "info", channel: "server", label: "shared event log cleared" })
    },
  }

  return {
    services,
    sync,
    controls,
    dispose: () => channel.close(),
  }
}
