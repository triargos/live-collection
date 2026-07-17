import {
  Context,
  Data,
  type Duration,
  Effect,
  Layer,
  Option,
  Order,
  PubSub,
  Ref,
  Schedule,
  type Scope,
  Stream,
} from "effect"
import {
  compareSyncId,
  type HydratedSyncEventEnvelope,
  type ModelId,
  type ModelName,
  SyncId,
} from "@triargos/live-collection-protocol"
import type { SchemaVersion } from "../persistence/schema-version.js"
import { type CollectionKey, globalKey, scopedKey, serializeKey } from "../registry/collection-key.js"
import { CatchupClient } from "./catchup-client.js"
import { EventLogStore, type LoggedEvent } from "./event-log-store.js"
import { LastSyncIdStore } from "./last-sync-id-store.js"
import { SyncTransport } from "./sync-transport.js"

/** Replay + live tail as one stream. Snapshot means the subscriber's local base is untrusted. */
export type SyncSignal = Data.TaggedEnum<{
  Snapshot: { readonly at: SyncId }
  Upsert: { readonly syncId: SyncId; readonly modelId: ModelId; readonly data: unknown }
  Delete: { readonly syncId: SyncId; readonly modelId: ModelId }
}>

export const SyncSignal = Data.taggedEnum<SyncSignal>()

export interface SyncBrokerShape {
  /**
   * Replay this collection's missing history, then continue with its live tail.
   * `schemaVersion` identifies the persisted base the subscriber hydrates from — the
   * base watermark is read under `(key, schemaVersion)`, so a schema change (which
   * dumps the persisted table) finds no watermark and decides `Snapshot`.
   */
  readonly subscribe: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
  }) => Stream.Stream<SyncSignal>

  /** Record that a subscriber has applied every relevant signal through this sync id. */
  readonly markApplied: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
    readonly through: SyncId
  }) => Effect.Effect<void>

  /** Run the single catchup and live-event ingest fiber. Fork exactly once. */
  readonly start: Effect.Effect<void>
}

export interface SyncBrokerOptions {
  readonly retention?: {
    readonly maxEventsPerModel: number
    readonly maxEventsTotal: number
    readonly trimEveryEvents: number
  }
  readonly watermarkFlushEvery?: Duration.Input
}

type PublishedItem =
  | { readonly _tag: "Event"; readonly row: LoggedEvent }
  | { readonly _tag: "Resync"; readonly at: SyncId }

type PendingWatermark = {
  readonly key: CollectionKey<unknown>
  readonly schemaVersion: SchemaVersion
  readonly at: SyncId
}

type MountDecision = "Skip" | "Replay" | "Snapshot"

const zero = SyncId.make("0")
const maxSyncId = Order.max(compareSyncId)

const decideOnMount = (input: {
  readonly baseWatermark: Option.Option<SyncId>
  readonly cursor: Option.Option<SyncId>
  readonly modelFloor: Option.Option<SyncId>
  readonly lastResyncAt: Option.Option<SyncId>
}): MountDecision => {
  if (Option.isNone(input.baseWatermark)) return "Snapshot"
  const base = input.baseWatermark.value
  if (Option.exists(input.lastResyncAt, (at) => compareSyncId(at, base) > 0)) return "Snapshot"
  const cursor = Option.getOrElse(input.cursor, () => zero)
  if (compareSyncId(base, cursor) >= 0) return "Skip"
  if (Option.exists(input.modelFloor, (floor) => compareSyncId(floor, base) > 0)) return "Snapshot"
  return "Replay"
}

const keyFor = (modelName: ModelName, scope: Option.Option<string>): CollectionKey<unknown> =>
  Option.match(scope, {
    onNone: () => globalKey(modelName),
    onSome: (value) => scopedKey({ entity: modelName, scope: value }),
  })

const signalFromRow = (row: LoggedEvent): SyncSignal =>
  row.tag === "Delete"
    ? SyncSignal.Delete({ syncId: row.syncId, modelId: row.modelId })
    : SyncSignal.Upsert({
        syncId: row.syncId,
        modelId: row.modelId,
        data: Option.getOrElse(row.data, () => null),
      })

const defaultOptions = {
  retention: { maxEventsPerModel: 1000, maxEventsTotal: 5000, trimEveryEvents: 100 },
  watermarkFlushEvery: "100 millis" as Duration.Input,
}

const make = (options: SyncBrokerOptions = {}): Effect.Effect<
  SyncBrokerShape,
  never,
  SyncTransport | CatchupClient | LastSyncIdStore | EventLogStore | Scope.Scope
> =>
  Effect.gen(function* () {
    const transport = yield* SyncTransport
    const catchup = yield* CatchupClient
    const cursorStore = yield* LastSyncIdStore
    const log = yield* EventLogStore
    const published = yield* PubSub.unbounded<PublishedItem>()
    const pending = yield* Ref.make(new Map<string, PendingWatermark>())
    const retention = options.retention ?? defaultOptions.retention
    const watermarkFlushEvery = options.watermarkFlushEvery ?? defaultOptions.watermarkFlushEvery
    let ingestsSinceTrim = 0

    const flushWatermarks = Effect.uninterruptible(
      Ref.modify(pending, (current) => [[...current.values()], new Map<string, PendingWatermark>()] as const).pipe(
        Effect.flatMap((watermarks) =>
          Effect.forEach(
            watermarks,
            ({ key, schemaVersion, at }) => log.setBaseWatermark({ key, schemaVersion, at }),
            { discard: true },
          ),
        ),
      ),
    )

    yield* Effect.addFinalizer(() => flushWatermarks)
    yield* Effect.sleep(watermarkFlushEvery).pipe(
      Effect.andThen(flushWatermarks),
      Effect.forever,
      Effect.forkScoped,
    )

    const markApplied: SyncBrokerShape["markApplied"] = ({ modelName, scope, schemaVersion, through }) => {
      const key = keyFor(modelName, scope)
      const id = `${serializeKey(key)}:${schemaVersion}`
      return Ref.update(pending, (current) => {
        const next = new Map(current)
        const existing = next.get(id)
        next.set(id, {
          key,
          schemaVersion,
          at: existing === undefined ? through : maxSyncId(existing.at, through),
        })
        return next
      })
    }

    const subscribe: SyncBrokerShape["subscribe"] = ({ modelName, scope, schemaVersion }) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const queue = yield* PubSub.subscribe(published)
          const key = keyFor(modelName, scope)
          const id = `${serializeKey(key)}:${schemaVersion}`
          const durableBase = yield* log.getBaseWatermark({ key, schemaVersion })
          const pendingBase = yield* Ref.get(pending).pipe(
            Effect.map((watermarks) => Option.fromNullishOr(watermarks.get(id)?.at)),
          )
          const baseWatermark = Option.match(pendingBase, {
            onNone: () => durableBase,
            onSome: (at) => Option.some(Option.match(durableBase, { onNone: () => at, onSome: (d) => maxSyncId(d, at) })),
          })
          const cursor = yield* cursorStore.get
          const modelFloor = yield* log.floor(modelName)
          const lastResyncAt = yield* log.getLastResync
          const cursorAt = Option.getOrElse(cursor, () => zero)
          const at = Option.match(lastResyncAt, {
            onNone: () => cursorAt,
            onSome: (resyncAt) => maxSyncId(cursorAt, resyncAt),
          })
          const decision = decideOnMount({ baseWatermark, cursor, modelFloor, lastResyncAt })
          const since = decision === "Snapshot" ? at : Option.getOrElse(baseWatermark, () => zero)
          const rows = yield* log.read({ modelName, since })
          const initialHead =
            decision === "Snapshot"
              ? at
              : Option.match(baseWatermark, { onNone: () => at, onSome: (base) => maxSyncId(at, base) })
          const head = rows.reduce((current, row) => maxSyncId(current, row.syncId), initialHead)
          const replay = [
            ...(decision === "Snapshot" ? [SyncSignal.Snapshot({ at })] : []),
            ...rows.map(signalFromRow),
          ]
          const tail = Stream.fromSubscription(queue).pipe(
            Stream.mapAccum(() => head, (lastEmitted, item) => {
              if (compareSyncId(item._tag === "Resync" ? item.at : item.row.syncId, lastEmitted) <= 0) {
                return [lastEmitted, []]
              }
              if (item._tag === "Resync") return [item.at, [SyncSignal.Snapshot({ at: item.at })]]
              if (item.row.modelName !== modelName) return [lastEmitted, []]
              return [item.row.syncId, [signalFromRow(item.row)]]
            }),
          )
          return Stream.concat(Stream.fromIterable(replay), tail)
        }),
      )

    const trimIfNeeded = (): Effect.Effect<void> => {
      ingestsSinceTrim += 1
      if (ingestsSinceTrim < retention.trimEveryEvents) return Effect.void
      ingestsSinceTrim = 0
      return log.prune({ perModel: retention.maxEventsPerModel, total: retention.maxEventsTotal })
    }

    const ingestEntity = (event: Exclude<HydratedSyncEventEnvelope, { readonly _tag: "Resync" }>) => {
      const row: LoggedEvent =
        event._tag === "Delete"
          ? {
              syncId: event.syncId,
              modelName: event.modelName,
              tag: "Delete",
              modelId: event.modelId,
              data: Option.none(),
            }
          : {
              syncId: event.syncId,
              modelName: event.modelName,
              tag: event._tag,
              modelId: event.modelId,
              data: Option.some(event.data),
            }
      return log.append([row]).pipe(
        Effect.andThen(PubSub.publish(published, { _tag: "Event", row })),
        Effect.andThen(cursorStore.set(event.syncId)),
        Effect.andThen(trimIfNeeded()),
        Effect.asVoid,
      )
    }

    const ingestLive = (event: HydratedSyncEventEnvelope): Effect.Effect<void> =>
      event._tag === "Resync"
        ? log.setLastResync(event.syncId).pipe(
            Effect.andThen(cursorStore.set(event.syncId)),
            Effect.andThen(PubSub.publish(published, { _tag: "Resync", at: event.syncId })),
            Effect.asVoid,
          )
        : ingestEntity(event)

    const applyCatchup = (response: {
      readonly events: ReadonlyArray<HydratedSyncEventEnvelope>
      readonly lastSyncId: SyncId
    }): Effect.Effect<void> => {
      const resyncs = response.events.filter((event) => event._tag === "Resync")
      if (resyncs.length > 0) {
        return Effect.forEach(resyncs, (event) => log.setLastResync(event.syncId), { discard: true }).pipe(
          Effect.andThen(cursorStore.set(response.lastSyncId)),
          Effect.andThen(PubSub.publish(published, { _tag: "Resync", at: response.lastSyncId })),
          Effect.asVoid,
        )
      }
      return Effect.forEach(response.events, (event) => (event._tag === "Resync" ? Effect.void : ingestEntity(event)), {
        discard: true,
      }).pipe(Effect.andThen(cursorStore.set(response.lastSyncId)))
    }

    const cycle = Effect.gen(function* () {
      const from = Option.getOrElse(yield* cursorStore.get, () => zero)
      const response = yield* catchup.fetch({ from }).pipe(
        Effect.map(Option.some),
        Effect.catchTag("CatchupFailed", (error) =>
          Effect.logWarning(`[SyncBroker] catchup failed, tailing anyway: ${error.reason}`).pipe(
            Effect.as(Option.none()),
          ),
        ),
      )
      yield* Option.match(response, { onNone: () => Effect.void, onSome: applyCatchup })
      yield* Stream.runForEach(transport.connect, ingestLive)
    })

    const start = cycle.pipe(
      Effect.retry({
        while: (error) => error._tag === "SyncConnectionLost",
        schedule: Schedule.spaced("3 seconds"),
      }),
      Effect.catchTag("SyncConnectionLost", () => Effect.void),
    )

    return { subscribe, markApplied, start }
  })

export class SyncBroker extends Context.Service<SyncBroker, SyncBrokerShape>()("SyncBroker") {
  static readonly layer = (options?: SyncBrokerOptions): Layer.Layer<
    SyncBroker,
    never,
    SyncTransport | CatchupClient | LastSyncIdStore | EventLogStore
  > => Layer.effect(SyncBroker, make(options))
}
