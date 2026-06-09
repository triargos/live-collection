import { Data, Effect, Option, Schedule, Schema, Stream } from "effect"
import {
  type CatchupResponse,
  compareSyncId,
  type HydratedSyncEventEnvelope,
  type ModelId,
  ModelName,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CollectionRegistry } from "../registry/collection-registry.js"
import { type CollectionKey, globalKey, scopedKey } from "../registry/collection-key.js"
import type { ModelMeta, SyncMap } from "../registry/define-collection.js"
import type { SyncWrite } from "../dispatch/sync-write.js"
import { LastSyncIdStore } from "./last-sync-id-store.js"
import { CatchupClient } from "./catchup-client.js"
import { SyncTransport } from "./sync-transport.js"
import { EventLogStore, type LoggedEvent } from "./event-log-store.js"
import { decideOnMount, MountDecision } from "./mount-decision.js"

/** An entity event (the `Resync` arm separated out) — what the loop applies to the local store. */
type EntityEvent = Exclude<HydratedSyncEventEnvelope, { readonly _tag: "Resync" }>

/** Minimal view of a mounted collection the loop writes through. */
type Writable = { readonly utils: SyncWrite<unknown>; readonly keys: () => Iterable<ModelId> }

/** The merged inbox: live SSE events and registry mount signals, drained on one fiber so they never interleave. */
type Inbox =
  | { readonly _tag: "Live"; readonly event: HydratedSyncEventEnvelope }
  | { readonly _tag: "Mount"; readonly key: CollectionKey<unknown> }

/** Internal control signal: a live resync was seen, so stop the tail (it must not be retried). */
class ResyncStop extends Data.TaggedError("ResyncStop")<{}> {}

/** Log-retention knobs: keep the newest `perModel` per model and `total` overall, pruning every `everyEvents` ingests. */
export interface SyncLoopOptions {
  readonly prune: { readonly perModel: number; readonly total: number; readonly everyEvents: number }
}

/** Generous defaults (tune against the browser proof + the backend's catchup retention). */
const defaultOptions: SyncLoopOptions = { prune: { perModel: 1000, total: 5000, everyEvents: 100 } }

/**
 * The read-path loop (DEC-R7) with the durable EventLog folded in. Behaviour is the locked transport
 * tier (DEC-T1…T9) plus replay-on-mount: every received event is **logged** (even when dropped for an
 * unmounted scope), so a collection that mounts after its events streamed past can converge by
 * **replaying the local log** instead of a network `listFn`.
 *
 * - Application (`applyWrite`/`applyDelete`) is **source-agnostic** — live, catchup, and replay all go
 *   through it; only the cursor/log *recording* is ingest-specific.
 * - On mount, `decideOnMount` picks skip / replay / bootstrap from syncId positions alone.
 * - A catchup `Resync` ⇒ snapshot every model + bump `lastResyncAt`; a live `Resync` ⇒ reload + stop.
 *
 * Runs forever — fork it (`useLiveSync`). Error channel is `never`: drops retry, `ResyncStop` is caught.
 */
export const syncLoop = (
  map: SyncMap,
  onResync: Effect.Effect<void>,
  options: SyncLoopOptions = defaultOptions,
): Effect.Effect<
  void,
  never,
  CollectionRegistry | SyncTransport | CatchupClient | LastSyncIdStore | EventLogStore
> =>
  Effect.gen(function* () {
    const registry = yield* CollectionRegistry
    const store = yield* LastSyncIdStore
    const catchup = yield* CatchupClient
    const transport = yield* SyncTransport
    const log = yield* EventLogStore

    let ingestsSincePrune = 0 // single-fibered (merged inbox), so a plain counter is safe

    // entity → wire model name, so a mount signal (keyed by entity) can read the model's log slice.
    const modelOfEntity = new Map<string, ModelName>()
    for (const [name, entry] of Object.entries(map)) modelOfEntity.set(entry._meta.entity, ModelName.make(name))

    // ── source-agnostic application (the dispatcher): apply ONE decoded event to the mounted store ──
    const applyWrite = (meta: ModelMeta<any>, data: any): Effect.Effect<void> =>
      Effect.gen(function* () {
        const key = Option.match(meta.scopeOf, {
          onNone: () => globalKey<Writable>(meta.entity),
          onSome: (scopeOf) => scopedKey<Writable>({ entity: meta.entity, scope: scopeOf(data) }),
        })
        const found = yield* registry.getById(key)
        return yield* Option.match(found, {
          onNone: () => Effect.void, // not mounted ⇒ ignore (the event is still in the log for later replay)
          onSome: (collection) => collection.utils.writeSynced(data),
        })
      })

    const applyDelete = (meta: ModelMeta<any>, modelId: ModelId): Effect.Effect<void> =>
      registry
        .getByEntity<Writable>(meta.entity)
        .pipe(
          Effect.flatMap((mounted) =>
            Effect.forEach(mounted, ({ collection }) => collection.utils.deleteSynced(modelId), { discard: true }),
          ),
        )

    // ── ingest: record a NEW event (append to the log + advance the cursor) and apply it ──
    const ingest = (event: EntityEvent): Effect.Effect<void> =>
      Effect.gen(function* () {
        const entry = map[event.modelName]
        if (entry === undefined) return // unknown model ⇒ skip both log and apply (a newer server may emit more)
        const meta = entry._meta
        if (event._tag === "Delete") {
          const row: LoggedEvent = {
            syncId: event.syncId,
            modelName: event.modelName,
            scope: Option.none(),
            tag: "Delete",
            modelId: event.modelId,
            data: Option.none(),
          }
          yield* log.append([row])
          yield* applyDelete(meta, event.modelId)
        } else {
          const data = yield* Schema.decodeUnknown(meta.schema)(event.data).pipe(Effect.orDie)
          const row: LoggedEvent = {
            syncId: event.syncId,
            modelName: event.modelName,
            scope: Option.map(meta.scopeOf, (scopeOf) => scopeOf(data)),
            tag: event._tag,
            modelId: meta.getKey(data),
            data: Option.some(event.data),
          }
          yield* log.append([row])
          yield* applyWrite(meta, data)
        }
        yield* store.set(event.syncId)
        ingestsSincePrune += 1
        if (ingestsSincePrune >= options.prune.everyEvents) {
          ingestsSincePrune = 0
          yield* log.prune({ perModel: options.prune.perModel, total: options.prune.total })
        }
      })

    // ── replay: apply a logged row through the SAME application path (decode at the boundary) ──
    const replayRow = (meta: ModelMeta<any>, row: LoggedEvent): Effect.Effect<void> =>
      row.tag === "Delete"
        ? applyDelete(meta, row.modelId)
        : Schema.decodeUnknown(meta.schema)(Option.getOrElse(row.data, () => null)).pipe(
            Effect.orDie,
            Effect.flatMap((data) => applyWrite(meta, data)),
          )

    // Replace one mounted instance's contents with the server's current rows (DEC-T9 reconcile).
    const snapshotInstance = (
      meta: ModelMeta<any>,
      scope: Option.Option<string>,
      collection: Writable,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const rows = yield* meta.listFn(scope)
        const fetched = new Set(rows.map(meta.getKey))
        const absent = Array.from(collection.keys()).filter((key) => !fetched.has(key))
        yield* Effect.forEach(rows, (row) => collection.utils.writeSynced(row), { discard: true })
        yield* Effect.forEach(absent, (key) => collection.utils.deleteSynced(key), { discard: true })
      })

    // Snapshot every mounted instance of every model in the map.
    const snapshotAll: Effect.Effect<void> = Effect.forEach(
      Object.values(map),
      (entry) =>
        registry
          .getByEntity<Writable>(entry._meta.entity)
          .pipe(
            Effect.flatMap((mounted) =>
              Effect.forEach(mounted, ({ key, collection }) => snapshotInstance(entry._meta, key.scope, collection), {
                discard: true,
              }),
            ),
          ),
      { discard: true },
    )

    // Mark every currently-mounted instance complete to `at` — only sound when every one of them was
    // just healed to current truth (the catchup-Resync branch, right after `snapshotAll`).
    const markAllMountedCaughtUp = (at: SyncId): Effect.Effect<void> =>
      Effect.forEach(
        Object.values(map),
        (entry) =>
          registry
            .getByEntity(entry._meta.entity)
            .pipe(
              Effect.flatMap((mounted) =>
                Effect.forEach(mounted, ({ key }) => log.setBaseWatermark({ key, at }), { discard: true }),
              ),
            ),
        { discard: true },
      )

    // Mark the instances that RODE this delta catchup complete to `at`. An instance rode it iff its
    // base was already complete to the catchup's `from` — or it has no base but the catchup covered
    // everything (`from = "0"` delivers the full visible state, by cursor-completeness). An instance
    // mounted mid-flight with a gap below `from` keeps its watermark and heals in its own `onMount`
    // (skip/replay/bootstrap) — stamping it here would silently skip that heal: a scope deep-linked on
    // a warm-cursor start would render only the delta window, durably (DEC-E11 amendment).
    const markCatchupRiders = (args: { readonly from: SyncId; readonly at: SyncId }): Effect.Effect<void> =>
      Effect.forEach(
        Object.values(map),
        (entry) =>
          registry.getByEntity(entry._meta.entity).pipe(
            Effect.flatMap((mounted) =>
              Effect.forEach(
                mounted,
                ({ key }) =>
                  log.getBaseWatermark(key).pipe(
                    Effect.flatMap((watermark) => {
                      const rode = Option.match(watermark, {
                        onNone: () => compareSyncId(args.from, SyncId.make("0")) === 0,
                        onSome: (base) => compareSyncId(base, args.from) >= 0,
                      })
                      return rode ? log.setBaseWatermark({ key, at: args.at }) : Effect.void
                    }),
                  ),
                { discard: true },
              ),
            ),
          ),
        { discard: true },
      )

    const applyCatchup = (args: { readonly response: CatchupResponse; readonly from: SyncId }): Effect.Effect<void> =>
      Effect.gen(function* () {
        const { response, from } = args
        if (response.events.some((event) => event._tag === "Resync")) {
          yield* Effect.forEach(
            response.events.filter((event) => event._tag === "Resync"),
            (event) => log.setLastResync(event.syncId),
            { discard: true },
          )
          yield* snapshotAll
          yield* store.set(response.lastSyncId)
          yield* markAllMountedCaughtUp(response.lastSyncId)
        } else {
          yield* Effect.forEach(response.events, (event) => (event._tag === "Resync" ? Effect.void : ingest(event)), {
            discard: true,
          })
          yield* store.set(response.lastSyncId)
          yield* markCatchupRiders({ from, at: response.lastSyncId })
        }
      })

    // ── heal a freshly-mounted collection from its freshness metadata ──
    const onMount = (key: CollectionKey<unknown>): Effect.Effect<void> =>
      Effect.gen(function* () {
        const modelName = modelOfEntity.get(key.entity)
        if (modelName === undefined) return
        const entry = map[modelName]
        if (entry === undefined) return
        const meta = entry._meta

        const baseWatermark = yield* log.getBaseWatermark(key)
        const cursor = yield* store.get
        const modelFloor = yield* log.floor(modelName)
        const lastResyncAt = yield* log.getLastResync
        const at = Option.getOrElse(cursor, () => SyncId.make("0"))

        switch (decideOnMount({ baseWatermark, cursor, modelFloor, lastResyncAt })) {
          case MountDecision.Skip:
            return
          case MountDecision.Replay: {
            const since = Option.getOrElse(baseWatermark, () => SyncId.make("0"))
            const rows = yield* log.read({ modelName, scope: key.scope, since })
            yield* Effect.forEach(rows, (row) => replayRow(meta, row), { discard: true })
            yield* log.setBaseWatermark({ key, at })
            return
          }
          case MountDecision.Bootstrap: {
            const found = yield* registry.getById(key as CollectionKey<Writable>)
            yield* Option.match(found, {
              onNone: () => Effect.void,
              onSome: (collection) => snapshotInstance(meta, key.scope, collection),
            })
            yield* log.setBaseWatermark({ key, at })
            return
          }
        }
      })

    // Heal every currently-mounted instance (idempotent — complete instances Skip). The registry also
    // queues a Mount signal per first mount, but a signal consumed by a cycle that then died with the
    // connection is gone for good — this pass makes healing a property of every cycle, not of queue
    // delivery, so a collection mounted during a disconnect still converges on the next catchup.
    const healMounted: Effect.Effect<void> = Effect.forEach(
      Object.values(map),
      (entry) =>
        registry
          .getByEntity(entry._meta.entity)
          .pipe(
            Effect.flatMap((mounted) => Effect.forEach(mounted, ({ key }) => onMount(key), { discard: true })),
          ),
      { discard: true },
    )

    const route = (item: Inbox): Effect.Effect<void, ResyncStop> => {
      if (item._tag === "Mount") return onMount(item.key)
      const event = item.event
      return event._tag === "Resync"
        ? log.setLastResync(event.syncId).pipe(
            Effect.zipRight(store.clear),
            Effect.zipRight(onResync),
            Effect.zipRight(Effect.fail(new ResyncStop())),
          )
        : ingest(event)
    }

    const inbox = Stream.merge(
      registry.mounts.pipe(Stream.map((key): Inbox => ({ _tag: "Mount", key }))),
      transport.connect.pipe(Stream.map((event): Inbox => ({ _tag: "Live", event }))),
    )

    const cycle = Effect.gen(function* () {
      const from = Option.getOrElse(yield* store.get, () => SyncId.make("0"))
      const response = yield* catchup.fetch({ from }).pipe(
        Effect.map(Option.some),
        Effect.catchTag("CatchupFailed", (error) =>
          Effect.logWarning(`[syncLoop] catchup failed, tailing anyway: ${error.reason}`).pipe(
            Effect.as(Option.none<CatchupResponse>()),
          ),
        ),
      )
      yield* Option.match(response, { onNone: () => Effect.void, onSome: (r) => applyCatchup({ response: r, from }) })
      yield* healMounted
      yield* Stream.runForEach(inbox, route)
    })

    yield* cycle.pipe(
      Effect.retry({
        while: (error) => error._tag === "SyncConnectionLost",
        schedule: Schedule.spaced("3 seconds"),
      }),
      Effect.catchTags({
        ResyncStop: () => Effect.void, // a live resync stopped the tail — done (prod has reloaded)
        SyncConnectionLost: () => Effect.void, // unreachable (infinite retry); present so the channel is `never`
      }),
    )
  })
