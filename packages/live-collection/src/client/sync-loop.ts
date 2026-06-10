import { Data, Effect, Either, Option, Schedule, Schema, Stream } from "effect"
import {
  type CatchupResponse,
  type HydratedSyncEventEnvelope,
  type ModelId,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CollectionRegistry } from "../registry/collection-registry.js"
import { type CollectionKey, globalKey, scopedKey } from "../registry/collection-key.js"
import type { ModelMeta, SyncModels } from "../registry/define-collection.js"
import { LastSyncIdStore } from "./last-sync-id-store.js"
import { CatchupClient } from "./catchup-client.js"
import { SyncTransport } from "./sync-transport.js"
import { EventLogStore, type LoggedEvent } from "./event-log-store.js"
import { makeMountHealer, type Writable } from "./mount-healer.js"

/** An entity event (the `Resync` arm separated out) — what the loop applies to the local store. */
type EntityEvent = Exclude<HydratedSyncEventEnvelope, { readonly _tag: "Resync" }>

/** The merged inbox: live SSE events and registry mount signals, drained on one fiber so they never interleave. */
type Inbox =
  | { readonly _tag: "Live"; readonly event: HydratedSyncEventEnvelope }
  | { readonly _tag: "Mount"; readonly key: CollectionKey<unknown> }

/** Internal control signal: a live resync was seen, so stop the tail (it must not be retried). */
class ResyncStop extends Data.TaggedError("ResyncStop")<{}> {}

/**
 * Event-log retention knobs: keep the newest `perModel` events per model and `total`
 * overall, pruning once every `everyEvents` ingested events.
 */
export interface SyncLoopOptions {
  readonly prune: { readonly perModel: number; readonly total: number; readonly everyEvents: number }
}

/** Generous defaults — tune against your models' churn and the backend's catchup retention. */
const defaultOptions: SyncLoopOptions = { prune: { perModel: 1000, total: 5000, everyEvents: 100 } }

/**
 * The sync loop — the whole read path as one forever-running Effect: catch up from the
 * stored cursor, then tail the live SSE stream, applying each event to the mounted
 * collections, recording it in the durable event log, and advancing the cursor. On a
 * dropped connection it retries the cycle (catchup heals the gap). A collection that
 * mounts mid-flight is healed from its freshness metadata: skipped if already complete,
 * replayed from the local log, or bootstrapped via its `listFn`.
 *
 * Most apps never call this directly — `makeLiveRuntime` wires it and `useLiveSync` (or
 * `runtime.forkLoop`) forks it. Call it yourself only when composing the runtime by hand
 * (e.g. a custom host without `LiveRuntime`); then provide the five service tags in the
 * `R` channel and fork it once.
 *
 * `onResync` runs when a **live** `Resync` event arrives (server-declared "local state
 * is unsalvageable"): the loop clears the cursor, runs `onResync` (prod: reload the
 * window), and stops. A `Resync` inside a catchup response instead re-snapshots every
 * mounted collection in place.
 *
 * Runs forever — fork it. The error channel is `never`: connection drops retry
 * internally and one undecodable event is skipped, never fatal.
 */
export const syncLoop = (
  models: SyncModels,
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

    // The routing index: the wire model name IS the entity name (one name, written once).
    const metaByName = new Map<string, ModelMeta<any>>()
    for (const { _meta } of models) metaByName.set(_meta.entity, _meta)

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
        const meta = metaByName.get(event.modelName)
        if (meta === undefined) return // unknown model ⇒ skip both log and apply (a newer server may emit more)
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
          // A known model whose `data` doesn't decode is schema drift (a newer server, an older
          // client) — the same forward-compatibility case as an unknown model, so the same policy:
          // skip it wholesale (no log, no apply, no cursor advance — catchup overlap re-delivers it;
          // a snapshot/resync heals divergence). One bad event must never kill the forked loop.
          const decoded = yield* Effect.either(Schema.decodeUnknown(meta.schema)(event.data))
          if (Either.isLeft(decoded)) {
            return yield* Effect.logWarning(
              `[syncLoop] dropping undecodable ${event.modelName} event #${event.syncId}: ${decoded.left.message}`,
            )
          }
          const data = decoded.right
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

    // ── replay: apply a logged row through the SAME application path (decode at the boundary).
    // A logged row that no longer decodes (schema drift across sessions) is skipped with a warning,
    // same policy as live ingest — the rest of the replay still applies. ──
    const replayRow = (meta: ModelMeta<any>, row: LoggedEvent): Effect.Effect<void> =>
      row.tag === "Delete"
        ? applyDelete(meta, row.modelId)
        : Schema.decodeUnknown(meta.schema)(Option.getOrElse(row.data, () => null)).pipe(
            Effect.flatMap((data) => applyWrite(meta, data)),
            Effect.catchTag("ParseError", (error) =>
              Effect.logWarning(
                `[syncLoop] skipping undecodable logged ${row.modelName} event #${row.syncId}: ${error.message}`,
              ),
            ),
          )

    // Replace one mounted instance's contents with the server's current rows: one truncate+writes
    // sync transaction — no read of current keys, so it cannot race hydration.
    const snapshotInstance = (
      meta: ModelMeta<any>,
      scope: Option.Option<string>,
      collection: Writable,
    ): Effect.Effect<void> =>
      meta.listFn(scope).pipe(Effect.flatMap((rows) => collection.utils.replaceSynced(rows)))

    // Snapshot every mounted instance of every model.
    const snapshotAll: Effect.Effect<void> = Effect.forEach(
      [...metaByName.values()],
      (meta) =>
        registry
          .getByEntity<Writable>(meta.entity)
          .pipe(
            Effect.flatMap((mounted) =>
              Effect.forEach(mounted, ({ key, collection }) => snapshotInstance(meta, key.scope, collection), {
                discard: true,
              }),
            ),
          ),
      { discard: true },
    )

    // The healer owns watermark policy (heal decisions + post-catchup completeness stamps); the loop
    // hands it the two application arms and never writes watermarks itself.
    const healer = makeMountHealer({ models: metaByName, registry, store, log, replayRow, snapshotInstance })

    const applyCatchup = (args: { readonly response: CatchupResponse; readonly from: SyncId }): Effect.Effect<void> =>
      Effect.gen(function* () {
        const { response, from } = args
        const resync = response.events.some((event) => event._tag === "Resync")
        if (resync) {
          yield* Effect.forEach(
            response.events.filter((event) => event._tag === "Resync"),
            (event) => log.setLastResync(event.syncId),
            { discard: true },
          )
          yield* snapshotAll
        } else {
          yield* Effect.forEach(response.events, (event) => (event._tag === "Resync" ? Effect.void : ingest(event)), {
            discard: true,
          })
        }
        yield* store.set(response.lastSyncId)
        yield* healer.onCatchupApplied({ from, at: response.lastSyncId, resync })
      })

    const route = (item: Inbox): Effect.Effect<void, ResyncStop> => {
      if (item._tag === "Mount") return healer.heal(item.key)
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
      yield* healer.healAllMounted
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
