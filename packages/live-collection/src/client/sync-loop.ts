import { Data, Effect, Option, Schedule, Schema, Stream } from "effect"
import {
  type CatchupResponse,
  type HydratedSyncEventEnvelope,
  type ModelId,
  SyncId,
} from "@triargos/live-collection-protocol"
import { CollectionRegistry } from "../registry/collection-registry.js"
import { globalKey, scopedKey } from "../registry/collection-key.js"
import type { SyncMap } from "../registry/define-collection.js"
import type { SyncWrite } from "../dispatch/sync-write.js"
import { LastSyncIdStore } from "./last-sync-id-store.js"
import { CatchupClient } from "./catchup-client.js"
import { SyncTransport } from "./sync-transport.js"

/** An entity event (the `Resync` arm separated out) — what `dispatch` applies to the local store. */
type EntityEvent = Exclude<HydratedSyncEventEnvelope, { readonly _tag: "Resync" }>

/** Minimal view of a mounted collection the loop writes through. */
type Writable = { readonly utils: SyncWrite<unknown>; readonly keys: () => Iterable<ModelId> }

/** Internal control signal: a live resync was seen, so stop the tail (it must not be retried). */
class ResyncStop extends Data.TaggedError("ResyncStop")<{}> {}

/**
 * The read-path loop (DEC-R7), re-driven by the explicit {@link SyncMap} instead of a dispatcher tag
 * + bootstrap specs. Behaviour is the locked transport tier (DEC-T1…T9): catchup from the stored
 * cursor (`"0"` cold), then tail the live SSE; reconnect re-runs catchup so the gap heals.
 *
 * - A catchup `Resync` ⇒ **snapshot** every model's mounted instances via `_meta.listFn` + reconcile;
 *   otherwise dispatch the returned deltas. The cursor is then set from `lastSyncId` (DEC-T5).
 * - A **live** `Resync` ⇒ `cursor.clear` then `onResync` (prod: reload), and the loop stops.
 *
 * Runs forever — fork it (`useLiveSync`). Error channel is `never`: drops retry, `ResyncStop` is caught.
 */
export const syncLoop = (
  map: SyncMap,
  onResync: Effect.Effect<void>,
): Effect.Effect<void, never, CollectionRegistry | SyncTransport | CatchupClient | LastSyncIdStore> =>
  Effect.gen(function* () {
    const registry = yield* CollectionRegistry
    const store = yield* LastSyncIdStore
    const catchup = yield* CatchupClient
    const transport = yield* SyncTransport

    // Apply one entity event to the local store.
    const dispatch = (event: EntityEvent): Effect.Effect<void> =>
      Effect.gen(function* () {
        const entry = map[event.modelName]
        if (entry === undefined) return // unknown model ⇒ skip (a newer server may emit more)
        const meta = entry._meta

        if (event._tag === "Delete") {
          // No data to scope on, but the id is globally unique — fan out and let the owner remove it.
          const mounted = yield* registry.getByEntity<Writable>(meta.entity)
          return yield* Effect.forEach(mounted, ({ collection }) => collection.utils.deleteSynced(event.modelId), {
            discard: true,
          })
        }

        const data = yield* Schema.decodeUnknown(meta.schema)(event.data).pipe(Effect.orDie)
        const key = Option.match(meta.scopeOf, {
          onNone: () => globalKey<Writable>(meta.entity),
          onSome: (scopeOf) => scopedKey<Writable>({ entity: meta.entity, scope: scopeOf(data) }),
        })
        const found = yield* registry.getById(key)
        return yield* Option.match(found, {
          onNone: () => Effect.void, // not mounted ⇒ ignore (DEC-A11)
          onSome: (collection) => collection.utils.writeSynced(data),
        })
      })

    // Replace one mounted instance's contents with the server's current rows (DEC-T9 reconcile).
    const snapshotInstance = (
      meta: SyncMap[string]["_meta"],
      key: { readonly scope: Option.Option<string> },
      collection: Writable,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const rows = yield* meta.listFn(key.scope)
        const fetched = new Set(rows.map(meta.getKey))
        const absent = Array.from(collection.keys()).filter((k) => !fetched.has(k))
        yield* Effect.forEach(rows, (row) => collection.utils.writeSynced(row), { discard: true })
        yield* Effect.forEach(absent, (k) => collection.utils.deleteSynced(k), { discard: true })
      })

    // Snapshot every mounted instance of every model in the map.
    const snapshotAll: Effect.Effect<void> = Effect.forEach(
      Object.values(map),
      (entry) =>
        registry
          .getByEntity<Writable>(entry._meta.entity)
          .pipe(
            Effect.flatMap((mounted) =>
              Effect.forEach(mounted, ({ key, collection }) => snapshotInstance(entry._meta, key, collection), {
                discard: true,
              }),
            ),
          ),
      { discard: true },
    )

    const applyCatchup = (response: CatchupResponse): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (response.events.some((event) => event._tag === "Resync")) {
          yield* snapshotAll
        } else {
          yield* Effect.forEach(
            response.events,
            (event) => (event._tag === "Resync" ? Effect.void : dispatch(event)),
            { discard: true },
          )
        }
        yield* store.set(response.lastSyncId)
      })

    const route = (event: HydratedSyncEventEnvelope): Effect.Effect<void, ResyncStop> =>
      event._tag === "Resync"
        ? store.clear.pipe(Effect.zipRight(onResync), Effect.zipRight(Effect.fail(new ResyncStop())))
        : dispatch(event).pipe(Effect.zipRight(store.set(event.syncId)))

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
      yield* Option.match(response, { onNone: () => Effect.void, onSome: applyCatchup })
      yield* Stream.runForEach(transport.connect, route)
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
