import { Context, Effect, Layer, Option, Schema } from "effect"
import type { HydratedSyncEventEnvelope } from "@triargos/live-collection-protocol"
import { CollectionRegistry } from "../registry/collection-registry.js"
import type { CollectionKey } from "../registry/collection-key.js"
import type { SyncWrite } from "./sync-write.js"

/**
 * An entity event with its `data` still opaque — the entity arms (`Insert` / `Update` /
 * `Delete`) of the wire envelope, with resync separated out beforehand. This is what a
 * transport hands to {@link SyncDispatcherShape.dispatch}.
 */
export type EntityEvent = Exclude<HydratedSyncEventEnvelope, { readonly _tag: "Resync" }>

/**
 * How one model's events are applied, as plain data. Bind a model name to one of these and
 * the dispatcher does the rest — no per-model code runs.
 *
 * @typeParam T - the model's entity type
 * @typeParam Args - the arguments the collection is mounted with (e.g. an org id)
 */
export interface DispatchEntry<T, Args> {
  /** Decodes an event's wire `data` into the entity type. */
  readonly schema: Schema.Schema<T, any>
  /**
   * The model's collection, as defined with `defineCollection`. Only its `.key` is read —
   * the dispatcher routes to collections that are already mounted and never mounts one itself.
   * The key carries the mounted value's shape: the synced-write path lives on `.utils` (a
   * `LiveCollection<T>` hosts `SyncWrite<T>` there), which is where `dispatch` writes.
   */
  readonly collection: (args: Args) => { readonly key: CollectionKey<{ readonly utils: SyncWrite<T> }> }
  /** Reads an entity's own mount arguments from it, e.g. `(webhook) => webhook.orgId`. */
  readonly scopeOf: (entity: T) => Args
}

/**
 * Builds a {@link DispatchEntry}, tying the schema, collection, and `scopeOf` to one entity
 * type. It only constructs a value — it registers nothing and runs no effect, so import order
 * never matters; a model is wired if its entry is in the set passed to
 * {@link SyncDispatcher.fromEntries}.
 */
export const dispatchEntry = <T, Args>(
  entry: DispatchEntry<T, Args>,
): DispatchEntry<unknown, unknown> =>
  entry as unknown as DispatchEntry<unknown, unknown>

export interface SyncDispatcherShape {
  /**
   * Apply one entity event to the local store. Unknown models are skipped (a newer server may
   * emit models this client doesn't know). `Insert`/`Update` are routed to the single owning
   * collection via `scopeOf`; `Delete` is fanned out to every mounted collection of the model
   * (its id may live in any scope) and is idempotent.
   */
  readonly dispatch: (event: EntityEvent) => Effect.Effect<void, never, CollectionRegistry>
}

const make = (
  entries: Record<string, DispatchEntry<unknown, unknown>>,
): SyncDispatcherShape => ({
  dispatch: (event) =>
    Effect.gen(function* () {
      const entry = entries[event.modelName]
      if (entry === undefined) return // unknown model ⇒ skip (a newer server may emit more)
      const registry = yield* CollectionRegistry

      if (event._tag === "Delete") {
        // No data to scope on, but the id is globally unique — fan out and let the owner remove it.
        const collections = yield* registry.getByEntity<{ utils: SyncWrite<unknown> }>(event.modelName)
        return yield* Effect.forEach(collections, (c) => c.utils.deleteSynced(event.modelId), {
          discard: true,
        })
      }

      const data = yield* Schema.decodeUnknown(entry.schema)(event.data).pipe(Effect.orDie)
      const key = entry.collection(entry.scopeOf(data)).key
      const found = yield* registry.getById<{ utils: SyncWrite<unknown> }>(key)
      return yield* Option.match(found, {
        onNone: () => Effect.void, // not mounted ⇒ ignore
        onSome: (collection) => collection.utils.writeSynced(data),
      })
    }),
})

/**
 * Routes server events to the collections that hold their data. Configure it with a static set
 * of {@link DispatchEntry} keyed by model name (the key is the model name and the collection's
 * entity), then `yield*` the tag wherever events arrive.
 */
export class SyncDispatcher extends Context.Tag("SyncDispatcher")<
  SyncDispatcher,
  SyncDispatcherShape
>() {
  /** A layer providing the dispatcher for the given model set. */
  static readonly fromEntries = (
    entries: Record<string, DispatchEntry<unknown, unknown>>,
  ): Layer.Layer<SyncDispatcher> => Layer.succeed(SyncDispatcher, make(entries))
}
