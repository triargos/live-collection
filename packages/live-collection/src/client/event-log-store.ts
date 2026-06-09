import { Context, Effect, Layer, Option, Order, Ref } from "effect"
import { compareSyncId, type ModelId, type ModelName, type SyncId } from "@triargos/live-collection-protocol"
import { type CollectionKey, serializeKey } from "../registry/collection-key.js"
import { prunePlan } from "./prune-plan.js"

/**
 * The at-rest log row — schema-agnostic (wire form), re-decoded on replay. It is an {@link AppliedEvent}
 * (modelName/tag/modelId/data) plus the two columns the store indexes on: `syncId` (PK, order, dedupe)
 * and `scope` (the read filter). `scope` is `None` for a global instance's event *or* a `Delete`
 * (which carries no data to derive a scope from); the two never collide because an entity is uniformly
 * global or scoped.
 */
export interface LoggedEvent {
  readonly syncId: SyncId
  readonly modelName: ModelName
  readonly scope: Option.Option<string>
  readonly tag: "Insert" | "Update" | "Delete"
  readonly modelId: ModelId
  readonly data: Option.Option<unknown>
}

/**
 * The durable client-side event log + its sync metadata — the one home for "what events have we
 * received" and "how complete is each collection's base." Append is fed by the same single ingest as
 * the live store; `read` serves replay slices; `floor` is the per-model **prune boundary** that guards
 * replay; watermarks (`B_X`) and `lastResyncAt` gate the mount decision.
 */
export interface EventLogStoreShape {
  /** Append received events; upsert by `syncId` so catchup/tail overlap dedupes for free. */
  readonly append: (rows: ReadonlyArray<LoggedEvent>) => Effect.Effect<void>
  /**
   * The replay slice for one collection — its model's events after `since` (exclusive), syncId-ordered.
   * `scope` `None` ⇒ the whole model (a global instance); `Some(s)` ⇒ that scope's rows plus the
   * model's scope-less `Delete`s (which fan across scopes, exactly as live dispatch does).
   */
  readonly read: (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly since: SyncId
  }) => Effect.Effect<ReadonlyArray<LoggedEvent>>
  /** Trim the log: per-model keep newest `perModel`, then globally keep newest `total`. */
  readonly prune: (caps: { readonly perModel: number; readonly total: number }) => Effect.Effect<void>
  /** The model's prune boundary: `None` ⇒ nothing pruned (complete from the start); `Some(f)` ⇒ deleted below `f`. */
  readonly floor: (modelName: ModelName) => Effect.Effect<Option.Option<SyncId>>

  /** `B_X` — the syncId through which this collection's base is complete. */
  readonly getBaseWatermark: (key: CollectionKey<unknown>) => Effect.Effect<Option.Option<SyncId>>
  readonly setBaseWatermark: (a: { readonly key: CollectionKey<unknown>; readonly at: SyncId }) => Effect.Effect<void>
  /** The newest resync the client has ingested (monotonic) — invalidates replay across it. */
  readonly getLastResync: Effect.Effect<Option.Option<SyncId>>
  readonly setLastResync: (at: SyncId) => Effect.Effect<void>
}

/** Keep whichever syncId is numerically larger — the monotonic step shared by watermark/resync setters. */
const advance = (current: Option.Option<SyncId>, next: SyncId): SyncId =>
  Option.match(current, { onNone: () => next, onSome: (c) => Order.max(compareSyncId)(c, next) })

const scopeMatches = (query: Option.Option<string>, row: Option.Option<string>): boolean =>
  Option.match(query, {
    onNone: () => true, // global model ⇒ every row of the model
    onSome: (s) => Option.match(row, { onNone: () => true, onSome: (rs) => rs === s }), // scope rows + scope-less Deletes
  })

/** In-memory adapter over `Ref`s — the loop's behavior tests. */
const makeMemory: Effect.Effect<EventLogStoreShape> = Effect.gen(function* () {
  const rows = yield* Ref.make(new Map<string, LoggedEvent>()) // keyed by syncId ⇒ upsert dedupe
  const watermarks = yield* Ref.make(new Map<string, SyncId>()) // keyed by serializeKey(key)
  const floors = yield* Ref.make(new Map<string, SyncId>()) // modelName ⇒ prune boundary (highest deleted)
  const lastResync = yield* Ref.make(Option.none<SyncId>())

  return {
    append: (incoming) =>
      Ref.update(rows, (m) => {
        const next = new Map(m)
        for (const row of incoming) next.set(row.syncId, row)
        return next
      }),
    read: ({ modelName, scope, since }) =>
      Ref.get(rows).pipe(
        Effect.map((m) =>
          [...m.values()]
            .filter(
              (r) => r.modelName === modelName && compareSyncId(r.syncId, since) > 0 && scopeMatches(scope, r.scope),
            )
            .sort((a, b) => compareSyncId(a.syncId, b.syncId)),
        ),
      ),
    prune: ({ perModel, total }) =>
      Ref.get(rows).pipe(
        Effect.flatMap((m) => {
          const plan = prunePlan({ rows: [...m.values()], perModel, total })
          return Ref.set(rows, new Map(plan.keep.map((r) => [r.syncId, r] as const))).pipe(
            Effect.zipRight(
              Ref.update(floors, (f) => {
                const next = new Map(f)
                for (const [model, at] of plan.deletedHighWater) {
                  const current = next.get(model)
                  next.set(model, current ? Order.max(compareSyncId)(current, at) : at)
                }
                return next
              }),
            ),
          )
        }),
      ),
    floor: (modelName) => Ref.get(floors).pipe(Effect.map((f) => Option.fromNullable(f.get(modelName)))),
    getBaseWatermark: (key) => Ref.get(watermarks).pipe(Effect.map((m) => Option.fromNullable(m.get(serializeKey(key))))),
    setBaseWatermark: ({ key, at }) =>
      Ref.update(watermarks, (m) => {
        const next = new Map(m)
        const id = serializeKey(key)
        next.set(id, advance(Option.fromNullable(m.get(id)), at))
        return next
      }),
    getLastResync: Ref.get(lastResync),
    setLastResync: (at) => Ref.update(lastResync, (c) => Option.some(advance(c, at))),
  }
})

/** The seam: `yield* EventLogStore`. */
export class EventLogStore extends Context.Tag("EventLogStore")<EventLogStore, EventLogStoreShape>() {
  /** Test/dev: `Ref`-backed. (Prod IndexedDB adapter — `layer` — lands with the browser proof.) */
  static readonly layerMemory: Layer.Layer<EventLogStore> = Layer.effect(EventLogStore, makeMemory)
}
