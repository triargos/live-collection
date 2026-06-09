import { Context, Effect, Layer, Option, Order, Ref, Schema, type Scope } from "effect"
import { compareSyncId, ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
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

// ── IndexedDB adapter (`layer`) — the durable home that survives reload/workspace-switch ──

/**
 * The at-rest IDB record for one log row: the plain, structured-clonable mirror of {@link LoggedEvent}.
 * `Option`s flatten to `string | null` (`scope`) / `unknown | null` (`data`) at the seam and decode back
 * on read — the store round-trips opaque wire `data`; the model schema re-decodes it later in replay.
 */
const StoredEvent = Schema.Struct({
  syncId: SyncId,
  modelName: ModelName,
  scope: Schema.OptionFromNullOr(Schema.String),
  tag: Schema.Literal("Insert", "Update", "Delete"),
  modelId: ModelId,
  data: Schema.OptionFromNullOr(Schema.Unknown),
})
const StoredEvents = Schema.Array(StoredEvent)

const DEFAULT_DATABASE_NAME = "live-collection-eventlog"
const EVENTS = "events" // object store: log rows, keyed by `syncId` (PK), indexed by `modelName`
const BY_MODEL = "byModel" // index on `events.modelName` — the replay read narrows to one model first
const META = "meta" // keyval object store (out-of-line keys): watermarks, prune floors, lastResync

const wmKey = (key: CollectionKey<unknown>): string => `wm:${serializeKey(key)}`
const floorKey = (modelName: string): string => `floor:${modelName}`
const RESYNC_KEY = "lastResync"

/** Resolve an `IDBRequest` to its result (rejection ⇒ a defect, surfaced by the caller's `Effect.promise`). */
const requestResult = <T>(request: IDBRequest<T>): Promise<T> =>
  new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
  })

/** Await a transaction's durable commit — the write is only real once `oncomplete` fires. */
const transactionDone = (tx: IDBTransaction): Promise<void> =>
  new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error)
    tx.onabort = () => reject(tx.error)
  })

const openEventLogDb = (databaseName: string): Promise<IDBDatabase> =>
  new Promise((resolve, reject) => {
    const open = indexedDB.open(databaseName, 1)
    open.onupgradeneeded = () => {
      const db = open.result
      db.createObjectStore(EVENTS, { keyPath: "syncId" }).createIndex(BY_MODEL, "modelName", { unique: false })
      db.createObjectStore(META)
    }
    open.onsuccess = () => resolve(open.result)
    open.onerror = () => reject(open.error)
  })

/**
 * The durable adapter. IDB orders string keys *lexicographically*, but `syncId`s order by *magnitude*, so
 * this never range-scans on the `syncId` key: `read`/`prune` narrow with the `modelName` index (or read
 * the whole — cap-bounded — store), then filter/sort/retain in memory with {@link compareSyncId}. Driver
 * faults are defects (`Effect.promise` dies on rejection); the method error channel stays empty.
 */
const makeIndexedDb = (databaseName: string): Effect.Effect<EventLogStoreShape, never, Scope.Scope> =>
  Effect.gen(function* () {
    const db = yield* Effect.acquireRelease(
      Effect.promise(() => openEventLogDb(databaseName)),
      (db) => Effect.sync(() => db.close()),
    )

    const getMetaSyncId = (key: string): Effect.Effect<Option.Option<SyncId>> =>
      Effect.promise(() => requestResult(db.transaction(META, "readonly").objectStore(META).get(key))).pipe(
        Effect.flatMap((value) =>
          value === undefined ? Effect.succeedNone : Schema.decodeUnknown(SyncId)(value).pipe(Effect.asSome),
        ),
        Effect.orDie,
      )

    // Monotonic write: read the current cursor, keep the larger, persist. (Sequential under the loop.)
    const advanceMetaSyncId = (key: string, at: SyncId): Effect.Effect<void> =>
      getMetaSyncId(key).pipe(
        Effect.flatMap((current) =>
          Effect.promise(async () => {
            const tx = db.transaction(META, "readwrite")
            tx.objectStore(META).put(advance(current, at), key)
            await transactionDone(tx)
          }),
        ),
      )

    const decodeAll = (raw: unknown): Effect.Effect<ReadonlyArray<LoggedEvent>> =>
      Schema.decodeUnknown(StoredEvents)(raw).pipe(Effect.orDie)

    return {
      append: (incoming) =>
        Schema.encode(StoredEvents)(incoming).pipe(
          Effect.orDie,
          Effect.flatMap((stored) =>
            Effect.promise(async () => {
              const tx = db.transaction(EVENTS, "readwrite")
              const store = tx.objectStore(EVENTS)
              for (const row of stored) store.put(row) // put = upsert by `syncId` keyPath ⇒ dedupe
              await transactionDone(tx)
            }),
          ),
        ),

      read: ({ modelName, scope, since }) =>
        Effect.promise(() =>
          requestResult(db.transaction(EVENTS, "readonly").objectStore(EVENTS).index(BY_MODEL).getAll(modelName)),
        ).pipe(
          Effect.flatMap(decodeAll),
          Effect.map((rows) =>
            rows
              .filter((r) => compareSyncId(r.syncId, since) > 0 && scopeMatches(scope, r.scope))
              .sort((a, b) => compareSyncId(a.syncId, b.syncId)),
          ),
        ),

      prune: ({ perModel, total }) =>
        Effect.promise(() => requestResult(db.transaction(EVENTS, "readonly").objectStore(EVENTS).getAll())).pipe(
          Effect.flatMap(decodeAll),
          Effect.flatMap((all) => {
            const plan = prunePlan({ rows: all, perModel, total })
            const kept = new Set(plan.keep.map((r) => r.syncId))
            const deleted = all.filter((r) => !kept.has(r.syncId))
            if (deleted.length === 0) return Effect.void
            // Merge each deleted-high-water into the current floor (monotonic) before the delete tx.
            return Effect.forEach([...plan.deletedHighWater], ([model, at]) =>
              getMetaSyncId(floorKey(model)).pipe(Effect.map((cur) => [floorKey(model), advance(cur, at)] as const)),
            ).pipe(
              Effect.flatMap((floors) =>
                Effect.promise(async () => {
                  const tx = db.transaction([EVENTS, META], "readwrite")
                  const events = tx.objectStore(EVENTS)
                  for (const r of deleted) events.delete(r.syncId)
                  const meta = tx.objectStore(META)
                  for (const [key, at] of floors) meta.put(at, key)
                  await transactionDone(tx)
                }),
              ),
            )
          }),
        ),

      floor: (modelName) => getMetaSyncId(floorKey(modelName)),
      getBaseWatermark: (key) => getMetaSyncId(wmKey(key)),
      setBaseWatermark: ({ key, at }) => advanceMetaSyncId(wmKey(key), at),
      getLastResync: getMetaSyncId(RESYNC_KEY),
      setLastResync: (at) => advanceMetaSyncId(RESYNC_KEY, at),
    }
  })

/** The seam: `yield* EventLogStore`. */
export class EventLogStore extends Context.Tag("EventLogStore")<EventLogStore, EventLogStoreShape>() {
  /** Test/dev: `Ref`-backed, non-durable. */
  static readonly layerMemory: Layer.Layer<EventLogStore> = Layer.effect(EventLogStore, makeMemory)
  /** Prod: durable IndexedDB. Opens (and closes on scope-out) `databaseName` ?? `live-collection-eventlog`. */
  static readonly layer = (options?: { readonly databaseName?: string }): Layer.Layer<EventLogStore> =>
    Layer.scoped(EventLogStore, makeIndexedDb(options?.databaseName ?? DEFAULT_DATABASE_NAME))
}
