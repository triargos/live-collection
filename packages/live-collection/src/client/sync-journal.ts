import { Context, Effect, Layer, Option, Order, Ref, Schema, type Scope } from "effect"
import { type DBSchema, openDB } from "idb"
import { advanceSyncId, compareSyncId, Epoch, maxSyncId, ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import { SchemaVersion } from "../core/schema-version.js"
import { type CollectionKey, serializeKey } from "../core/collection-key.js"
import { prunePlan } from "./prune-plan.js"

/**
 * One journal row — the schema-agnostic wire form of a received event, re-decoded by each
 * subscriber on replay. The broker is model-blind, so scope is derived and filtered only
 * after the subscriber decodes `data`. The encoded side doubles as the at-rest IDB record:
 * `data` flattens to `unknown | null` at the seam and decodes back on read.
 */
export const JournalEvent = Schema.Struct({
  syncId: SyncId,
  modelName: ModelName,
  tag: Schema.Literals(["Insert", "Update", "Delete"]),
  modelId: ModelId,
  data: Schema.OptionFromNullOr(Schema.Unknown),
})
export type JournalEvent = typeof JournalEvent.Type

/**
 * The client's durable **sync journal** — its local copy of the server's sync timeline,
 * plus the trust metadata that makes that copy interpretable. Its one job: let a
 * collection that mounts (or re-mounts after reload) answer locally, without the
 * network, *"can I trust my saved rows, and if so, what am I missing?"*
 *
 * Two halves, one consistency unit:
 *
 * - **The log** — every received event for every model, mounted or not. `append` is fed
 *   by the broker's single ingest path; `read` serves a mounting collection's replay
 *   slice; `prune`/`floor` trim history and remember, per model, how much was destroyed.
 * - **Trust metadata** — the global cursor ("the newest syncId ingested from any model";
 *   it gates catchup), per-collection last-applied syncIds ("the last event applied
 *   to this collection's saved rows was N"), `lastResync` ("replay across this point is
 *   invalid"), and the epoch ("every stored syncId is a coordinate on server timeline E").
 *
 * The broker's on-mount decision (Skip / Replay / Snapshot) is computed from journal
 * reads alone. The halves share one service because an epoch reset (`adoptEpoch`) must
 * wipe the log and move the cursor atomically, and `prune` + `floor` are one operation
 * split across both stores.
 */
export interface SyncJournalShape {
  /** Append received events; upsert by `syncId` so catchup/tail overlap dedupes for free. */
  readonly append: (rows: ReadonlyArray<JournalEvent>) => Effect.Effect<void>
  /**
   * The replay slice for one model after `since` (exclusive), syncId-ordered.
   * Scoped subscribers decode and filter this bounded per-model slice themselves.
   */
  readonly read: (args: {
    readonly modelName: ModelName
    readonly since: SyncId
  }) => Effect.Effect<ReadonlyArray<JournalEvent>>
  /**
   * Trim the log in three stages: squash to the newest event per entity, drop rows every
   * collection has already applied (both floor-neutral), then enforce the count caps —
   * the only stage that moves the floor. See {@link prunePlan} for the full policy.
   */
  readonly prune: (caps: {
    readonly maxEventsPerModel: number
    readonly maxEventsTotal: number
  }) => Effect.Effect<void>
  /** The model's prune boundary: `None` ⇒ nothing pruned (complete from the start); `Some(f)` ⇒ deleted below `f`. */
  readonly floor: (modelName: ModelName) => Effect.Effect<Option.Option<SyncId>>

  /**
   * The syncId of the last event applied to this collection's saved rows. Application
   * is sequential and gapless, so everything at or below it is present. One record per
   * collection key, stamped with the `schemaVersion` of the saved rows it describes:
   * a write under a different version **supersedes** the record outright (the saved
   * table was dumped, so the old mark describes rows that no longer exist), and a read
   * under a version other than the stored one finds `None` — the mount then decides
   * `Snapshot` instead of trusting a mark for a dead table. Within one version the
   * record advances monotonically. `prune` folds these records into its per-model
   * minimum: everything at or below the minimum is dead weight no replayer can need.
   */
  readonly getCollectionLastAppliedSyncId: (args: {
    readonly key: CollectionKey<unknown>
    readonly schemaVersion: SchemaVersion
  }) => Effect.Effect<Option.Option<SyncId>>
  readonly setCollectionLastAppliedSyncId: (args: {
    readonly key: CollectionKey<unknown>
    readonly schemaVersion: SchemaVersion
    readonly at: SyncId
  }) => Effect.Effect<void>
  /** The newest resync the client has ingested (monotonic) — invalidates replay across it. */
  readonly getLastResync: Effect.Effect<Option.Option<SyncId>>
  readonly setLastResync: (at: SyncId) => Effect.Effect<void>

  /** The durable global cursor — newest syncId ingested from any model; gates catchup
   *  (`from = cursor ?? "0"`). `None` only on a truly cold start. */
  readonly getCursor: Effect.Effect<Option.Option<SyncId>>
  /** Monotonic — keeps the larger id by numeric magnitude; a late out-of-order event can
   *  never pull the cursor backwards. */
  readonly setCursor: (id: SyncId) => Effect.Effect<void>

  /**
   * The epoch of the server timeline this journal was built from — the identity every
   * stored syncId is relative to. `None` until a catchup response first carries one.
   */
  readonly getEpoch: Effect.Effect<Option.Option<Epoch>>
  readonly setEpoch: (epoch: Epoch) => Effect.Effect<void>
  /**
   * Epoch reset in ONE transaction: wipe the ENTIRE journal (every logged event, all
   * collection last-applied syncIds, all prune floors, lastResync), install the new
   * epoch, and place the cursor at `at`. Nothing survives; the next mount necessarily
   * decides Snapshot. A stale epoch means *no* local sync state is trustworthy, so
   * partial retention has no correct form — and a cursor that outlived the wipe would
   * be an old-timeline coordinate gating catchup on the new timeline.
   */
  readonly adoptEpoch: (args: { readonly epoch: Epoch; readonly at: SyncId }) => Effect.Effect<void>
}

/**
 * The stored last-applied mark — a structured value so `prune` can group by `entity`
 * without ever parsing a key (`serializeKey` stays write-only). `schemaVersion` is the
 * record's supersede guard, not part of its identity: one record per collection key.
 */
const LastAppliedRecord = Schema.Struct({
  entity: Schema.String,
  schemaVersion: SchemaVersion,
  at: SyncId,
})
type LastAppliedRecord = typeof LastAppliedRecord.Type

/** Supersede-or-advance: same version ⇒ monotonic; version change ⇒ replace outright. */
const foldLastApplied = (
  current: Option.Option<LastAppliedRecord>,
  next: { readonly entity: string; readonly schemaVersion: SchemaVersion; readonly at: SyncId },
): LastAppliedRecord => ({
  entity: next.entity,
  schemaVersion: next.schemaVersion,
  at: advanceSyncId(
    current.pipe(
      Option.filter((record) => record.schemaVersion === next.schemaVersion),
      Option.map((record) => record.at),
    ),
    next.at,
  ),
})

/** Stage-2 input: the minimum last-applied syncId per model, folded from every record. */
const minLastAppliedByModel = (records: Iterable<LastAppliedRecord>): Map<string, SyncId> => {
  const min = new Map<string, SyncId>()
  for (const record of records) {
    const current = min.get(record.entity)
    min.set(record.entity, current ? Order.min(compareSyncId)(current, record.at) : record.at)
  }
  return min
}

/** In-memory adapter over `Ref`s — broker behavior tests. */
const makeMemory: Effect.Effect<SyncJournalShape> = Effect.gen(function* () {
  const rows = yield* Ref.make(new Map<string, JournalEvent>()) // keyed by syncId ⇒ upsert dedupe
  const lastApplied = yield* Ref.make(new Map<string, LastAppliedRecord>()) // keyed by serializeKey(key) — one record per collection
  const floors = yield* Ref.make(new Map<string, SyncId>()) // modelName ⇒ prune boundary (highest deleted)
  const lastResync = yield* Ref.make(Option.none<SyncId>())
  const epoch = yield* Ref.make(Option.none<Epoch>())
  const cursor = yield* Ref.make(Option.none<SyncId>())

  return {
    append: (incoming) =>
      Ref.update(rows, (m) => {
        const next = new Map(m)
        for (const row of incoming) next.set(row.syncId, row)
        return next
      }),
    read: ({ modelName, since }) =>
      Ref.get(rows).pipe(
        Effect.map((m) =>
          [...m.values()]
            .filter((r) => r.modelName === modelName && compareSyncId(r.syncId, since) > 0)
            .sort((a, b) => compareSyncId(a.syncId, b.syncId)),
        ),
      ),
    prune: ({ maxEventsPerModel, maxEventsTotal }) =>
      Effect.all([Ref.get(rows), Ref.get(lastApplied)]).pipe(
        Effect.flatMap(([m, applied]) => {
          const plan = prunePlan({
            rows: [...m.values()],
            minLastApplied: minLastAppliedByModel(applied.values()),
            maxEventsPerModel,
            maxEventsTotal,
          })
          return Ref.set(rows, new Map(plan.keep.map((r) => [r.syncId, r] as const))).pipe(
            Effect.andThen(
              Ref.update(floors, (f) => {
                const next = new Map(f)
                for (const [model, at] of plan.maxDeletedSyncId) {
                  const current = next.get(model)
                  next.set(model, current ? maxSyncId(current, at) : at)
                }
                return next
              }),
            ),
          )
        }),
      ),
    floor: (modelName) => Ref.get(floors).pipe(Effect.map((f) => Option.fromNullishOr(f.get(modelName)))),
    getCollectionLastAppliedSyncId: ({ key, schemaVersion }) =>
      Ref.get(lastApplied).pipe(
        Effect.map((m) =>
          Option.fromNullishOr(m.get(serializeKey(key))).pipe(
            Option.filter((record) => record.schemaVersion === schemaVersion),
            Option.map((record) => record.at),
          ),
        ),
      ),
    setCollectionLastAppliedSyncId: ({ key, schemaVersion, at }) =>
      Ref.update(lastApplied, (m) => {
        const next = new Map(m)
        const id = serializeKey(key)
        next.set(id, foldLastApplied(Option.fromNullishOr(m.get(id)), { entity: key.entity, schemaVersion, at }))
        return next
      }),
    getLastResync: Ref.get(lastResync),
    setLastResync: (at) => Ref.update(lastResync, (c) => Option.some(advanceSyncId(c, at))),
    getCursor: Ref.get(cursor),
    setCursor: (id) => Ref.update(cursor, (c) => Option.some(advanceSyncId(c, id))),
    getEpoch: Ref.get(epoch),
    setEpoch: (value) => Ref.set(epoch, Option.some(value)),
    adoptEpoch: ({ epoch: next, at }) =>
      Effect.all(
        [
          Ref.set(rows, new Map<string, JournalEvent>()),
          Ref.set(lastApplied, new Map<string, LastAppliedRecord>()),
          Ref.set(floors, new Map<string, SyncId>()),
          Ref.set(lastResync, Option.none<SyncId>()),
          Ref.set(epoch, Option.some(next)),
          Ref.set(cursor, Option.some(at)),
        ],
        { discard: true },
      ),
  }
})

// ── IndexedDB adapter (`layer`) — the durable home that survives reload/workspace-switch ──

const StoredEvents = Schema.Array(JournalEvent)

// The on-disk name predates the SyncJournal rename; changing it would orphan every
// existing client's journal (a silent global reset), so it stays "eventlog".
const DEFAULT_DATABASE_NAME = "live-collection-eventlog"
const EVENTS = "events" // object store: journal rows, keyed by `syncId` (PK), indexed by `modelName`
const BY_MODEL = "byModel" // index on `events.modelName` — the replay read narrows to one model first
const META = "meta" // keyval object store (out-of-line keys): collection last-applied syncIds, prune floors, lastResync

// One record per collection key; the schema version lives in the *value* as the
// supersede guard. The "wm:" prefix predates the rename; it is only ever used for
// lookup and prefix ranges — never parsed back (the entity for prune's grouping comes
// from the stored record).
const LAST_APPLIED_PREFIX = "wm:"
const lastAppliedKey = (key: CollectionKey<unknown>): string => `${LAST_APPLIED_PREFIX}${serializeKey(key)}`
const floorKey = (modelName: string): string => `floor:${modelName}`
const RESYNC_KEY = "lastResync"
const EPOCH_KEY = "epoch"
const CURSOR_KEY = "cursor"

interface SyncJournalDbSchema extends DBSchema {
  [EVENTS]: {
    key: string
    value: Schema.Codec.Encoded<typeof JournalEvent>
    indexes: { [BY_MODEL]: string }
  }
  [META]: {
    key: string
    value: unknown
  }
}

/**
 * The durable adapter. IDB orders string keys *lexicographically*, but `syncId`s order by *magnitude*, so
 * this never range-scans on the `syncId` key: `read`/`prune` narrow with the `modelName` index (or read
 * the whole — cap-bounded — store), then filter/sort/retain in memory with {@link compareSyncId}. Driver
 * faults are defects (`Effect.promise` dies on rejection); the method error channel stays empty.
 */
const makeIndexedDb = (databaseName: string): Effect.Effect<SyncJournalShape, never, Scope.Scope> =>
  Effect.gen(function* () {
    const db = yield* Effect.acquireRelease(
      Effect.promise(() =>
        openDB<SyncJournalDbSchema>(databaseName, 1, {
          upgrade(db) {
            db.createObjectStore(EVENTS, { keyPath: "syncId" }).createIndex(BY_MODEL, "modelName", { unique: false })
            db.createObjectStore(META)
          },
        }),
      ),
      (db) => Effect.sync(() => db.close()),
    )

    const getMetaSyncId = (key: string): Effect.Effect<Option.Option<SyncId>> =>
      Effect.promise(() => db.get(META, key)).pipe(
        Effect.flatMap((value) =>
          value === undefined ? Effect.succeedNone : Schema.decodeUnknownEffect(SyncId)(value).pipe(Effect.asSome),
        ),
        Effect.orDie,
      )

    // Monotonic write: read the current cursor, keep the larger, persist. (Sequential under broker ingest.)
    const advanceMetaSyncId = (key: string, at: SyncId): Effect.Effect<void> =>
      getMetaSyncId(key).pipe(
        Effect.flatMap((current) => Effect.promise(() => db.put(META, advanceSyncId(current, at), key))),
        Effect.asVoid,
      )

    const decodeAll = (raw: unknown): Effect.Effect<ReadonlyArray<JournalEvent>> =>
      Schema.decodeUnknownEffect(StoredEvents)(raw).pipe(Effect.orDie)

    // Every last-applied record, via a prefix range over the "wm:" keys (keys are never
    // parsed — the entity for grouping comes from the stored record's value).
    const readLastAppliedRecords: Effect.Effect<ReadonlyArray<LastAppliedRecord>> = Effect.promise(() =>
      db.getAll(META, IDBKeyRange.bound(LAST_APPLIED_PREFIX, `${LAST_APPLIED_PREFIX}\uffff`)),
    ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(LastAppliedRecord))), Effect.orDie)

    const getLastAppliedRecord = (key: CollectionKey<unknown>): Effect.Effect<Option.Option<LastAppliedRecord>> =>
      Effect.promise(() => db.get(META, lastAppliedKey(key))).pipe(
        Effect.flatMap((value) =>
          value === undefined
            ? Effect.succeedNone
            : Schema.decodeUnknownEffect(LastAppliedRecord)(value).pipe(Effect.asSome),
        ),
        Effect.orDie,
      )

    return {
      append: (incoming) =>
        Schema.encodeEffect(StoredEvents)(incoming).pipe(
          Effect.orDie,
          Effect.flatMap((stored) =>
            Effect.promise(async () => {
              const tx = db.transaction(EVENTS, "readwrite")
              // put = upsert by `syncId` keyPath ⇒ dedupe
              await Promise.all([...stored.map((row) => tx.store.put(row)), tx.done])
            }),
          ),
        ),

      read: ({ modelName, since }) =>
        Effect.promise(() => db.getAllFromIndex(EVENTS, BY_MODEL, modelName)).pipe(
          Effect.flatMap(decodeAll),
          Effect.map((rows) =>
            rows.filter((r) => compareSyncId(r.syncId, since) > 0).sort((a, b) => compareSyncId(a.syncId, b.syncId)),
          ),
        ),

      prune: ({ maxEventsPerModel, maxEventsTotal }) =>
        Effect.all([Effect.promise(() => db.getAll(EVENTS)).pipe(Effect.flatMap(decodeAll)), readLastAppliedRecords]).pipe(
          Effect.flatMap(([all, records]) => {
            const plan = prunePlan({
              rows: all,
              minLastApplied: minLastAppliedByModel(records),
              maxEventsPerModel,
              maxEventsTotal,
            })
            const kept = new Set(plan.keep.map((r) => r.syncId))
            const deleted = all.filter((r) => !kept.has(r.syncId))
            if (deleted.length === 0) return Effect.void
            // Merge each model's max deleted syncId into the current floor (monotonic) before the delete tx.
            return Effect.forEach([...plan.maxDeletedSyncId], ([model, at]) =>
              getMetaSyncId(floorKey(model)).pipe(Effect.map((cur) => [floorKey(model), advanceSyncId(cur, at)] as const)),
            ).pipe(
              Effect.flatMap((floors) =>
                Effect.promise(async () => {
                  const tx = db.transaction([EVENTS, META], "readwrite")
                  const events = tx.objectStore(EVENTS)
                  const meta = tx.objectStore(META)
                  await Promise.all([
                    ...deleted.map((r) => events.delete(r.syncId)),
                    ...floors.map(([key, at]) => meta.put(at, key)),
                    tx.done,
                  ])
                }),
              ),
            )
          }),
        ),

      floor: (modelName) => getMetaSyncId(floorKey(modelName)),
      getCollectionLastAppliedSyncId: ({ key, schemaVersion }) =>
        getLastAppliedRecord(key).pipe(
          Effect.map(
            Option.flatMap((record) =>
              record.schemaVersion === schemaVersion ? Option.some(record.at) : Option.none(),
            ),
          ),
        ),
      // Supersede-or-advance: read the record, fold, persist. (Sequential under broker ingest.)
      setCollectionLastAppliedSyncId: ({ key, schemaVersion, at }) =>
        getLastAppliedRecord(key).pipe(
          Effect.flatMap((current) =>
            Effect.promise(() =>
              db.put(META, foldLastApplied(current, { entity: key.entity, schemaVersion, at }), lastAppliedKey(key)),
            ),
          ),
          Effect.asVoid,
        ),
      getLastResync: getMetaSyncId(RESYNC_KEY),
      setLastResync: (at) => advanceMetaSyncId(RESYNC_KEY, at),
      getCursor: getMetaSyncId(CURSOR_KEY),
      setCursor: (id) => advanceMetaSyncId(CURSOR_KEY, id),

      getEpoch: Effect.promise(() => db.get(META, EPOCH_KEY)).pipe(
        Effect.flatMap((value) =>
          value === undefined ? Effect.succeedNone : Schema.decodeUnknownEffect(Epoch)(value).pipe(Effect.asSome),
        ),
        Effect.orDie,
      ),
      setEpoch: (epoch) => Effect.promise(() => db.put(META, epoch, EPOCH_KEY)).pipe(Effect.asVoid),
      // One tx: wipe events + every meta record, then install the new epoch and cursor —
      // a partial reset (events wiped but an old-epoch cursor surviving, or vice versa)
      // would recreate exactly the inconsistency this method exists to end.
      adoptEpoch: ({ epoch, at }) =>
        Effect.promise(async () => {
          const tx = db.transaction([EVENTS, META], "readwrite")
          const meta = tx.objectStore(META)
          await tx.objectStore(EVENTS).clear()
          await meta.clear()
          await Promise.all([meta.put(epoch, EPOCH_KEY), meta.put(at, CURSOR_KEY), tx.done])
        }),
    }
  })

/**
 * The sync-journal service tag. Provide one of its layers as part of the `loop` layer
 * handed to `makeLiveRuntime`:
 *
 * @example
 * ```ts
 * SyncJournal.layer({ databaseName: "myapp-eventlog" }) // browser, durable
 * SyncJournal.layerMemory                               // tests/SSR, non-durable
 * ```
 */
export class SyncJournal extends Context.Service<SyncJournal, SyncJournalShape>()("SyncJournal") {
  /** In-memory (`Ref`-backed), non-durable — for tests and SSR. */
  static readonly layerMemory: Layer.Layer<SyncJournal> = Layer.effect(SyncJournal, makeMemory)
  /** Browser default: durable IndexedDB. Opens (and closes on scope-out) `databaseName` ?? `"live-collection-eventlog"`. */
  static readonly layer = (options?: { readonly databaseName?: string }): Layer.Layer<SyncJournal> =>
    Layer.effect(SyncJournal, makeIndexedDb(options?.databaseName ?? DEFAULT_DATABASE_NAME))
}
