import { Context, Effect, Layer, Option, Order, Schema } from "effect"
import { advanceSyncId, compareSyncId, Epoch, ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import { SchemaVersion } from "../core/schema-version.js"
import { type CollectionKey, serializeKey } from "../core/collection-key.js"
import {
  type JournalStore,
  JournalWrite,
  LAST_APPLIED_PREFIX,
  makeIdbStore,
  makeMemoryStore,
} from "./journal-store.js"
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
 *   slice; `prune`/`highestPrunedSyncId` trim history and remember, per model, how much
 *   was destroyed.
 * - **Trust metadata** — the last-ingested syncId ("the newest syncId ingested from any
 *   model"; it gates catchup), per-collection last-applied syncIds ("the last event
 *   applied to this collection's saved rows was N"), `lastResync` ("replay across this
 *   point is invalid"), and the epoch ("every stored syncId is a coordinate on server
 *   timeline E").
 *
 * The broker's on-mount decision (Skip / Replay / Snapshot) is computed from journal
 * reads alone. The halves share one service because an epoch reset (`resetToEpoch`)
 * must wipe the log and move the last-ingested syncId atomically, and `prune` +
 * `highestPrunedSyncId` are one operation split across both halves.
 *
 * All policy — codecs, fold rules, prune orchestration — lives here, written once over
 * the {@link JournalStore} port; the per-engine adapters are dumb keyed storage.
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
   * collection has already applied (both prune-boundary-neutral), then enforce the count
   * caps — the only stage that moves {@link highestPrunedSyncId}. See {@link prunePlan}
   * for the full policy.
   */
  readonly prune: (caps: {
    readonly maxEventsPerModel: number
    readonly maxEventsTotal: number
  }) => Effect.Effect<void>
  /**
   * The highest syncId among events pruning permanently deleted for this model.
   * `None` ⇒ nothing pruned (the log is complete from the start); `Some(p)` ⇒ events at
   * or below `p` are gone forever, so a replay gap starting at or below it is unfillable
   * — the mount must decide `Snapshot`.
   */
  readonly highestPrunedSyncId: (modelName: ModelName) => Effect.Effect<Option.Option<SyncId>>

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
  /**
   * The newest server-declared break in the timeline (a `Resync` event) — replay across
   * it is invalid: the server itself cannot connect the two sides by deltas (permission
   * change, bulk correction, or its own retention loss). A collection whose last-applied
   * syncId predates it must `Snapshot`, no matter what the log contains. Monotonic —
   * only the newest break matters.
   */
  readonly getLastResync: Effect.Effect<Option.Option<SyncId>>
  readonly setLastResync: (at: SyncId) => Effect.Effect<void>

  /**
   * The global high-water mark of the log: the newest syncId durably ingested from
   * *any* model. Gates catchup (`from = lastIngested ?? "0"`) and is the "how far has
   * the world moved" side of the mount decision. NOT per-collection "applied" — an
   * unmounted collection's last-applied mark can trail far behind this. `None` only on
   * a truly cold start.
   */
  readonly getLastIngestedSyncId: Effect.Effect<Option.Option<SyncId>>
  /** Monotonic — keeps the larger id by numeric magnitude; a late out-of-order event can
   *  never pull the mark backwards. */
  readonly setLastIngestedSyncId: (id: SyncId) => Effect.Effect<void>

  /**
   * The epoch of the server timeline this journal was built from — the identity every
   * stored syncId is relative to. `None` until a catchup response first carries one.
   * `setEpoch` is the first-seen stamp: it labels *existing, trusted* state and wipes
   * nothing — the gentle sibling of {@link resetToEpoch}.
   */
  readonly getEpoch: Effect.Effect<Option.Option<Epoch>>
  readonly setEpoch: (epoch: Epoch) => Effect.Effect<void>
  /**
   * The server timeline changed identity: wipe the ENTIRE journal (every logged event,
   * all collection last-applied syncIds, all prune boundaries, lastResync), install the
   * new epoch, and place the last-ingested syncId at `at` — in ONE atomic write. Nothing
   * survives; the next mount necessarily decides Snapshot.
   *
   * This is NOT expressible as prune-then-set: `prune` records destruction (it advances
   * {@link highestPrunedSyncId}), but those boundaries are coordinates on the dead
   * timeline — poison on the new one, forcing `Snapshot` forever. The epoch reset must
   * destroy the record of destruction too. And it must be one write: a partial reset
   * (log wiped but an old-epoch last-ingested mark surviving, or vice versa) would
   * recreate exactly the inconsistency this method exists to end.
   */
  readonly resetToEpoch: (args: { readonly epoch: Epoch; readonly at: SyncId }) => Effect.Effect<void>
}

// ── Record keys — the frozen physical encodings ──
// The spellings predate the honest method names ("cursor", "wm:") and are only ever
// built and looked up, never parsed back; changing them would orphan every existing
// client's journal (a silent global reset), so they stay.

const lastAppliedKey = (key: CollectionKey<unknown>): string => `${LAST_APPLIED_PREFIX}${serializeKey(key)}`
const prunedKey = (modelName: string): string => `floor:${modelName}`
const RESYNC_KEY = "lastResync"
const EPOCH_KEY = "epoch"
const LAST_INGESTED_KEY = "cursor"

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

const StoredEvents = Schema.Array(JournalEvent)

// The on-disk name predates the SyncJournal rename; changing it would orphan every
// existing client's journal (a silent global reset), so it stays "eventlog".
const DEFAULT_DATABASE_NAME = "live-collection-eventlog"

/**
 * The policy layer — every codec, fold rule, and orchestration, written ONCE over the
 * {@link JournalStore} port. The port's contract carries what the policy relies on:
 * single-writer read-fold-commit sequences (sequential under broker ingest) and
 * all-or-nothing `commit`s. Store faults are defects; the error channel stays empty.
 */
const makeSyncJournal = (store: JournalStore): SyncJournalShape => {
  const decodeRows = (raw: ReadonlyArray<unknown>): Effect.Effect<ReadonlyArray<JournalEvent>> =>
    Schema.decodeUnknownEffect(StoredEvents)(raw).pipe(Effect.orDie)

  const recordSyncId = (key: string): Effect.Effect<Option.Option<SyncId>> =>
    store.record(key).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (value) => Schema.decodeUnknownEffect(SyncId)(value).pipe(Effect.asSome),
        }),
      ),
      Effect.orDie,
    )

  // Monotonic write: read the current mark, keep the larger, persist. (Sequential under broker ingest.)
  const advanceRecordSyncId = (key: string, at: SyncId): Effect.Effect<void> =>
    recordSyncId(key).pipe(
      Effect.flatMap((current) => store.commit(JournalWrite.Patch({ putRecords: [[key, advanceSyncId(current, at)]] }))),
    )

  const readLastAppliedRecords: Effect.Effect<ReadonlyArray<LastAppliedRecord>> = store.lastAppliedRecords.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(LastAppliedRecord))),
    Effect.orDie,
  )

  const getLastAppliedRecord = (key: CollectionKey<unknown>): Effect.Effect<Option.Option<LastAppliedRecord>> =>
    store.record(lastAppliedKey(key)).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (value) => Schema.decodeUnknownEffect(LastAppliedRecord)(value).pipe(Effect.asSome),
        }),
      ),
      Effect.orDie,
    )

  return {
    append: (incoming) =>
      Schema.encodeEffect(StoredEvents)(incoming).pipe(
        Effect.orDie,
        Effect.flatMap((stored) =>
          store.commit(
            JournalWrite.Patch({
              putLog: incoming.map((row, index) => ({
                syncId: row.syncId,
                modelName: row.modelName,
                value: stored[index],
              })),
            }),
          ),
        ),
      ),

    read: ({ modelName, since }) =>
      store.logByModel(modelName).pipe(
        Effect.flatMap(decodeRows),
        Effect.map((rows) =>
          rows.filter((r) => compareSyncId(r.syncId, since) > 0).sort((a, b) => compareSyncId(a.syncId, b.syncId)),
        ),
      ),

    prune: ({ maxEventsPerModel, maxEventsTotal }) =>
      Effect.all([store.logAll.pipe(Effect.flatMap(decodeRows)), readLastAppliedRecords]).pipe(
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
          // Merge each model's max deleted syncId into the current boundary (monotonic),
          // then land deletions + boundaries in ONE commit.
          return Effect.forEach([...plan.maxDeletedSyncId], ([model, at]) =>
            recordSyncId(prunedKey(model)).pipe(
              Effect.map((current) => [prunedKey(model), advanceSyncId(current, at)] as const),
            ),
          ).pipe(
            Effect.flatMap((boundaries) =>
              store.commit(
                JournalWrite.Patch({ deleteLog: deleted.map((r) => r.syncId), putRecords: boundaries }),
              ),
            ),
          )
        }),
      ),

    highestPrunedSyncId: (modelName) => recordSyncId(prunedKey(modelName)),

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
          store.commit(
            JournalWrite.Patch({
              putRecords: [[lastAppliedKey(key), foldLastApplied(current, { entity: key.entity, schemaVersion, at })]],
            }),
          ),
        ),
      ),

    getLastResync: recordSyncId(RESYNC_KEY),
    setLastResync: (at) => advanceRecordSyncId(RESYNC_KEY, at),
    getLastIngestedSyncId: recordSyncId(LAST_INGESTED_KEY),
    setLastIngestedSyncId: (id) => advanceRecordSyncId(LAST_INGESTED_KEY, id),

    getEpoch: store.record(EPOCH_KEY).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (value) => Schema.decodeUnknownEffect(Epoch)(value).pipe(Effect.asSome),
        }),
      ),
      Effect.orDie,
    ),
    setEpoch: (epoch) => store.commit(JournalWrite.Patch({ putRecords: [[EPOCH_KEY, epoch]] })),

    // One atomic Reset: nothing survives, the new identity and position land together.
    resetToEpoch: ({ epoch, at }) =>
      store.commit(
        JournalWrite.Reset({
          records: [
            [EPOCH_KEY, epoch],
            [LAST_INGESTED_KEY, at],
          ],
        }),
      ),
  }
}

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
  /** In-memory (`Ref`-backed), non-durable — for tests and SSR. Same policy layer as production. */
  static readonly layerMemory: Layer.Layer<SyncJournal> = Layer.effect(
    SyncJournal,
    Effect.map(makeMemoryStore, makeSyncJournal),
  )
  /** Browser default: durable IndexedDB. Opens (and closes on scope-out) `databaseName` ?? `"live-collection-eventlog"`. */
  static readonly layer = (options?: { readonly databaseName?: string }): Layer.Layer<SyncJournal> =>
    Layer.effect(
      SyncJournal,
      Effect.map(makeIdbStore(options?.databaseName ?? DEFAULT_DATABASE_NAME), makeSyncJournal),
    )
}
