import { Data, Effect, Option, Ref, type Scope } from "effect"
import { type DBSchema, openDB } from "idb"

/**
 * A log row as the store shelves it: opaque encoded value + the two keys it's found
 * by. The store never looks inside `value` — all codecs live in the policy layer
 * (`makeSyncJournal`). CONTRACT: `syncId` and `modelName` MUST equal the same-named
 * fields inside `value` — a store may index via the explicit fields (memory) or via
 * an in-value keyPath (IndexedDB); both must find the same row.
 */
export interface StoredLogRow {
  /** Primary key — put = upsert = reconnect-overlap dedupe. */
  readonly syncId: string
  /** Secondary index — replay reads narrow by model. */
  readonly modelName: string
  /** Schema-encoded `JournalEvent`; opaque to the store. */
  readonly value: unknown
}

/**
 * One atomic write — everything in one descriptor lands together or not at all.
 *
 * - `Patch` — normal operation: append/prune log rows, advance bookkeeping records.
 * - `Reset` — `resetToEpoch`, structurally: wipe the log + ALL records, then install
 *   exactly these records (the new epoch + lastIngested). Nothing else survives.
 */
export type JournalWrite = Data.TaggedEnum<{
  Patch: {
    readonly putLog?: ReadonlyArray<StoredLogRow>
    /** By `syncId` (prune stage-3 deletions). */
    readonly deleteLog?: ReadonlyArray<string>
    readonly putRecords?: ReadonlyArray<readonly [key: string, value: unknown]>
  }
  Reset: { readonly records: ReadonlyArray<readonly [key: string, value: unknown]> }
}>
export const JournalWrite = Data.taggedEnum<JournalWrite>()

/**
 * Key prefix identifying collection last-applied records — part of the port's contract:
 * `lastAppliedRecords` returns exactly the records whose key carries this prefix. The
 * "wm:" spelling predates the last-applied rename and is frozen on disk; keys are only
 * ever built and range-scanned, never parsed back.
 */
export const LAST_APPLIED_PREFIX = "wm:"

/**
 * The journal's storage port: dumb keyed durability + one atomic multi-write. Two
 * shelves — the **log** (rows keyed by syncId, indexed by modelName) and **records**
 * (out-of-line string keys). All values are opaque; the policy layer owns every codec
 * and every fold rule.
 *
 * CONTRACT — single writer: callers (the policy layer) perform read-fold-commit
 * sequences without store-side concurrency control; the broker guarantees writes are
 * sequential (one ingest fiber; ack flushes serialized behind it).
 *
 * CONTRACT — atomicity: one `commit` lands entirely or not at all. A partial `Reset`
 * (log wiped but an old-epoch record surviving, or vice versa) would recreate exactly
 * the inconsistency `resetToEpoch` exists to end.
 */
export interface JournalStore {
  readonly commit: (write: JournalWrite) => Effect.Effect<void>
  // ── log reads (values opaque; the policy layer decodes) ──
  readonly logByModel: (modelName: string) => Effect.Effect<ReadonlyArray<unknown>>
  /** Prune's full scan — the log is cap-bounded, so this stays small. */
  readonly logAll: Effect.Effect<ReadonlyArray<unknown>>
  // ── record reads ──
  readonly record: (key: string) => Effect.Effect<Option.Option<unknown>>
  /**
   * Every collection last-applied record (keys under {@link LAST_APPLIED_PREFIX}) —
   * prune stage-2's input. How it's answered (prefix range, filter) is the store's
   * business.
   */
  readonly lastAppliedRecords: Effect.Effect<ReadonlyArray<unknown>>
}

// ── In-memory store — broker behavior tests and SSR ──

interface MemoryState {
  readonly log: ReadonlyMap<string, StoredLogRow>
  readonly records: ReadonlyMap<string, unknown>
}

/** One `Ref` holding both shelves ⇒ every `commit` is trivially atomic. */
export const makeMemoryStore: Effect.Effect<JournalStore> = Effect.gen(function* () {
  const state = yield* Ref.make<MemoryState>({ log: new Map(), records: new Map() })

  const applyWrite = (current: MemoryState, write: JournalWrite): MemoryState =>
    JournalWrite.$match(write, {
      Patch: ({ putLog = [], deleteLog = [], putRecords = [] }) => {
        const log = new Map(current.log)
        for (const syncId of deleteLog) log.delete(syncId)
        for (const row of putLog) log.set(row.syncId, row)
        const records = new Map(current.records)
        for (const [key, value] of putRecords) records.set(key, value)
        return { log, records }
      },
      Reset: ({ records }) => ({ log: new Map(), records: new Map(records) }),
    })

  return {
    commit: (write) => Ref.update(state, (current) => applyWrite(current, write)),
    logByModel: (modelName) =>
      Ref.get(state).pipe(
        Effect.map(({ log }) => [...log.values()].filter((row) => row.modelName === modelName).map((row) => row.value)),
      ),
    logAll: Ref.get(state).pipe(Effect.map(({ log }) => [...log.values()].map((row) => row.value))),
    record: (key) =>
      Ref.get(state).pipe(
        Effect.map(({ records }) => (records.has(key) ? Option.some(records.get(key)) : Option.none())),
      ),
    lastAppliedRecords: Ref.get(state).pipe(
      Effect.map(({ records }) =>
        [...records.entries()].filter(([key]) => key.startsWith(LAST_APPLIED_PREFIX)).map(([, value]) => value),
      ),
    ),
  }
})

// ── IndexedDB store — the durable home that survives reload/workspace-switch ──

const EVENTS = "events" // object store: log rows, keyed by `syncId` (in-value keyPath), indexed by `modelName`
const BY_MODEL = "byModel" // index on `events.modelName` — the replay read narrows to one model first
const META = "meta" // keyval object store (out-of-line keys): the journal's bookkeeping records

interface JournalDbSchema extends DBSchema {
  [EVENTS]: {
    key: string
    value: unknown
    indexes: { [BY_MODEL]: string }
  }
  [META]: {
    key: string
    value: unknown
  }
}

/**
 * The durable store. Every `commit` is ONE `readwrite` transaction over both object
 * stores — atomicity is structural, not a discipline. IDB orders string keys
 * *lexicographically*, but syncIds order by *magnitude*, so this never range-scans on
 * the `syncId` key: log reads narrow with the `modelName` index (or read the whole —
 * cap-bounded — store) and the policy layer filters/sorts by magnitude. Driver faults
 * are defects (`Effect.promise` dies on rejection); the error channel stays empty.
 */
export const makeIdbStore = (databaseName: string): Effect.Effect<JournalStore, never, Scope.Scope> =>
  Effect.gen(function* () {
    const db = yield* Effect.acquireRelease(
      Effect.promise(() =>
        openDB<JournalDbSchema>(databaseName, 1, {
          upgrade(db) {
            db.createObjectStore(EVENTS, { keyPath: "syncId" }).createIndex(BY_MODEL, "modelName", { unique: false })
            db.createObjectStore(META)
          },
        }),
      ),
      (db) => Effect.sync(() => db.close()),
    )

    return {
      commit: (write) =>
        JournalWrite.$match(write, {
          Patch: ({ putLog = [], deleteLog = [], putRecords = [] }) => {
            const tx = db.transaction([EVENTS, META], "readwrite")
            const events = tx.objectStore(EVENTS)
            const meta = tx.objectStore(META)
            return Effect.promise(() =>
              Promise.all([
                ...deleteLog.map((syncId) => events.delete(syncId)),
                // put = upsert by the `syncId` keyPath inside `value` ⇒ dedupe
                ...putLog.map((row) => events.put(row.value)),
                ...putRecords.map(([key, value]) => meta.put(value, key)),
                tx.done,
              ]),
            ).pipe(Effect.asVoid)
          },
          Reset: ({ records }) =>
            Effect.gen(function* () {
              const tx = db.transaction([EVENTS, META], "readwrite")
              const meta = tx.objectStore(META)
              yield* Effect.promise(() => tx.objectStore(EVENTS).clear())
              yield* Effect.promise(() => meta.clear())
              yield* Effect.promise(() =>
                Promise.all([...records.map(([key, value]) => meta.put(value, key)), tx.done]),
              )
            }),
        }),

      logByModel: (modelName) => Effect.promise(() => db.getAllFromIndex(EVENTS, BY_MODEL, modelName)),
      logAll: Effect.promise(() => db.getAll(EVENTS)),
      record: (key) =>
        Effect.promise(() => db.get(META, key)).pipe(
          Effect.map((value) => (value === undefined ? Option.none() : Option.some(value))),
        ),
      lastAppliedRecords: Effect.promise(() =>
        db.getAll(META, IDBKeyRange.bound(LAST_APPLIED_PREFIX, `${LAST_APPLIED_PREFIX}\uffff`)),
      ),
    }
  })
