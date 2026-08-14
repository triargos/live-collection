import { Effect, Option, Ref, Schema } from "effect"
import { compareSyncId, maxSyncId, type ModelId, type SyncId } from "@triargos/live-collection-protocol"
import type { SubsetKey } from "./core/collection-key.js"
import type { SyncSignal } from "./client/sync-signal.js"
import { SyncSignal as Signal } from "./client/sync-signal.js"

/**
 * A partial collection's in-memory coverage: `indexKey → keyValue → mark`, seeded from
 * the durable subset marks on mount and advanced per applied signal. It is both the
 * drain's membership filter (an event lands only when some index covers its extracted
 * value) and the per-subset tail guard (an event at or below a subset's mark is
 * already reflected by that subset's rows — drop, don't re-apply).
 */
export type CoverageState = ReadonlyMap<string, ReadonlyMap<string, SyncId>>

export const emptyCoverage: CoverageState = new Map()

export const seedCoverage = (
  marks: ReadonlyArray<{ readonly subset: SubsetKey; readonly at: SyncId }>,
): CoverageState => {
  const state = new Map<string, Map<string, SyncId>>()
  for (const { subset, at } of marks) {
    const values = state.get(subset.indexKey) ?? new Map<string, SyncId>()
    values.set(subset.keyValue, at)
    state.set(subset.indexKey, values)
  }
  return state
}

/**
 * Per-pair-max union — the mount hydration merge. The drain seeds durable marks INTO
 * the live map rather than replacing it, so an ensure that activated a subset while
 * hydration was still reading can never be wiped by it.
 */
export const mergeCoverage = (a: CoverageState, b: CoverageState): CoverageState => {
  const merged = new Map<string, Map<string, SyncId>>()
  for (const state of [a, b]) {
    for (const [indexKey, values] of state) {
      const target = merged.get(indexKey) ?? new Map<string, SyncId>()
      for (const [keyValue, mark] of values) {
        const current = target.get(keyValue)
        target.set(keyValue, current === undefined ? mark : maxSyncId(current, mark))
      }
      merged.set(indexKey, target)
    }
  }
  return merged
}

/** Every covered subset — what an applied signal proves current through its syncId. */
export const coveredSubsets = (state: CoverageState): ReadonlyArray<SubsetKey> =>
  [...state.entries()].flatMap(([indexKey, values]) =>
    [...values.keys()].map((keyValue) => ({ indexKey, keyValue })),
  )

/** Monotonic per-subset advance — a slice stamp or an applied signal can never regress a mark. */
const advance = (state: CoverageState, subset: SubsetKey, at: SyncId): CoverageState => {
  const values = new Map(state.get(subset.indexKey) ?? [])
  const current = values.get(subset.keyValue)
  values.set(subset.keyValue, current === undefined ? at : maxSyncId(current, at))
  return new Map(state).set(subset.indexKey, values)
}

/** Advance every covered subset to `at` — the per-signal ack's in-memory mirror. */
const advanceAll = (state: CoverageState, at: SyncId): CoverageState =>
  new Map(
    [...state.entries()].map(([indexKey, values]) => [
      indexKey,
      new Map([...values.entries()].map(([keyValue, mark]) => [keyValue, maxSyncId(mark, at)])),
    ]),
  )

/** The slice of a collection the applier writes through. */
export interface PartialWrite<T> {
  readonly has: (id: ModelId) => boolean
  readonly currentRows: () => IterableIterator<T>
  readonly writeSynced: (row: T) => Effect.Effect<void>
  readonly deleteSynced: (id: ModelId) => Effect.Effect<void>
  readonly replaceSynced: (rows: ReadonlyArray<T>) => Effect.Effect<void>
}

/**
 * The partial drain's signal application — one function shared by the live drain and
 * the ensure's replay callbacks, so both paths follow the same rules:
 *
 * - `Upsert`, freshly covered (some index covers the row's value with `syncId` above
 *   that subset's mark) ⇒ write.
 * - `Upsert`, covered but stale (`syncId ≤` every covering mark) ⇒ drop — the subset's
 *   rows already reflect a newer server state (the mid-ensure interleaving guard).
 * - `Upsert`, uncovered ⇒ delete any local row with that key (delete-on-mismatch: the
 *   row moved out of every covered subset) — else drop.
 * - `Delete` ⇒ remove if present.
 * - `Snapshot` ⇒ local state older than `at` is untrusted (resync / epoch reset /
 *   cold attach): drop every subset whose mark is **strictly below** `at` and every
 *   row not covered by a surviving subset. A subset marked at or above `at` keeps its
 *   rows — its slice is provably at least as fresh as the snapshot point, which is
 *   what lets a first ensure race the cold-attach Snapshot (at 0) without losing its
 *   just-landed slice. Wiped subsets refetch on their next ensure, decided from the
 *   durable metadata.
 *
 * Returns the covered subsets whose marks the broker should advance through this
 * signal's syncId (empty for `Snapshot`), mirroring the same advance in-memory.
 *
 * Callers MUST hold the collection's application gate — response slices and live
 * events never interleave mid-apply.
 */
export const makePartialApplier = <T extends object>(deps: {
  readonly entity: string
  readonly by: Record<string, (entity: T) => string>
  readonly getKey: (entity: T) => ModelId
  readonly decode: (data: unknown) => Effect.Effect<T, Schema.SchemaError>
  readonly write: PartialWrite<T>
  readonly coverage: Ref.Ref<CoverageState>
}): ((signal: SyncSignal) => Effect.Effect<ReadonlyArray<SubsetKey>>) => {
  const { entity, by, getKey, decode, write, coverage } = deps

  const ackAndAdvance = (at: SyncId): Effect.Effect<ReadonlyArray<SubsetKey>> =>
    Ref.modify(coverage, (state) => [coveredSubsets(state), advanceAll(state, at)] as const)

  return Signal.$match({
    Snapshot: ({ at }) =>
      Effect.gen(function* () {
        const state = yield* Ref.get(coverage)
        const kept = new Map(
          [...state.entries()].flatMap(([indexKey, values]) => {
            const fresh = [...values.entries()].filter(([, mark]) => compareSyncId(mark, at) >= 0)
            return fresh.length === 0 ? [] : [[indexKey, new Map(fresh)] as const]
          }),
        )
        const size = (s: CoverageState | ReadonlyMap<string, ReadonlyMap<string, SyncId>>): number =>
          [...s.values()].reduce((n, values) => n + values.size, 0)
        if (size(kept) === size(state) && size(state) > 0) return [] as ReadonlyArray<SubsetKey> // nothing invalidated
        const covered = (row: T): boolean =>
          [...kept.entries()].some(([indexKey, values]) => {
            const extractor = by[indexKey]
            return extractor !== undefined && values.has(extractor(row))
          })
        yield* write.replaceSynced([...write.currentRows()].filter(covered))
        yield* Ref.set(coverage, kept)
        return [] as ReadonlyArray<SubsetKey>
      }),

    Delete: ({ syncId, modelId }) =>
      write.deleteSynced(modelId).pipe(Effect.andThen(ackAndAdvance(syncId))),

    Upsert: ({ syncId, data }) =>
      decode(data).pipe(
        Effect.flatMap((row) =>
          Effect.gen(function* () {
            const state = yield* Ref.get(coverage)
            let fresh = false
            let coveredStale = false
            for (const [indexKey, extractor] of Object.entries(by)) {
              const mark = state.get(indexKey)?.get(extractor(row))
              if (mark === undefined) continue
              if (compareSyncId(syncId, mark) > 0) fresh = true
              else coveredStale = true
            }
            if (fresh) yield* write.writeSynced(row)
            else if (!coveredStale && write.has(getKey(row))) yield* write.deleteSynced(getKey(row))
            return yield* ackAndAdvance(syncId)
          }),
        ),
        Effect.catchTag("SchemaError", (error) =>
          Effect.logWarning(
            `[defineCollection] skipping undecodable ${entity} event #${syncId}: ${error.message}`,
          ).pipe(Effect.andThen(ackAndAdvance(syncId))),
        ),
      ),
  })
}

/**
 * Land one fetched subset slice: decode the wire rows, delete every local row that
 * still claims this subset but vanished from the fetched membership, upsert the rest
 * in ONE synced transaction, and activate the subset's coverage at the stamp. An
 * undecodable row is logged and skipped — the mark still activates (matching the
 * drain's skip rule).
 *
 * Caller MUST hold the application gate.
 */
export const applySlice = <T extends object>(deps: {
  readonly entity: string
  readonly extractor: (entity: T) => string
  readonly getKey: (entity: T) => ModelId
  readonly decode: (data: unknown) => Effect.Effect<T, Schema.SchemaError>
  readonly currentRows: () => IterableIterator<T>
  readonly patchSynced: (args: {
    readonly deleteKeys: ReadonlyArray<ModelId>
    readonly rows: ReadonlyArray<T>
  }) => Effect.Effect<void>
  readonly coverage: Ref.Ref<CoverageState>
  readonly subset: SubsetKey
  readonly rows: ReadonlyArray<unknown>
  readonly at: SyncId
}): Effect.Effect<void> =>
  Effect.gen(function* () {
    const decoded = yield* Effect.forEach(deps.rows, (raw) =>
      deps.decode(raw).pipe(
        Effect.map(Option.some),
        Effect.catchTag("SchemaError", (error) =>
          Effect.logWarning(
            `[defineCollection] skipping undecodable ${deps.entity} slice row: ${error.message}`,
          ).pipe(Effect.as(Option.none<T>())),
        ),
      ),
    )
    const rows = decoded.flatMap(Option.toArray)
    const fetchedKeys = new Set(rows.map(deps.getKey))
    const deleteKeys = [...deps.currentRows()]
      .filter((row) => deps.extractor(row) === deps.subset.keyValue && !fetchedKeys.has(deps.getKey(row)))
      .map(deps.getKey)
    yield* deps.patchSynced({ deleteKeys, rows })
    yield* Ref.update(deps.coverage, (state) => advance(state, deps.subset, deps.at))
  })
