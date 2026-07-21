import { type Duration, Effect, Option, Ref, type Scope } from "effect"
import { maxSyncId, type SyncId } from "@triargos/live-collection-protocol"
import type { SchemaVersion } from "../core/schema-version.js"
import { type CollectionKey, serializeKey } from "../core/collection-key.js"
import type { SyncJournalShape } from "./sync-journal.js"

/**
 * The ACK machine — the single authority on each collection's last-applied syncId.
 *
 * `markApplied` is called per applied signal, so writing the journal each time would
 * mean one durable transaction per event. Instead marks accumulate in memory (merged
 * monotonically per `(key, schemaVersion)`) and a scoped fiber flushes them every
 * `flushEvery`; a finalizer flushes on shutdown. `current` merges the durable record
 * with the pending one, so readers never observe the batching delay.
 */
export interface LastAppliedTracker {
  /** Record that a subscriber has applied every relevant signal through `through`. */
  readonly markApplied: (args: {
    readonly key: CollectionKey<unknown>
    readonly schemaVersion: SchemaVersion
    readonly through: SyncId
  }) => Effect.Effect<void>
  /** max(durable, pending) — the only way to read a collection's last-applied syncId. */
  readonly current: (args: {
    readonly key: CollectionKey<unknown>
    readonly schemaVersion: SchemaVersion
  }) => Effect.Effect<Option.Option<SyncId>>
  /**
   * Drop all pending marks un-flushed. Epoch reset only: an old-epoch mark must
   * never reach the journal after the wipe — call this BEFORE `journal.resetToEpoch`.
   */
  readonly clear: Effect.Effect<void>
  /**
   * Write every pending mark to the journal now, ahead of the timer. The trim tick
   * calls this before `journal.prune` so dead-weight pruning sees fresh marks — a
   * stale mark is only ever conservative (prunes less), but fresh ones prune fully.
   */
  readonly flush: Effect.Effect<void>
}

type PendingLastApplied = {
  readonly key: CollectionKey<unknown>
  readonly schemaVersion: SchemaVersion
  readonly at: SyncId
}

const pendingId = (key: CollectionKey<unknown>, schemaVersion: SchemaVersion): string =>
  `${serializeKey(key)}:${schemaVersion}`

export const makeLastAppliedTracker = (deps: {
  readonly journal: SyncJournalShape
  readonly flushEvery: Duration.Input
}): Effect.Effect<LastAppliedTracker, never, Scope.Scope> =>
  Effect.gen(function* () {
    const pending = yield* Ref.make(new Map<string, PendingLastApplied>())

    const flush = Effect.uninterruptible(
      Ref.modify(pending, (current) => [[...current.values()], new Map<string, PendingLastApplied>()] as const).pipe(
        Effect.flatMap((marks) =>
          Effect.forEach(
            marks,
            ({ key, schemaVersion, at }) => deps.journal.setCollectionLastAppliedSyncId({ key, schemaVersion, at }),
            { discard: true },
          ),
        ),
      ),
    )

    yield* Effect.addFinalizer(() => flush)
    yield* Effect.sleep(deps.flushEvery).pipe(Effect.andThen(flush), Effect.forever, Effect.forkScoped)

    return {
      markApplied: ({ key, schemaVersion, through }) =>
        Ref.update(pending, (current) => {
          const next = new Map(current)
          const id = pendingId(key, schemaVersion)
          const existing = next.get(id)
          next.set(id, {
            key,
            schemaVersion,
            at: existing === undefined ? through : maxSyncId(existing.at, through),
          })
          return next
        }),

      current: ({ key, schemaVersion }) =>
        Effect.gen(function* () {
          const durable = yield* deps.journal.getCollectionLastAppliedSyncId({ key, schemaVersion })
          const inFlight = yield* Ref.get(pending).pipe(
            Effect.map((marks) => Option.fromNullishOr(marks.get(pendingId(key, schemaVersion))?.at)),
          )
          return Option.match(inFlight, {
            onNone: () => durable,
            onSome: (at) =>
              Option.some(Option.match(durable, { onNone: () => at, onSome: (d) => maxSyncId(d, at) })),
          })
        }),

      clear: Ref.set(pending, new Map<string, PendingLastApplied>()),

      flush,
    }
  })
