import { Deferred, Effect, Exit } from "effect"
import type { SyncAppliedReceipt } from "@tanstack/db"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "./sync-write.js"

/**
 * The live handle into a started collection's synced-write path — the `begin/write/commit` trio the
 * `sync` closure captures. Each operation is one sync transaction and returns its commit receipt:
 * `true` when applied synchronously, otherwise a promise that settles once the rows are visible and
 * the persisted wrapper has written them to SQLite.
 */
export interface SyncSession<T> {
  /** The collection id, for failure logs. */
  readonly collectionId: string
  /** Upsert one entity into the synced store (insert if absent, replace if present). */
  readonly upsert: (entity: T) => SyncAppliedReceipt
  /** Remove the entity with `id` from the synced store. */
  readonly remove: (id: ModelId) => SyncAppliedReceipt
  /** Replace the whole synced store with `rows` — one transaction: truncate, then write each row. */
  readonly replace: (rows: ReadonlyArray<T>) => SyncAppliedReceipt
  /** Delete + upsert in one transaction — the subset slice replace. */
  readonly patch: (args: {
    readonly deleteKeys: ReadonlyArray<ModelId>
    readonly rows: ReadonlyArray<T>
  }) => SyncAppliedReceipt
}

/**
 * The fire-and-forget reconcile path for mutation handlers. A handler must not await its receipt:
 * TanStack holds a sync transaction committed while a mutation persists until that mutation settles,
 * so awaiting it from inside the handler deadlocks. A lost reconcile write self-heals — the server's
 * own event for the row reaches the drain through normal sync.
 */
export interface ReconcileWrite<T> {
  readonly writeSynced: (entity: T) => Effect.Effect<void>
  readonly deleteSynced: (id: ModelId) => Effect.Effect<void>
}

const isAbortError = (error: unknown): boolean => error instanceof Error && error.name === "AbortError"

/**
 * Waits for a receipt so a caller resumes only once the write is durable. `AbortError` means
 * `cleanup()` dropped the transaction before it applied — the write did not happen, so the caller is
 * interrupted rather than resumed (the broker must not ack it). Any other rejection is a durability
 * failure that fail-stops the collection; it surfaces as a defect.
 */
const awaitReceipt = (receipt: SyncAppliedReceipt): Effect.Effect<void> =>
  receipt === true
    ? Effect.void
    : Effect.promise(() =>
        receipt.then(
          () => "applied" as const,
          (error: unknown) => (isAbortError(error) ? ("aborted" as const) : Promise.reject(error)),
        ),
      ).pipe(Effect.flatMap((outcome) => (outcome === "aborted" ? Effect.interrupt : Effect.void)))

/**
 * Surfaces a failed receipt nobody awaits. `AbortError` (cleanup dropped the transaction) is expected
 * on this path; anything else would otherwise be a silent lost write.
 */
const warnOnFailedReceipt = (collectionId: string, receipt: SyncAppliedReceipt): void => {
  if (receipt === true) return
  receipt.catch((error: unknown) => {
    if (isAbortError(error)) return
    Effect.runFork(
      Effect.logWarning(`[liveCollection] sync transaction for "${collectionId}" failed to persist`, error),
    )
  })
}

/**
 * Builds the utils-hosted {@link SyncWrite}, the handler-side {@link ReconcileWrite}, and the
 * `provide` the `sync` closure calls once the collection is first ready (hydrated). Both write paths
 * are constructed at config time, but the session is only handed out after hydration — so a one-shot
 * `Deferred` bridges them: a write issued before then simply waits. Sound because the collection is kept alive with
 * `gcTime: Infinity`, so `sync()` is captured exactly once and never restarts.
 */
export const makeSyncWrite = <T>(): Effect.Effect<{
  readonly syncWrite: SyncWrite<T>
  readonly reconcileWrite: ReconcileWrite<T>
  readonly provide: (session: SyncSession<T>) => void
}> =>
  Effect.gen(function* () {
    const session = yield* Deferred.make<SyncSession<T>>()
    const durable = (run: (s: SyncSession<T>) => SyncAppliedReceipt): Effect.Effect<void> =>
      Deferred.await(session).pipe(Effect.flatMap((s) => awaitReceipt(run(s))))
    const fire = (run: (s: SyncSession<T>) => SyncAppliedReceipt): Effect.Effect<void> =>
      Deferred.await(session).pipe(Effect.map((s) => warnOnFailedReceipt(s.collectionId, run(s))))
    const syncWrite: SyncWrite<T> = {
      writeSynced: (entity) => durable((s) => s.upsert(entity)),
      deleteSynced: (id) => durable((s) => s.remove(id)),
      replaceSynced: (rows) => durable((s) => s.replace(rows)),
      patchSynced: (args) => durable((s) => s.patch(args)),
    }
    const reconcileWrite: ReconcileWrite<T> = {
      writeSynced: (entity) => fire((s) => s.upsert(entity)),
      deleteSynced: (id) => fire((s) => s.remove(id)),
    }
    const provide = (s: SyncSession<T>): void => {
      Deferred.doneUnsafe(session, Exit.succeed(s))
    }
    return { syncWrite, reconcileWrite, provide }
  })
