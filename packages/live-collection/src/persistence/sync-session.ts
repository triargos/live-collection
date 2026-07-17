import { Deferred, Effect, Exit } from "effect"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "../dispatch/sync-write.js"

/**
 * The live handle into a started collection's synced-write path — the `begin/write/commit` trio the
 * `sync` closure captures, wrapped as the two operations our write path needs.
 */
export interface SyncSession<T> {
  /** Upsert one entity into the synced store (insert if absent, replace if present). */
  readonly upsert: (entity: T) => void
  /** Remove the entity with `id` from the synced store. */
  readonly remove: (id: ModelId) => void
  /** Replace the whole synced store with `rows` — one transaction: truncate, then write each row. */
  readonly replace: (rows: ReadonlyArray<T>) => void
}

/**
 * Builds the utils-hosted {@link SyncWrite} and the `provide` the `sync` closure calls once on
 * start. `utils.writeSynced`/`deleteSynced` are constructed at config time, but the session only
 * exists once `sync()` runs — so a one-shot `Deferred` bridges them: a write issued before the
 * session is provided simply waits. Sound because the collection is kept alive with
 * `gcTime: Infinity`, so `sync()` is captured exactly once and never restarts.
 */
export const makeSyncWrite = <T>(): Effect.Effect<{
  readonly syncWrite: SyncWrite<T>
  readonly provide: (session: SyncSession<T>) => void
}> =>
  Effect.gen(function* () {
    const session = yield* Deferred.make<SyncSession<T>>()
    const syncWrite: SyncWrite<T> = {
      writeSynced: (entity) => Deferred.await(session).pipe(Effect.map((s) => s.upsert(entity))),
      deleteSynced: (id) => Deferred.await(session).pipe(Effect.map((s) => s.remove(id))),
      replaceSynced: (rows) => Deferred.await(session).pipe(Effect.map((s) => s.replace(rows))),
    }
    const provide = (s: SyncSession<T>): void => {
      Deferred.doneUnsafe(session, Exit.succeed(s))
    }
    return { syncWrite, provide }
  })
