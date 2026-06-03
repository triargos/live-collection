import { Effect, type Schema, type Scope } from "effect"
import { createCollection } from "@tanstack/db"
import { persistedCollectionOptions } from "@tanstack/db-sqlite-persistence-core"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "../dispatch/sync-write.js"
import { PersistenceBase } from "./persistence-base.js"
import type { LiveCollection } from "./live-collection.js"
import { deriveSchemaVersion } from "./schema-version.js"
import { makeSyncWrite, type SyncSession } from "./sync-session.js"

/**
 * The per-entity collection factory — the body of `defineCollection`'s `make`. Builds a
 * {@link LiveCollection} over the {@link PersistenceBase}, registering its own teardown (so `R`
 * carries `Scope`). The TanStack alpha — `createCollection`, `persistedCollectionOptions`, the
 * `sync` begin/write/commit loop, the durable base — is entirely hidden here (DESIGN §2).
 *
 * In this pass the `sync` config is network-free (DEC-A5): the persistence layer hydrates the
 * in-memory store from storage on start (eager `syncMode`, internal to the library), and our `sync`
 * only installs the sync-session holder and signals ready. Catchup + SSE arrive in the transport
 * pass (DEC-A12).
 */
export const effectCollectionOptions = <T extends object>(args: {
  /** Stable, unique-per-`(entity, scope)` id for the SQLite table + TanStack collection id.
   *  Injected by `defineCollection` (DEC-A3) — the app never hand-builds it. */
  readonly collectionId: string
  /** The entity schema — used **only** to derive `schemaVersion` (DEC-A6 dump-and-rebuild) and to
   *  infer `T`. It is *not* handed to TanStack and performs no validation (DEC-A1/A4): decoding
   *  already happens at the dispatch seam. */
  readonly schema: Schema.Schema<T, any, never>
  /** `T` → its row key. */
  readonly getKey: (entity: T) => ModelId
}): Effect.Effect<LiveCollection<T>, never, PersistenceBase | Scope.Scope> =>
  Effect.gen(function* () {
    const { syncWrite, provide } = yield* makeSyncWrite<T>()
    const { persistence } = yield* PersistenceBase

    const collection = createCollection(
      persistedCollectionOptions<T, ModelId, never, SyncWrite<T>>({
        id: args.collectionId,
        getKey: args.getKey,
        schemaVersion: deriveSchemaVersion(args.schema),
        syncMode: "eager", // load the full persisted base on start, not query-driven (DESIGN §, DEC-A5)
        startSync: true, // start sync on mount → session captured + hydration runs
        gcTime: Infinity, // DEC-A10 — registry is the sole GC; sync never tears down while mounted
        utils: syncWrite,
        persistence,
        sync: {
          sync: (params) => {
            const session: SyncSession<T> = {
              upsert: (entity) => {
                params.begin()
                params.write({ type: "update", value: entity })
                params.commit()
              },
              remove: (id) => {
                params.begin()
                params.write({ type: "delete", key: id })
                params.commit()
              },
            }
            provide(session)
            params.markReady() // wrapper defers this until internal hydration completes
          },
        },
      }),
    )

    yield* Effect.addFinalizer(() => Effect.promise(() => collection.cleanup())) // the only GC
    return collection satisfies LiveCollection<T>
  })
