import type { Effect } from "effect"
import type { ModelId } from "@triargos/live-collection-protocol"

/**
 * The synced-store write path of a collection, hosted on `collection.utils`: how
 * server-authoritative state is applied to the local baseline. This is distinct from the
 * optimistic-mutation path the UI writes through (`collection.insert/update/delete`) —
 * synced writes reflect confirmed server truth and are never rolled back.
 *
 * The collection drain is the primary caller, applying events decoded from the server. Apps
 * normally don't call these directly — reach for them only when feeding the store from a
 * source the library doesn't manage.
 *
 * @typeParam T - the collection's entity type
 */
export interface SyncWrite<T> {
  /** Upsert one entity into the local baseline (insert if absent, replace if present). */
  readonly writeSynced: (entity: T) => Effect.Effect<void>
  /** Remove the entity with `id` from the local baseline. A no-op if it isn't present. */
  readonly deleteSynced: (id: ModelId) => Effect.Effect<void>
  /**
   * Replace the **entire** local baseline with `rows`, in one sync transaction (truncate
   * + writes) — applied atomically to the in-memory store and the persisted table. Rows
   * absent from `rows` are gone afterwards, with no read of the current keys (so it
   * cannot race background hydration). The collection drain uses this to reconcile snapshots.
   */
  readonly replaceSynced: (rows: ReadonlyArray<T>) => Effect.Effect<void>
  /**
   * Structural-only index so `SyncWrite<T>` matches TanStack's `UtilsRecord` shape —
   * which is what `useLiveQuery((q) => q.from({ … }))` requires of a collection's
   * `utils`. `(...args: never[]) => unknown` (NOT `any`) is the widest function the real
   * methods all satisfy; it carries no callable surface of its own. The named members
   * above are what callers use.
   */
  readonly [util: string]: (...args: never[]) => unknown
}
