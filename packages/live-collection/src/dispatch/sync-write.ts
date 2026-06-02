import type { Effect } from "effect"
import type { ModelId } from "@triargos/live-collection-protocol"

/**
 * The synced-store write path of a collection: how server-authoritative state is applied to
 * the local baseline. This is distinct from the optimistic-mutation path the UI writes through
 * — synced writes reflect confirmed server truth and must not be rolled back.
 *
 * A live collection implements this; the sync dispatcher is the primary caller, applying events
 * decoded from the server. `writeSynced` is an upsert — both inserts and updates simply make the
 * local row match the server's.
 *
 * @typeParam T - the collection's entity type
 */
export interface SyncWrite<T> {
  /** Upsert one entity into the local baseline (insert if absent, replace if present). */
  readonly writeSynced: (entity: T) => Effect.Effect<void>
  /** Remove the entity with `id` from the local baseline. A no-op if it isn't present. */
  readonly deleteSynced: (id: ModelId) => Effect.Effect<void>
}
