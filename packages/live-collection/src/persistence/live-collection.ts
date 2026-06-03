import type { Collection } from "@tanstack/db"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "../dispatch/sync-write.js"

/**
 * The hero type. NOT a wrapper — it *is* a TanStack `Collection` whose key is {@link ModelId} and
 * whose `utils` host the server-truth write path. The UI reads it directly
 * (`useLiveQuery(collection)`); the `SyncDispatcher` reaches the synced-write path through
 * `collection.utils`.
 *
 * `Collection<T, TKey, TUtils, TSchema, TInsertInput>`:
 *   - `TKey   = ModelId`      branded string, assignable to TanStack's `string | number`
 *   - `TUtils = SyncWrite<T>` `writeSynced` / `deleteSynced`, hosted in utils (DESIGN §1)
 *   - `TSchema = never`       the schema-less overload: `data` is already decoded + branded at the
 *                             dispatch seam, so TanStack does no validation of its own (DEC-A1)
 */
export type LiveCollection<T extends object> = Collection<T, ModelId, SyncWrite<T>, never, T>
