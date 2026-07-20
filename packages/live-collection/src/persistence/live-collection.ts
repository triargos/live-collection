import type { Collection } from "@tanstack/db"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "./sync-write.js"

/**
 * A live-synced collection — what a collection handle from `defineCollection` returns.
 * NOT a wrapper: it *is* a native TanStack `Collection`, so everything TanStack offers
 * works directly — query it with `useLiveQuery`, write optimistically with
 * `.insert/.update/.delete`. The library keeps it in sync underneath: rows are persisted
 * locally (SQLite) and updated live from the server's event stream.
 *
 * Its `utils` carry the {@link SyncWrite} server-truth write path the collection drain applies
 * events through — apps normally never touch it.
 *
 * @example
 * ```ts
 * const webhooks: LiveCollection<Webhook> = webhookCollection(orgId)
 * const { data } = useLiveQuery((q) => q.from({ w: webhooks }))
 * webhooks.insert({ id: crypto.randomUUID(), orgId, url })
 * ```
 *
 * Type parameters of the underlying `Collection<T, TKey, TUtils, TSchema, TInsertInput>`:
 *   - `TKey   = ModelId`      branded string, assignable to TanStack's `string | number`
 *   - `TUtils = SyncWrite<T>` the synced-store write path, hosted in `utils`
 *   - `TSchema = never`       entities are already decoded and branded at the sync
 *                             boundary, so TanStack does no validation of its own
 */
export type LiveCollection<T extends object> = Collection<T, ModelId, SyncWrite<T>, never, T>
