import { Data } from "effect"
import type { ModelId, SyncId } from "@triargos/live-collection-protocol"

/**
 * What a subscriber receives from the broker — replay + live tail as one stream.
 * `Snapshot` means the subscriber's local base is untrusted: re-list the server truth
 * and replace the whole table, then treat `at` as applied.
 */
export type SyncSignal = Data.TaggedEnum<{
  Snapshot: { readonly at: SyncId }
  Upsert: { readonly syncId: SyncId; readonly modelId: ModelId; readonly data: unknown }
  Delete: { readonly syncId: SyncId; readonly modelId: ModelId }
}>
export const SyncSignal = Data.taggedEnum<SyncSignal>()
