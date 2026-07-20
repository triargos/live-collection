import { Context, Effect, Layer, Option, Order, Ref, Schema } from "effect"
import { compareSyncId, SyncId } from "@triargos/live-collection-protocol"

/**
 * The single, durable, **global** sync cursor — the newest syncId this client has
 * ingested from any model. It gates catchup (`from = cursor ?? "0"`) and advances as catchup
 * responses and live events land. It must survive reloads — that is its whole point, and
 * why it isn't TanStack's `staleTime` (which resets on reload).
 *
 * `get` is `None` only on a truly cold start. `set` is **monotonic** — it keeps the
 * larger of the current and incoming id by {@link compareSyncId} (numeric, exact beyond
 * `Number.MAX_SAFE_INTEGER`) — so a late, out-of-order event can never pull the cursor
 * backwards. `clear` is used by the live-resync reload path; the next start then catches
 * up cold and re-snapshots.
 */
export interface LastSyncIdStoreShape {
  readonly get: Effect.Effect<Option.Option<SyncId>>
  readonly set: (id: SyncId) => Effect.Effect<void>
  readonly clear: Effect.Effect<void>
}

/** The monotonic step shared by both adapters: keep whichever id is numerically larger. */
const advance = (current: Option.Option<SyncId>, next: SyncId): SyncId =>
  Option.match(current, {
    onNone: () => next,
    onSome: (c) => Order.max(compareSyncId)(c, next),
  })

const STORAGE_KEY = "live-collection:lastSyncId"

/** `localStorage`-backed cursor. The stored string is external input, so it is decoded against
 *  {@link SyncId} on read (a corrupt value reads as `None`); storage faults are defects (`orDie`). */
const makeLocalStorage: Effect.Effect<LastSyncIdStoreShape> = Effect.sync(() => {
  const decode = Schema.decodeUnknownOption(SyncId)
  const get: Effect.Effect<Option.Option<SyncId>> = Effect.sync(() =>
    Option.fromNullishOr(localStorage.getItem(STORAGE_KEY)).pipe(Option.flatMap(decode)),
  )
  return {
    get,
    set: (id) => get.pipe(Effect.map((c) => localStorage.setItem(STORAGE_KEY, advance(c, id)))),
    clear: Effect.sync(() => localStorage.removeItem(STORAGE_KEY)),
  }
})

/** In-memory cursor over a `Ref` — the test/SSR adapter. */
const makeMemory: Effect.Effect<LastSyncIdStoreShape> = Effect.gen(function* () {
  const ref = yield* Ref.make(Option.none<SyncId>())
  return {
    get: Ref.get(ref),
    set: (id) => Ref.update(ref, (c) => Option.some(advance(c, id))),
    clear: Ref.set(ref, Option.none()),
  }
})

/**
 * The sync-cursor service tag. Provide one of its layers as part of the `loop` layer
 * handed to `makeLiveRuntime`:
 *
 * @example
 * ```ts
 * const loop = Layer.mergeAll(
 *   SyncTransport.layer({ url: "/api/sync", keepAlive: "45 seconds" }),
 *   CatchupClient.layer({ url: "/api/catchup" }),
 *   LastSyncIdStore.layer,
 *   SyncJournal.layer(),
 * )
 * ```
 */
export class LastSyncIdStore extends Context.Service<
  LastSyncIdStore,
  LastSyncIdStoreShape
>()("LastSyncIdStore") {
  /** Browser default: a single `localStorage` entry, durable across reloads. */
  static readonly layer: Layer.Layer<LastSyncIdStore> = Layer.effect(LastSyncIdStore, makeLocalStorage)
  /** In-memory (a `Ref`) — for tests and SSR; resets on every run. */
  static readonly layerMemory: Layer.Layer<LastSyncIdStore> = Layer.effect(LastSyncIdStore, makeMemory)
}
