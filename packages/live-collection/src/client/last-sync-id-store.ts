import { Context, Effect, Layer, Option, Order, Ref, Schema } from "effect"
import { compareSyncId, SyncId } from "@triargos/live-collection-protocol"

/**
 * The single, durable, **global** sync cursor — the high-water mark of events this client has
 * applied. It gates catchup (`from = cursor ?? "0"`) and advances as catchup responses and live
 * events land. It is deliberately *ours*, not the framework's `staleTime` (decision 5): `staleTime`
 * resets on reload, a sync cursor must not.
 *
 * `get` is `None` only on a truly cold start. `set` is **monotonic** — it keeps the larger of the
 * current and incoming id by {@link compareSyncId} (numeric, exact beyond `Number.MAX_SAFE_INTEGER`)
 * — so a late, out-of-order event can never pull the cursor backwards. `clear` is used by the
 * live-resync reload path; the next start then catches up cold and re-snapshots (DEC-T6).
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
    Option.fromNullable(localStorage.getItem(STORAGE_KEY)).pipe(Option.flatMap(decode)),
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

/** The seam: `yield* LastSyncIdStore`. */
export class LastSyncIdStore extends Context.Tag("LastSyncIdStore")<
  LastSyncIdStore,
  LastSyncIdStoreShape
>() {
  /** Prod (browser): a single `localStorage` entry. */
  static readonly layer: Layer.Layer<LastSyncIdStore> = Layer.effect(LastSyncIdStore, makeLocalStorage)
  /** Test/SSR: a `Ref`. */
  static readonly layerMemory: Layer.Layer<LastSyncIdStore> = Layer.effect(LastSyncIdStore, makeMemory)
}
