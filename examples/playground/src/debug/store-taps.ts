import { useEffect } from "react"
import { Effect, Option } from "effect"
import { serializeKey } from "@triargos/live-collection"
import type { Playground } from "../live/playground.js"

/** A change message as TanStack DB delivers it to `subscribeChanges`. */
interface StoreChange {
  readonly type: "insert" | "update" | "delete"
  readonly key: string | number
}

/** The slice of a mounted collection we tap — its local-store change stream + the persistence preload. */
interface Watchable {
  readonly subscribeChanges: (
    callback: (changes: ReadonlyArray<StoreChange>) => void,
    options?: { readonly includeInitialState?: boolean },
  ) => { readonly unsubscribe: () => void }
  readonly preload: () => Promise<void>
}

/**
 * Surface the **read path landing in the local store** — the half of sync the backend can't see. The loop
 * applies catchup/SSE deltas through `writeSynced`/`deleteSynced`, and the persisted collection hydrates
 * from OPFS on mount; both show up here as `subscribeChanges` batches. We sweep the registry so newly
 * mounted instances (e.g. switching workspace) get tapped too, and label the synchronous initial batch as
 * the OPFS hydrate (the closest thing to a "loadFn" call landing). Optimistic writes also flow through, so
 * an insert appears in the store the instant it's made, before the server confirms it.
 */
export function useStoreTaps(pg: Playground): void {
  useEffect(() => {
    const subscriptions = new Map<string, { readonly unsubscribe: () => void }>()

    const sweep = (): void => {
      for (const entity of pg.models.map((m) => m._meta.entity)) {
        const mounted = Effect.runSync(pg.runtime.registry.getByEntity<Watchable>(entity))
        for (const { key, collection } of mounted) {
          const id = serializeKey(key)
          if (subscriptions.has(id)) continue
          const scope = Option.getOrElse(key.scope, () => "—")

          // Rows loaded from OPFS arrive as change events while `preload()` is in flight, so treat that
          // window as the hydrate phase; everything after preload resolves is a live store mutation
          // (a `writeSynced`/`deleteSynced` from the loop, or an optimistic write).
          let phase: "hydrate" | "live" = "hydrate"
          const subscription = collection.subscribeChanges(
            (changes) => {
              for (const change of changes) {
                const id8 = String(change.key).slice(0, 8)
                if (phase === "hydrate") {
                  pg.bus.push({ direction: "in", channel: "hydrate", label: `${entity}(${scope}) ${id8} ← OPFS` })
                } else {
                  pg.bus.push({
                    direction: change.type === "delete" ? "info" : "in",
                    channel: "store",
                    label: `${change.type} ${entity}(${scope}) ${id8}`,
                  })
                }
              }
            },
            { includeInitialState: true },
          )
          void collection.preload().then(() => {
            phase = "live"
          })
          subscriptions.set(id, subscription)
        }
      }
    }

    sweep()
    const handle = setInterval(sweep, 600)
    return () => {
      clearInterval(handle)
      for (const subscription of subscriptions.values()) subscription.unsubscribe()
    }
  }, [pg])
}
