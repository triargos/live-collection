import { useEffect } from "react"
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
      for (const [scope, collection] of pg.mounted as ReadonlyMap<string, Watchable>) {
        if (subscriptions.has(scope)) continue
        let phase: "hydrate" | "live" = "hydrate"
        const subscription = collection.subscribeChanges(
          (changes) => {
            for (const change of changes) {
              const id8 = String(change.key).slice(0, 8)
              if (phase === "hydrate") {
                pg.bus.push({ direction: "in", channel: "hydrate", label: `Webhook(${scope}) ${id8} ← OPFS` })
              } else {
                pg.bus.push({
                  direction: change.type === "delete" ? "info" : "in",
                  channel: "store",
                  label: `${change.type} Webhook(${scope}) ${id8}`,
                })
              }
            }
          },
          { includeInitialState: true },
        )
        void collection.preload().then(() => {
          phase = "live"
        })
        subscriptions.set(scope, subscription)
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
