import { ArrowDown, ArrowUp, type LucideIcon, Minus, RefreshCw, X } from "lucide-react"
import type { DebugDirection } from "./debug-bus.js"

/**
 * The shared vocabulary for the traffic log — one place that defines what every direction and channel
 * *means*, so the panel can render a legend and the rows stay self-explanatory. Colour is anchored to the
 * **direction** (the pipeline stage), and the **channel** names the specific operation. Tailwind needs
 * literal class strings, so the accents are spelled out rather than computed.
 */
export interface DirectionMeta {
  readonly Icon: LucideIcon
  readonly label: string
  readonly badge: string
  readonly dot: string
  readonly desc: string
}

export const DIRECTION_META: Record<DebugDirection, DirectionMeta> = {
  out: {
    Icon: ArrowUp,
    label: "out",
    badge: "text-amber-600 dark:text-amber-400 border-amber-500/30 bg-amber-500/5",
    dot: "bg-amber-500",
    desc: "request leaving this tab → the (fake) server",
  },
  in: {
    Icon: ArrowDown,
    label: "in",
    badge: "text-emerald-600 dark:text-emerald-400 border-emerald-500/30 bg-emerald-500/5",
    dot: "bg-emerald-500",
    desc: "arriving from the server, or landing in the local store",
  },
  echo: {
    Icon: RefreshCw,
    label: "echo",
    badge: "text-sky-600 dark:text-sky-400 border-sky-500/30 bg-sky-500/5",
    dot: "bg-sky-500",
    desc: "this tab's own write, confirmed back through the loop",
  },
  info: {
    Icon: Minus,
    label: "info",
    badge: "text-muted-foreground border-border bg-muted/40",
    dot: "bg-muted-foreground",
    desc: "lifecycle / system note",
  },
  error: {
    Icon: X,
    label: "error",
    badge: "text-red-600 dark:text-red-400 border-red-500/30 bg-red-500/5",
    dot: "bg-red-500",
    desc: "mutation rejected → optimistic write rolled back",
  },
}

/** One-line meaning for each channel — shown as a row tooltip and in the legend. */
export const CHANNEL_DESC: Record<string, string> = {
  mutation: "optimistic create/delete calling the server",
  sync: "live SSE event (this tab's echo or a cross-tab arrival)",
  store: "writeSynced / deleteSynced landing in the local TanStack store",
  hydrate: "rows loaded from OPFS on mount",
  catchup: "GET /catchup replaying the shared log since the cursor",
  listFn: "snapshot source — cold start / resync reconcile",
  resync: "broadcast Resync(All) to other tabs",
  broadcast: "raw cross-tab BroadcastChannel message",
  server: "the shared server log changed",
}

export const directionOf = (channel: string): string => CHANNEL_DESC[channel] ?? channel
