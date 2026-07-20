import { useEffect, useState } from "react"
import { cn } from "@/lib/utils.js"
import type { Playground } from "../live/playground.js"

function Segment({ label, value, className }: { readonly label: string; readonly value: string; readonly className?: string }) {
  return (
    <div className="flex items-baseline gap-1.5 px-3 py-1.5">
      <span className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className={cn("font-mono text-xs tabular-nums", className)}>{value}</span>
    </div>
  )
}

/**
 * The always-on system status strip — a dev-tools-style readout so the live state of the sync engine is
 * visible without opening the inspector: the loop is running, this tab's id, the server's latest
 * syncId, the shared event-log size, and whether chaos (failure injection) is armed.
 */
export function StatusBar({ pg }: { readonly pg: Playground }) {
  const [stats, setStats] = useState(() => ({
    log: pg.controls.serverLogSize(),
    lastSyncId: pg.controls.lastSyncId(),
    failureRate: pg.controls.getFailureRate(),
  }))

  useEffect(() => {
    const handle = setInterval(
      () =>
        setStats({
          log: pg.controls.serverLogSize(),
          lastSyncId: pg.controls.lastSyncId(),
          failureRate: pg.controls.getFailureRate(),
        }),
      600,
    )
    return () => clearInterval(handle)
  }, [pg])

  return (
    <div className="flex flex-wrap items-stretch divide-x border bg-card text-xs">
      <div className="flex items-center gap-1.5 px-3 py-1.5">
        <span className="relative flex size-1.5">
          <span className="absolute inline-flex size-full animate-ping rounded-full bg-emerald-500/70" />
          <span className="relative inline-flex size-1.5 rounded-full bg-emerald-500" />
        </span>
        <span className="text-[10px] font-medium uppercase tracking-wide">live</span>
      </div>
      <Segment label="tab" value={pg.tabId} />
      <Segment label="cursor" value={`#${stats.lastSyncId}`} />
      <Segment label="log" value={`${stats.log}`} />
      <Segment
        label="chaos"
        value={stats.failureRate > 0 ? `${Math.round(stats.failureRate * 100)}%` : "off"}
        className={stats.failureRate > 0 ? "text-red-600 dark:text-red-400" : "text-muted-foreground"}
      />
    </div>
  )
}
