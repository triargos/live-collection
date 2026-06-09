import { useMemo, useState } from "react"
import { Trash2 } from "lucide-react"
import { Button } from "@/components/ui/button.js"
import { cn } from "@/lib/utils.js"
import type { DebugBus, DebugEntry } from "./debug-bus.js"
import { useDebugLog } from "./use-debug-bus.js"
import { CHANNEL_DESC, DIRECTION_META } from "./channels.js"

const formatTime = (at: number): string => {
  const d = new Date(at)
  return `${d.toLocaleTimeString("en-US", { hour12: false })}.${String(d.getMilliseconds()).padStart(3, "0")}`
}

function Row({ entry }: { readonly entry: DebugEntry }) {
  const { Icon, badge } = DIRECTION_META[entry.direction]
  return (
    <div className="flex items-start gap-2 border-b px-3 py-1.5 transition-colors hover:bg-muted/40">
      <span
        title={CHANNEL_DESC[entry.channel] ?? entry.channel}
        className={cn(
          "mt-px flex shrink-0 items-center gap-1 border px-1.5 py-0.5 font-mono text-[10px] leading-none",
          badge,
        )}
      >
        <Icon className="size-2.5" />
        {entry.channel}
      </span>
      <div className="min-w-0 flex-1">
        <div className="break-words text-xs leading-snug">{entry.label}</div>
        {entry.payload !== undefined && (
          <details className="mt-0.5">
            <summary className="cursor-pointer text-[10px] text-muted-foreground select-none hover:text-foreground">
              payload
            </summary>
            <pre className="mt-1 overflow-x-auto border bg-muted/50 p-2 font-mono text-[10px] leading-relaxed">
              {JSON.stringify(entry.payload, null, 2)}
            </pre>
          </details>
        )}
      </div>
      <time className="mt-px shrink-0 font-mono text-[10px] tabular-nums text-muted-foreground">
        {formatTime(entry.at)}
      </time>
    </div>
  )
}

function Legend() {
  return (
    <details className="border-b bg-muted/20 px-3 py-2 text-[11px]">
      <summary className="cursor-pointer font-medium text-muted-foreground select-none hover:text-foreground">
        What am I looking at?
      </summary>
      <p className="mt-2 text-muted-foreground">
        Each row is one event crossing the sync boundary. Colour = direction; the tag names the operation.
      </p>
      <ul className="mt-2 grid gap-1.5">
        {Object.entries(DIRECTION_META).map(([key, meta]) => (
          <li key={key} className="flex items-center gap-2">
            <span className={cn("flex w-14 shrink-0 items-center gap-1 border px-1.5 py-0.5 font-mono text-[10px]", meta.badge)}>
              <meta.Icon className="size-2.5" />
              {meta.label}
            </span>
            <span className="text-muted-foreground">{meta.desc}</span>
          </li>
        ))}
      </ul>
    </details>
  )
}

export function NetworkTab({ bus }: { readonly bus: DebugBus }) {
  const entries = useDebugLog(bus)
  const [hidden, setHidden] = useState<ReadonlySet<string>>(new Set())

  const channels = useMemo(() => {
    const seen = new Set<string>()
    for (const e of entries) seen.add(e.channel)
    return [...seen].sort()
  }, [entries])

  const visible = entries.filter((e) => !hidden.has(e.channel))
  const toggle = (channel: string) =>
    setHidden((current) => {
      const next = new Set(current)
      if (next.has(channel)) next.delete(channel)
      else next.add(channel)
      return next
    })

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b px-3 py-2">
        <div>
          <div className="text-xs font-medium">Sync traffic</div>
          <div className="text-[11px] text-muted-foreground">
            {visible.length} of {entries.length} · newest first
          </div>
        </div>
        <Button variant="ghost" size="sm" onClick={() => bus.clear()} disabled={entries.length === 0}>
          <Trash2 className="size-3.5" />
          Clear
        </Button>
      </div>

      <Legend />

      {channels.length > 0 && (
        <div className="flex flex-wrap gap-1 border-b px-3 py-2">
          {channels.map((channel) => {
            const off = hidden.has(channel)
            return (
              <button
                key={channel}
                type="button"
                onClick={() => toggle(channel)}
                title={CHANNEL_DESC[channel] ?? channel}
                className={cn(
                  "border px-1.5 py-0.5 font-mono text-[10px] transition-colors",
                  off
                    ? "border-dashed text-muted-foreground/50 line-through"
                    : "border-border bg-muted/50 text-foreground hover:bg-muted",
                )}
              >
                {channel}
              </button>
            )
          })}
        </div>
      )}

      <div className="min-h-0 flex-1 overflow-y-auto">
        {visible.length === 0 ? (
          <p className="px-3 py-10 text-center text-xs text-muted-foreground">
            {entries.length === 0
              ? "No traffic yet. Add or delete a webhook to see the read/write path light up."
              : "Every channel is filtered out."}
          </p>
        ) : (
          visible.map((entry) => <Row key={entry.id} entry={entry} />)
        )}
      </div>
    </div>
  )
}
