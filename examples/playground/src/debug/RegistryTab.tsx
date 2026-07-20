import { type ReactNode, useEffect, useState } from "react"
import { scopedKey, serializeKey } from "@triargos/live-collection"
import { Boxes, Server } from "lucide-react"
import { cn } from "@/lib/utils.js"
import type { Playground } from "../live/playground.js"

interface RegistryRow {
  readonly entity: string
  readonly scope: string
  readonly tableId: string
  readonly count: number
}

const readRegistry = (pg: Playground): ReadonlyArray<RegistryRow> =>
  [...pg.mounted].map(([scope, collection]) => ({
    entity: "Webhook",
    scope,
    tableId: serializeKey(scopedKey({ entity: "Webhook", scope })),
    count: Array.from(collection.keys()).length,
  }))

function Section({
  icon: Icon,
  title,
  hint,
  children,
}: {
  readonly icon: typeof Server
  readonly title: string
  readonly hint: string
  readonly children: ReactNode
}) {
  return (
    <section className="space-y-2">
      <div>
        <h3 className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wide">
          <Icon className="size-3.5" />
          {title}
        </h3>
        <p className="mt-0.5 text-[11px] text-muted-foreground">{hint}</p>
      </div>
      {children}
    </section>
  )
}

function Stat({ label, value, mono = true }: { readonly label: string; readonly value: string; readonly mono?: boolean }) {
  return (
    <div className="border bg-card px-2.5 py-1.5">
      <div className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className={cn("text-xs", mono && "font-mono")}>{value}</div>
    </div>
  )
}

export function RegistryTab({ pg }: { readonly pg: Playground }) {
  const [rows, setRows] = useState<ReadonlyArray<RegistryRow>>(() => readRegistry(pg))
  const [server, setServer] = useState(() => ({
    log: pg.controls.serverLogSize(),
    lastSyncId: pg.controls.lastSyncId(),
  }))

  useEffect(() => {
    const handle = setInterval(() => {
      setRows(readRegistry(pg))
      setServer({ log: pg.controls.serverLogSize(), lastSyncId: pg.controls.lastSyncId() })
    }, 500)
    return () => clearInterval(handle)
  }, [pg])

  return (
    <div className="h-full overflow-y-auto">
      <div className="space-y-5 p-3">
        <Section
          icon={Server}
          title="Shared server"
          hint="The fake backend every tab talks to — a localStorage event log + BroadcastChannel."
        >
          <div className="grid grid-cols-2 gap-2">
            <Stat label="Event log" value={`${server.log} events`} />
            <Stat label="Latest syncId" value={`#${server.lastSyncId}`} />
          </div>
        </Section>

        <Section
          icon={Boxes}
          title="This client"
          hint={`Tab ${pg.tabId} · the registry caches one native collection per (entity, scope).`}
        >
          {rows.length === 0 ? (
            <p className="border border-dashed px-3 py-6 text-center text-[11px] text-muted-foreground">
              Nothing mounted yet — switch workspace to mount a collection.
            </p>
          ) : (
            <ul className="space-y-2">
              {rows.map((row) => (
                <li key={row.tableId} className="border bg-card p-2.5">
                  <div className="flex items-center justify-between">
                    <span className="font-mono text-xs font-medium">
                      {row.entity}
                      <span className="text-muted-foreground">:{row.scope}</span>
                    </span>
                    <span className="border bg-muted/50 px-1.5 py-0.5 font-mono text-[10px] tabular-nums">
                      {row.count} row{row.count === 1 ? "" : "s"}
                    </span>
                  </div>
                  <div className="mt-1.5 truncate font-mono text-[10px] text-muted-foreground" title={row.tableId}>
                    {row.tableId}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Section>
      </div>
    </div>
  )
}
