import { type FormEvent, useState } from "react"
import { useLiveQuery } from "@tanstack/react-db"
import { Link2, Plus, Trash2 } from "lucide-react"
import { Button } from "@/components/ui/button.js"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card.js"
import { Input } from "@/components/ui/input.js"
import { cn } from "@/lib/utils.js"
import { usePlayground } from "../live/context.js"
import { webhookKey } from "../live/schema.js"

const ORGS = ["org-1", "org-2"] as const

// Scoped collection, one instance per `orgId`. Writes go through the NATIVE optimistic path: `coll.insert`
// with a client-minted id appears instantly, the handler calls the fake server + reconciles via
// `writeSynced`, the SSE echo confirms it idempotently, and it persists to OPFS (survives reload).
export function WebhooksPage() {
  const pg = usePlayground()
  const [orgId, setOrgId] = useState<string>(ORGS[0])
  const [url, setUrl] = useState("https://example.com/hook")
  const coll = pg.webhooks(orgId)
  const { data } = useLiveQuery(() => coll, [orgId])
  const rows = data ?? []

  const onAdd = (e: FormEvent) => {
    e.preventDefault()
    if (url.trim().length === 0) return
    coll.insert({ id: crypto.randomUUID(), orgId, url }) // client-minted id
  }

  return (
    <Card>
      <CardHeader className="gap-3">
        <div className="flex items-center justify-between">
          <CardTitle>Webhooks</CardTitle>
          <span className="border bg-muted/50 px-1.5 py-0.5 font-mono text-[10px] tabular-nums text-muted-foreground">
            {rows.length} rows
          </span>
        </div>
        <div className="flex items-center gap-3">
          <div className="inline-flex border p-0.5">
            {ORGS.map((org) => (
              <button
                key={org}
                type="button"
                onClick={() => setOrgId(org)}
                className={cn(
                  "px-2.5 py-1 font-mono text-xs transition-colors",
                  org === orgId ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground",
                )}
              >
                {org}
              </button>
            ))}
          </div>
          <span className="font-mono text-[11px] text-muted-foreground">
            collection <span className="text-foreground">(Webhook, {orgId})</span>
          </span>
        </div>
      </CardHeader>

      <CardContent className="space-y-3">
        <form className="flex gap-2" onSubmit={onAdd}>
          <Input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://…" className="font-mono" />
          <Button type="submit">
            <Plus /> Add
          </Button>
        </form>

        <ul className="divide-y border">
          {rows.map((w) => (
            <li key={w.id} className="group flex items-center justify-between gap-3 px-3 py-2 transition-colors hover:bg-muted/40">
              <div className="flex min-w-0 items-center gap-2">
                <Link2 className="size-3.5 shrink-0 text-muted-foreground" />
                <span className="truncate text-xs">{w.url}</span>
                <code className="shrink-0 font-mono text-[10px] text-muted-foreground">{w.id.slice(0, 8)}</code>
              </div>
              <Button
                variant="ghost"
                size="icon-sm"
                className="opacity-0 transition-opacity group-hover:opacity-100"
                onClick={() => coll.delete(webhookKey(w))}
              >
                <Trash2 className="size-3.5" />
              </Button>
            </li>
          ))}
          {rows.length === 0 && (
            <li className="px-3 py-10 text-center text-xs text-muted-foreground">
              No webhooks in <span className="font-mono">{orgId}</span> yet.
            </li>
          )}
        </ul>
      </CardContent>
    </Card>
  )
}
