import { useEffect, useMemo, useState } from "react"
import { Effect } from "effect"
import { type CollectionKey, scopedKey } from "@triargos/live-collection"
import { CheckCircle2, Database, Power, PowerOff, RotateCcw, Sparkles, Zap } from "lucide-react"
import { Badge } from "@/components/ui/badge.js"
import { Button } from "@/components/ui/button.js"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card.js"
import { cn } from "@/lib/utils.js"
import { usePlayground } from "../live/context.js"
import { useDebugLog } from "../debug/use-debug-bus.js"

// A dedicated, throwaway scope — separate from the org-1/org-2 tabs (which stay mounted once visited) so
// the lab can be unmounted at will. Reset bumps the generation to a *fresh* scope, the cleanest wipe: a
// new scope has no base watermark and no logged events, so it starts the cycle over from zero.
const SCOPE_BASE = "replay-lab"
const keyFor = (scope: string): CollectionKey<unknown> => scopedKey({ entity: "Webhook", scope })

const peek = (pg: ReturnType<typeof usePlayground>, scope: string): { mounted: boolean; rows: number } => {
  const collection = pg.mounted.get(scope)
  return collection === undefined
    ? { mounted: false, rows: 0 }
    : { mounted: true, rows: Array.from(collection.keys()).length }
}

/** Which action the demo nudges you toward next — drives the highlighted button + the status line. */
type Step = "mount-1" | "unmount" | "seed" | "mount-2" | "done"

function Stat({ label, value, accent }: { readonly label: string; readonly value: string; readonly accent?: boolean }) {
  return (
    <div className={cn("border bg-card px-2.5 py-1.5", accent && "border-primary/40 bg-primary/5")}>
      <div className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="font-mono text-sm tabular-nums">{value}</div>
    </div>
  )
}

// A guided, four-step proof of replay-on-mount: mount a scope, unmount it, let "remote" events stream past
// while it's away, then mount it again — and watch it heal from the durable IndexedDB log with zero
// network. The status line narrates where you are; the highlighted button is the suggested next step.
export function ReplayLab() {
  const pg = usePlayground()
  const [gen, setGen] = useState(1)
  const [everMounted, setEverMounted] = useState(false)
  const [seeded, setSeeded] = useState(0)
  const scope = `${SCOPE_BASE}-${gen}`
  const labKey = useMemo(() => keyFor(scope), [scope])

  const [{ mounted, rows }, setState] = useState(() => peek(pg, scope))
  const refresh = () => setState(peek(pg, scope))
  useEffect(() => {
    setState(peek(pg, scope)) // re-sync immediately when the scope changes (Reset)
    const handle = setInterval(() => setState(peek(pg, scope)), 400)
    return () => clearInterval(handle)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [labKey])

  // Network proof, read straight from the traffic log: how many times `listFn` ran for THIS scope.
  const log = useDebugLog(pg.bus)
  const listFnCalls = log.filter((e) => e.channel === "listFn" && e.label.includes(`(${scope})`)).length

  const step: Step = !everMounted
    ? "mount-1"
    : mounted && seeded === 0
      ? "unmount"
      : !mounted && seeded === 0
        ? "seed"
        : !mounted && seeded > 0
          ? "mount-2"
          : "done"

  const mount = () => {
    pg.webhooks(scope) // getOrCreate → mounts; the loop's onMount decides skip/replay/bootstrap
    setEverMounted(true)
    refresh()
  }
  const unmount = () => {
    // dispose closes the collection's scope, which runs its async `cleanup()` finalizer — NOT runSync-safe.
    void Effect.runPromise(pg.runtime.registry.dispose(labKey)).then(
      () => {
        pg.mounted.delete(scope)
        refresh()
      },
      refresh,
    )
  }
  const seed = () => {
    pg.controls.seedRemote({ orgId: scope, url: `https://lab.example/hook-${seeded + 1}` })
    setSeeded((n) => n + 1)
  }
  const reset = () => {
    if (mounted) {
      Effect.runFork(pg.runtime.registry.dispose(labKey))
      pg.mounted.delete(scope)
    }
    setSeeded(0)
    setEverMounted(false)
    setGen((g) => g + 1) // fresh scope ⇒ clean slate
  }

  const status = {
    "mount-1": { icon: Power, tone: "muted", text: "Empty scope. Mount it — the first mount bootstraps once (you'll see one listFn call)." },
    unmount: { icon: PowerOff, tone: "muted", text: "Mounted. Now unmount it — pretend you navigated away or closed the workspace." },
    seed: { icon: Zap, tone: "wait", text: "Unmounted. Seed a few remote events — the loop logs them to IndexedDB but shows nothing (nothing's mounted)." },
    "mount-2": { icon: Database, tone: "wait", text: `${seeded} event${seeded === 1 ? "" : "s"} waiting in the local log. Mount again — they replay instantly, no network.` },
    done: { icon: CheckCircle2, tone: "win", text: `Replayed ${rows} event${rows === 1 ? "" : "s"} from IndexedDB — listFn calls: ${listFnCalls}. No new network ⇒ healed from disk. Reset to run again.` },
  }[step] as { readonly icon: typeof Power; readonly text: string; readonly tone: "muted" | "wait" | "win" }
  const StatusIcon = status.icon

  return (
    <Card>
      <CardHeader className="gap-3">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-1.5">
            <Sparkles className="size-4" /> Replay-on-mount
          </CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="font-mono text-[10px]">
              {scope}
            </Badge>
            <Button variant="ghost" size="sm" onClick={reset} title="Start over in a fresh scope">
              <RotateCcw /> Reset
            </Button>
          </div>
        </div>
        <p className="text-sm text-muted-foreground">
          A scope mounted <em>after</em> its events streamed past heals from the durable{" "}
          <strong className="font-medium text-foreground">IndexedDB log</strong> — no network. Follow the
          highlighted button; watch <strong className="font-medium text-foreground">Rows</strong> fill while{" "}
          <strong className="font-medium text-foreground">listFn</strong> stays put.
        </p>
      </CardHeader>

      <CardContent className="space-y-4">
        <div
          className={cn(
            "flex items-start gap-2 border-l-2 px-3 py-2 text-xs",
            status.tone === "win"
              ? "border-primary bg-primary/10 text-foreground"
              : status.tone === "wait"
                ? "border-amber-500/60 bg-amber-500/5 text-foreground"
                : "border-border bg-muted/40 text-muted-foreground",
          )}
        >
          <StatusIcon className="mt-0.5 size-3.5 shrink-0" />
          <span>{status.text}</span>
        </div>

        <div className="grid grid-cols-3 gap-2">
          <Stat label="Mounted" value={mounted ? "yes" : "no"} />
          <Stat label="Rows" value={String(rows)} accent={rows > 0} />
          <Stat label="listFn (network)" value={String(listFnCalls)} accent />
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button
            onClick={mount}
            disabled={mounted}
            variant={!mounted && (step === "mount-1" || step === "mount-2") ? "default" : "outline"}
            className={cn(!mounted && (step === "mount-1" || step === "mount-2") && "ring-2 ring-primary ring-offset-1 ring-offset-background")}
          >
            <Power /> {step === "mount-2" ? "Mount (replays)" : "Mount"}
          </Button>
          <Button
            onClick={unmount}
            disabled={!mounted}
            variant="outline"
            className={cn(step === "unmount" && "ring-2 ring-primary ring-offset-1 ring-offset-background")}
          >
            <PowerOff /> Unmount
          </Button>
          <Button
            onClick={seed}
            variant="secondary"
            className={cn(step === "seed" && "ring-2 ring-primary ring-offset-1 ring-offset-background")}
          >
            <Zap /> Seed remote event
          </Button>
        </div>

        <p className="text-[11px] text-muted-foreground">
          Pop the <strong className="font-medium text-foreground">Inspector</strong> to watch it land — a{" "}
          <code className="font-mono">listFn({scope})</code> tap on the first mount, then only{" "}
          <code className="font-mono">seed</code> + local <code className="font-mono">store insert</code> taps on
          replay. <strong className="font-medium text-foreground">Reset</strong> starts a fresh scope — the
          cleanest wipe (new scope, no base, no logged events).
        </p>
      </CardContent>
    </Card>
  )
}
