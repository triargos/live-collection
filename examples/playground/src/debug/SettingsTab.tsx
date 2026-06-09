import { useEffect, useState } from "react"
import { RadioTower, RotateCcw, Trash2, Zap } from "lucide-react"
import { Button } from "@/components/ui/button.js"
import { Label } from "@/components/ui/label.js"
import { Slider } from "@/components/ui/slider.js"
import { Switch } from "@/components/ui/switch.js"
import type { DebugBus } from "./debug-bus.js"
import type { BackendControls } from "../live/shared-backend.js"

export function SettingsTab({
  bus,
  controls,
}: {
  readonly bus: DebugBus
  readonly controls: BackendControls
}) {
  const [enabled, setEnabled] = useState(controls.getFailureRate() > 0)
  const [rate, setRate] = useState(Math.round((controls.getFailureRate() || 0.3) * 100))

  useEffect(() => {
    controls.setFailureRate(enabled ? rate / 100 : 0)
  }, [controls, enabled, rate])

  return (
    <div className="h-full overflow-y-auto">
      <div className="space-y-5 p-3">
        <section className="space-y-3 border bg-card p-3">
          <div className="flex items-start justify-between gap-3">
            <Label htmlFor="fail-toggle" className="flex items-center gap-1.5 text-xs font-semibold">
              <Zap className="size-3.5" />
              Chaos: inject failures
            </Label>
            <Switch id="fail-toggle" checked={enabled} onCheckedChange={setEnabled} />
          </div>
          <p className="text-[11px] text-muted-foreground">
            Each mutation is randomly rejected, so TanStack rolls the optimistic write back. Watch a row
            appear then vanish.
          </p>
          <div className={enabled ? "space-y-1.5" : "pointer-events-none space-y-1.5 opacity-40"}>
            <div className="flex items-center justify-between text-[11px]">
              <span className="text-muted-foreground">Failure probability</span>
              <span className="font-mono tabular-nums">{rate}%</span>
            </div>
            <Slider
              value={[rate]}
              min={0}
              max={100}
              step={5}
              disabled={!enabled}
              onValueChange={(v) => setRate(v[0] ?? 0)}
            />
          </div>
        </section>

        <section className="space-y-1.5">
          <h3 className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Actions</h3>
          <Button variant="outline" size="sm" className="w-full justify-start" onClick={() => controls.broadcastResync()}>
            <RadioTower className="size-3.5" />
            Force resync — reloads other tabs
          </Button>
          <Button variant="outline" size="sm" className="w-full justify-start" onClick={() => controls.resetServer()}>
            <RotateCcw className="size-3.5" />
            Reset shared server log
          </Button>
          <Button variant="outline" size="sm" className="w-full justify-start" onClick={() => bus.clear()}>
            <Trash2 className="size-3.5" />
            Clear traffic log
          </Button>
        </section>

        <section className="space-y-2 border bg-muted/30 p-3 text-[11px] text-muted-foreground">
          <p className="font-medium text-foreground">Things to try</p>
          <ol className="list-inside list-decimal space-y-1">
            <li>Open this URL in a second tab, add a webhook — it syncs over a BroadcastChannel.</li>
            <li>Reload — rows hydrate from OPFS, then catchup fills any gap.</li>
            <li>Set failures to 100%, add a webhook — it appears, then rolls back.</li>
          </ol>
        </section>
      </div>
    </div>
  )
}
