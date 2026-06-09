import { useState } from "react"
import { Bug, X } from "lucide-react"
import { Badge } from "@/components/ui/badge.js"
import { Button } from "@/components/ui/button.js"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs.js"
import { cn } from "@/lib/utils.js"
import type { Playground } from "../live/playground.js"
import { NetworkTab } from "./NetworkTab.js"
import { RegistryTab } from "./RegistryTab.js"
import { SettingsTab } from "./SettingsTab.js"

/**
 * The debug drawer — the app's window into the library internals: live sync traffic, the registry of
 * mounted collections + shared-server state, and the failure-injection / resync controls. Non-modal (no
 * scrim) on purpose: you keep interacting with the app while watching the traffic land.
 */
export function DebugPanel({ pg }: { readonly pg: Playground }) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <Button
        className="fixed right-5 bottom-5 z-50 shadow-lg"
        onClick={() => setOpen((value) => !value)}
      >
        {open ? <X /> : <Bug />}
        {open ? "Close" : "Inspector"}
      </Button>

      <aside
        aria-hidden={!open}
        className={cn(
          "fixed inset-y-0 right-0 z-40 flex w-[460px] max-w-[92vw] flex-col border-l bg-background shadow-2xl",
          "transition-transform duration-300 ease-[cubic-bezier(0.32,0.72,0,1)]",
          open ? "translate-x-0" : "translate-x-full",
        )}
      >
        <header className="flex shrink-0 items-center justify-between border-b px-3 py-2.5">
          <div className="flex items-center gap-2">
            <span className="relative flex size-2">
              <span className="absolute inline-flex size-full animate-ping rounded-full bg-emerald-500/70" />
              <span className="relative inline-flex size-2 rounded-full bg-emerald-500" />
            </span>
            <div className="leading-none">
              <div className="flex items-center gap-1.5 text-xs font-semibold">
                <Bug className="size-3.5" />
                Live Sync Inspector
              </div>
              <div className="mt-1 text-[10px] text-muted-foreground">sync loop running · two-surface runtime</div>
            </div>
          </div>
          <Badge variant="outline" className="font-mono text-[10px]">
            {pg.tabId}
          </Badge>
        </header>

        <Tabs defaultValue="network" className="flex min-h-0 flex-1 flex-col gap-0">
          <div className="shrink-0 border-b px-3 py-2">
            <TabsList variant="line" className="w-full gap-1">
              <TabsTrigger value="network">Network</TabsTrigger>
              <TabsTrigger value="registry">Registry</TabsTrigger>
              <TabsTrigger value="settings">Controls</TabsTrigger>
            </TabsList>
          </div>
          <TabsContent value="network" className="min-h-0 flex-1">
            <NetworkTab bus={pg.bus} />
          </TabsContent>
          <TabsContent value="registry" className="min-h-0 flex-1">
            <RegistryTab pg={pg} />
          </TabsContent>
          <TabsContent value="settings" className="min-h-0 flex-1">
            <SettingsTab bus={pg.bus} controls={pg.controls} />
          </TabsContent>
        </Tabs>
      </aside>
    </>
  )
}
