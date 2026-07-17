import { useLiveSync } from "@triargos/live-collection-react"
import { Boxes } from "lucide-react"
import { Badge } from "@/components/ui/badge.js"
import { usePlayground } from "../live/context.js"
import { DebugPanel } from "../debug/DebugPanel.js"
import { useStoreTaps } from "../debug/store-taps.js"
import { ReplayLab } from "./ReplayLab.js"
import { StatusBar } from "./StatusBar.js"
import { WebhooksPage } from "./WebhooksPage.js"

// The app shell. `useLiveSync` forks the sync loop once for the app's lifetime (DEC-R8) — mounted here,
// near the root, so the whole app shares one loop. The DebugPanel overlays it as a slide-over.
export function App() {
  const pg = usePlayground()
  useLiveSync(pg.runtime)
  useStoreTaps(pg) // tap the local-store read path (hydrate + writeSynced/deleteSynced) into the debug bus
  return (
    <div className="min-h-dvh">
      <div className="mx-auto max-w-3xl space-y-6 px-6 py-12">
        <header className="space-y-3">
          <div className="flex items-center gap-2">
            <span className="flex size-7 items-center justify-center border bg-card">
              <Boxes className="size-4" />
            </span>
            <h1 className="font-mono text-lg font-semibold tracking-tight">live-collection</h1>
            <Badge variant="outline" className="font-mono text-[10px]">
              playground
            </Badge>
          </div>
          <p className="max-w-prose text-sm text-muted-foreground">
            Effect + TanStack DB live-sync. A working demo of the internals: optimistic writes, cross-tab
            sync over a BroadcastChannel, OPFS persistence and rollback. Pop the{" "}
            <strong className="font-medium text-foreground">Inspector</strong> to watch the read/write path
            land in real time.
          </p>
        </header>

        <StatusBar pg={pg} />
        <WebhooksPage />
        <ReplayLab />
      </div>
      <DebugPanel pg={pg} />
    </div>
  )
}
