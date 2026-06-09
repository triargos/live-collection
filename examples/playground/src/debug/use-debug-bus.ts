import { useSyncExternalStore } from "react"
import type { DebugBus, DebugEntry } from "./debug-bus.js"

/** Subscribe a component to the live network/traffic log. */
export function useDebugLog(bus: DebugBus): ReadonlyArray<DebugEntry> {
  return useSyncExternalStore(bus.subscribe, bus.snapshot)
}
