import { createContext, type ReactNode, useContext } from "react"
import type { Playground } from "./playground.js"

// A plain React context for the single, app-lifetime Playground value built at startup. Components read
// the runtime + collection handles from here instead of importing a module singleton.
const PlaygroundContext = createContext<Playground | null>(null)

export function PlaygroundProvider({
  value,
  children,
}: {
  value: Playground
  children: ReactNode
}) {
  return <PlaygroundContext.Provider value={value}>{children}</PlaygroundContext.Provider>
}

export function usePlayground(): Playground {
  const pg = useContext(PlaygroundContext)
  if (pg === null) {
    throw new Error("usePlayground must be used within <PlaygroundProvider>")
  }
  return pg
}
