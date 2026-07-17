/**
 * `@triargos/live-collection-react` — optional React bindings for
 * `@triargos/live-collection`.
 *
 * The core is already React-friendly: `defineCollection(...)` returns a **native**
 * TanStack collection, so reads use `@tanstack/react-db`'s `useLiveQuery` directly —
 * import it from there, this package doesn't wrap or re-export it. The only genuinely
 * React-specific piece is lifecycle: {@link useLiveSync} forks broker ingest on mount
 * and interrupts it on unmount.
 */
import { useEffect } from "react"
import { Effect, Fiber } from "effect"
import type { LiveRuntime } from "@triargos/live-collection"

/**
 * Run broker ingest for the lifetime of the mounting component. Forks
 * `runtime.forkSync` on mount and interrupts the fiber on unmount. Mount it **once**
 * near the app root; collections subscribe themselves when mounted.
 *
 * Interrupting stops the live connection but does **not** dispose collections — registry
 * lifetime is the app's, so a remount reuses the warm local store.
 *
 * @example
 * ```tsx
 * import { useLiveSync } from "@triargos/live-collection-react"
 * import { runtime } from "./collections"
 *
 * export function App() {
 *   useLiveSync(runtime)
 *   return <Routes />
 * }
 * ```
 */
export function useLiveSync(runtime: LiveRuntime): void {
  useEffect(() => {
    const fiber = runtime.forkSync()
    return () => {
      Effect.runFork(Fiber.interrupt(fiber))
    }
  }, [runtime])
}
