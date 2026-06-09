/**
 * `@triargos/live-collection-react` — optional React bindings.
 *
 * The core is already React-friendly: `defineCollection(...)` returns a **native** TanStack collection,
 * so reads use `@tanstack/react-db`'s `useLiveQuery` directly — import it from there, we don't wrap or
 * re-export it. The only genuinely React-specific piece is lifecycle: {@link useLiveSync} forks the sync
 * loop on mount and interrupts it on unmount (DEC-R8).
 */
import { useEffect } from "react"
import { Effect, Fiber } from "effect"
import type { LiveRuntime, SyncModels } from "@triargos/live-collection"

/**
 * Run the live sync loop for `models` for the lifetime of the mounting component (DEC-R8). Forks
 * `runtime.forkLoop` on mount and `Fiber.interrupt`s it on unmount. Mount once near the app root.
 *
 * Interrupting stops the SSE loop but does **not** dispose collections — registry lifetime is the
 * app's, so a remount reuses the warm local store. `models` is captured at mount (the loop reads it
 * once at start); changing it later has no effect — keep it a stable, module-level array.
 */
export function useLiveSync(runtime: LiveRuntime, models: SyncModels): void {
  // `models` intentionally omitted from deps: the loop snapshots it at start; re-forking per render
  // (a fresh `[ ... ]` literal each render) would thrash the connection.
  useEffect(() => {
    const fiber = runtime.forkLoop(models)
    return () => {
      Effect.runFork(Fiber.interrupt(fiber))
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- models is captured at mount by design
  }, [runtime])
}
