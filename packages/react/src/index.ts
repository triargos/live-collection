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
import { useEffect, useState } from "react"
import { Data, Effect, Fiber } from "effect"
import {
  HydrateFailed,
  loadByMethodName,
  type LiveRuntime,
  type PartialLiveCollection,
  SubsetForbidden,
} from "@triargos/live-collection"

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

/**
 * Exactly one index key of a partial collection with its value — `{ templateId: "t1" }`.
 * Distributes over `By`, so a key outside `partial.by` (or a two-key literal, via
 * excess-property checking) is a compile error.
 */
export type SubsetOf<T extends object, By extends Record<string, (e: T) => string>> = {
  [K in keyof By]: { readonly [P in K]: ReturnType<By[K]> } & {
    readonly [P in Exclude<keyof By, K>]?: never
  }
}[keyof By]

/**
 * The Result-shaped status of a subset ensure. The ensure carries no success payload —
 * data flows through `useLiveQuery` — so the cases are bare states with typed errors:
 *
 * - `Loading` — ensure in flight. With persisted rows already hydrated this is
 *   stale-while-revalidate, not an empty screen.
 * - `Ready` — the subset's rows are current and live.
 * - `Forbidden` — the server refused visibility; re-mount (or value change) retries.
 * - `Failed` — network trouble; a real state with a `retry` handle, not an eternal spinner.
 */
export type SubsetStatus = Data.TaggedEnum<{
  Loading: {}
  Ready: {}
  Forbidden: { readonly error: SubsetForbidden }
  Failed: { readonly error: HydrateFailed; readonly retry: () => void }
}>
export const SubsetStatus = Data.taggedEnum<SubsetStatus>()

/**
 * Map one settled ensure rejection to its status. Exported for tests; the hook is a
 * `useEffect` around this. An unrecognized rejection (a defect) is reported as
 * `Failed` rather than thrown — the UI keeps its retry handle either way.
 */
export const statusFromRejection = (error: unknown, retry: () => void): SubsetStatus => {
  if (error instanceof SubsetForbidden) return SubsetStatus.Forbidden({ error })
  if (error instanceof HydrateFailed) return SubsetStatus.Failed({ error, retry })
  return SubsetStatus.Failed({ error: new HydrateFailed({ reason: String(error) }), retry })
}

/**
 * Ensure one subset of a partial collection for the lifetime of the component — the
 * React face of `utils.loadBy*`. Runs the ensure on mount and whenever the value
 * changes (a Skip after the first load, so re-renders are free); pair it with a
 * `useLiveQuery` filtered on the same key:
 *
 * @example
 * ```tsx
 * const values = templateValues()
 * const subset = usePartialLoad(values, { templateId })
 * const { data } = useLiveQuery(
 *   (q) => q.from({ v: values }).where(({ v }) => eq(v.templateId, templateId)),
 *   [templateId],
 * )
 * return SubsetStatus.$match(subset, {
 *   Loading: () => <Spinner />,          // or data — persisted rows are stale-while-revalidate
 *   Ready: () => <ValueList values={data} />,
 *   Forbidden: () => <NoAccess />,
 *   Failed: ({ error, retry }) => <RetryBanner error={error} onRetry={retry} />,
 * })
 * ```
 *
 * The completeness contract: a query over a partial collection is only complete for
 * subsets you have ensured — ensure and `where` on the same key, as above.
 */
export function usePartialLoad<T extends object, By extends Record<string, (e: T) => string>>(
  collection: PartialLiveCollection<T, By>,
  subset: SubsetOf<T, By>,
): SubsetStatus {
  const keys = Object.keys(subset)
  if (keys.length !== 1) {
    throw new Error(`[usePartialLoad] expected exactly one index key, got: {${keys.join(", ")}}`)
  }
  const indexKey = keys[0]!
  const value = (subset as Record<string, string>)[indexKey]!
  const [status, setStatus] = useState<SubsetStatus>(SubsetStatus.Loading())
  const [attempt, setAttempt] = useState(0)

  useEffect(() => {
    let cancelled = false
    setStatus(SubsetStatus.Loading())
    const method = (collection.utils as Record<string, unknown>)[loadByMethodName(indexKey)]
    if (typeof method !== "function") {
      throw new Error(`[usePartialLoad] "${indexKey}" is not an index key of this collection`)
    }
    ;(method as (value: string) => Promise<void>)(value).then(
      () => {
        if (!cancelled) setStatus(SubsetStatus.Ready())
      },
      (error: unknown) => {
        if (!cancelled) setStatus(statusFromRejection(error, () => setAttempt((n) => n + 1)))
      },
    )
    return () => {
      cancelled = true
    }
  }, [collection, indexKey, value, attempt])

  return status
}
