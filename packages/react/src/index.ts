/**
 * `@triargos/live-collection-react` — optional React bindings.
 *
 * Keeps the core framework-neutral; non-React apps never install `react`.
 * The real hook + provider land with the client read path (TASKS.md A.6+).
 *
 * This file is the package skeleton.
 */
import type { LiveCollection } from "@triargos/live-collection"

/**
 * Subscribe a component to a live collection's current rows.
 *
 * TODO(A.6/A.9): implement over `@tanstack/react-db` + the runtime provider.
 */
export type UseLiveCollection = <T extends object>(collection: LiveCollection<T>) => ReadonlyArray<T>
