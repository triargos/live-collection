import { Schema } from "effect"
import type { NonEmptyReadonlyArray } from "effect/Array"

/**
 * A routing key for sync events, such as `"organization:abc"` or `"session:x7k2"`.
 * Groups are opaque to the protocol: delivery and resync supersession match by exact
 * equality only, so any internal structure (like `:`-delimited paths) is purely an
 * application naming convention with no protocol semantics. Events always carry
 * concrete groups; the value itself has no wildcards.
 */
export const SyncGroup = Schema.NonEmptyString.pipe(Schema.brand("SyncGroup"))
export type SyncGroup = typeof SyncGroup.Type

/**
 * Naming convenience: joins path-like segments with `:`. The protocol attaches no
 * meaning to the result's structure — `deriveGroup(["organization", orgId])` and a
 * hand-written `"organization:abc"` are equal exactly when their strings are.
 */
export const deriveGroup = (
  segments: NonEmptyReadonlyArray<string>
): SyncGroup => SyncGroup.make(segments.join(":"))

/**
 * Whether two sets of groups share at least one group. This is the delivery test: an
 * event reaches a subscriber when the event's groups intersect the subscriber's.
 * Matching is by exact equality — a structurally "nested" group name does not match
 * its prefix — so a private sub-group can't leak to members of a broader one.
 *
 * @example
 * ```ts
 * // event.syncGroups vs. the groups this subscriber may see:
 * if (intersects(event.syncGroups, subscriberGroups)) deliver(event)
 * ```
 */
export const intersects = (
  a: ReadonlyArray<SyncGroup>,
  b: ReadonlyArray<SyncGroup>
): boolean => {
  const seen = new Set<string>(a)
  return b.some((g) => seen.has(g))
}
