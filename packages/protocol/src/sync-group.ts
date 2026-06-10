import { String as Str, Schema } from "effect"
import type { NonEmptyReadonlyArray } from "effect/Array"

/**
 * A routing key for sync events: a `:`-delimited path of non-empty segments, such as
 * `"organization:abc"` or `"organization:abc:channel:xyz"`. Groups are purely
 * structural — what the segments mean (`organization`, `channel`, `user`, …) is up to
 * the application. Events always carry concrete groups; the value itself has no
 * wildcards.
 */
export const SyncGroup = Schema.NonEmptyString.pipe(
  Schema.filter(
    (s) =>
      s.split(":").every((segment) => segment.length > 0) ||
      "every ':'-delimited segment must be non-empty",
    { identifier: "SyncGroup" }
  ),
  Schema.brand("SyncGroup")
)
export type SyncGroup = typeof SyncGroup.Type

/**
 * Builds a group from its path segments. Inverse of {@link parseGroup}.
 *
 * @example
 * ```ts
 * deriveGroup(["organization", orgId])                  // "organization:abc"
 * deriveGroup(["organization", orgId, "channel", chId]) // "organization:abc:channel:xyz"
 * ```
 */
export const deriveGroup = (
  segments: NonEmptyReadonlyArray<string>
): SyncGroup => SyncGroup.make(segments.join(":"))

/** Splits a group into its path segments. Inverse of {@link deriveGroup}. */
export const parseGroup = (
  g: SyncGroup
): { readonly segments: NonEmptyReadonlyArray<string> } => ({
  segments: Str.split(":")(g)
})

/**
 * Whether two sets of groups share at least one group. This is the delivery test: an
 * event reaches a subscriber when the event's groups intersect the subscriber's.
 * Matching is by exact equality and is never hierarchical — a child group does not
 * match its parent — so a private sub-group can't leak to members of a broader one.
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

/**
 * Whether `group` lies within `scope` by segment-prefix, including equality:
 *
 * - `isUnder("organization:abc", "organization:abc")` → `true`
 * - `isUnder("organization:abc", "organization:abc:channel:xyz")` → `true`
 * - `isUnder("organization:abc", "organization:abcd")` → `false` (matched per segment, not by substring)
 *
 * Use it to test whether a group falls under a broader scope — for instance, which
 * groups a resync target should clear.
 */
export const isUnder = (scope: SyncGroup, group: SyncGroup): boolean => {
  const scopeSegments = scope.split(":")
  const groupSegments = group.split(":")
  if (groupSegments.length < scopeSegments.length) return false
  return scopeSegments.every((segment, i) => segment === groupSegments[i])
}
