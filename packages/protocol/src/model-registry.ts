import { Effect, Option, Result, Schema } from "effect"
import { ModelId, ModelName } from "./ids.js"
import { SyncGroup } from "./sync-group.js"

/**
 * Maps each model name to how its entities are validated and fetched, and gives an
 * app a closed union of its known model names.
 *
 * On the wire a model name is just a string, so a newer backend can emit a name an
 * older client doesn't recognize. Inside an app it's a closed union derived from the
 * registry, and {@link narrowModelName} is the single checkpoint that turns one into
 * the other.
 *
 * `ModelDescriptor` is a plain type with no runtime footprint; the backend supplies
 * the implementations.
 */

/**
 * Describes one synced model: how to decode its entities and how to fetch a current
 * snapshot when hydrating an event.
 *
 * `syncGroups` is the caller's current visibility capability set. Hydration is the
 * second, authoritative visibility check: the event-level group filter uses the groups
 * stamped on the event when it was logged, but access may have changed since — so
 * `hydrate` must return `Option.none()` for an entity the caller can no longer see,
 * which the dispatcher emits as a Delete. Auth context beyond the group set (the
 * caller's principal, sessions, …) travels through `R` as backend-owned services, not
 * through this signature.
 *
 * @typeParam Name - the model's name as a string literal
 * @typeParam T - the decoded entity type
 * @typeParam R - services required to hydrate it
 */
export interface ModelDescriptor<Name extends string, T, R> {
  readonly modelName: Name
  // `any` is the schema's Encoded slot — it accepts a schema with any wire shape that
  // decodes to `T`.
  readonly schema: Schema.Codec<T, any, R, R>
  readonly hydrate: (
    id: ModelId,
    syncGroups: ReadonlyArray<SyncGroup>
  ) => Effect.Effect<Option.Option<T>, never, R>
  // Optional batch variant, to avoid N+1 fetches when hydrating many ids at once.
  readonly hydrateMany?: (
    ids: ReadonlyArray<ModelId>,
    syncGroups: ReadonlyArray<SyncGroup>
  ) => Effect.Effect<ReadonlyMap<ModelId, T>, never, R>
}

/**
 * Defines a model registry, checking that every descriptor's `modelName` equals its
 * key — a mismatched or mistyped name is a compile error. The result's keys form the
 * app's model-name union: `type ModelName = keyof typeof registry`.
 *
 * @example
 * ```ts
 * const registry = defineModelRegistry({
 *   Webhook: {
 *     modelName: "Webhook", // must equal the key — checked at compile time
 *     schema: Webhook,
 *     hydrate: (id, ctx) => webhookRepo.findVisible(id, ctx),
 *   },
 * })
 * type AppModelName = keyof typeof registry // "Webhook" | …
 * ```
 */
export const defineModelRegistry = <
  const R extends Record<string, ModelDescriptor<string, any, any>>
>(
  r: { [K in keyof R]: R[K] & ModelDescriptor<K & string, any, any> }
): R => r

/** Raised when a wire model name isn't in the registry. */
export class UnknownModelError extends Schema.TaggedErrorClass<UnknownModelError>()(
  "UnknownModelError",
  {
    modelName: ModelName, // the unrecognized wire name
    known: Schema.Array(ModelName) // the names this registry knows
  }
) {}

/**
 * Resolves a wire model name against the app's known names. Returns `Success(name)`
 * with the narrowed literal when it's registered, or `Failure(UnknownModelError)` when
 * it isn't — letting the caller skip the event instead of failing the stream, so a
 * client stays forward-compatible with a backend that knows more models than it does.
 *
 * @example
 * ```ts
 * const knownNames = Object.keys(registry) as Array<keyof typeof registry>
 *
 * Result.match(narrowModelName(knownNames, event.modelName), {
 *   onFailure: () => Effect.logDebug(`skipping unknown model ${event.modelName}`),
 *   onSuccess: (name) => dispatch(registry[name], event), // name: "Webhook" | …
 * })
 * ```
 */
export const narrowModelName = <N extends string>(
  known: ReadonlyArray<N>,
  raw: ModelName
): Result.Result<N, UnknownModelError> => {
  const match = known.find((name) => name === String(raw))
  return match !== undefined
    ? Result.succeed(match)
    : Result.fail(
        new UnknownModelError({
          modelName: raw,
          known: known.map((name) => ModelName.make(name))
        })
      )
}
