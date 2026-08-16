import { Context, Effect, Layer, Option, Schema } from "effect"
import type { ModelDescriptor, ModelId, SyncGroup } from "@triargos/live-collection-protocol"

/**
 * The kernel's runtime form of the app's model registry.
 *
 * Built with the make-pattern: {@link ModelRegistry.layer} takes an *effect*
 * that yields the app's repos once and returns the descriptor record (checked
 * with the protocol's `defineModelRegistry`), so descriptors are plain closures
 * over already-resolved services — `R` lives on the build effect and is
 * inferred natively by Effect:
 *
 * ```ts
 * export const RegistryLayer = ModelRegistry.layer(Effect.gen(function* () {
 *   const todos = yield* TodoRepo
 *   return defineModelRegistry({
 *     Todo: {
 *       modelName: "Todo",
 *       schema: Todo,
 *       hydrate: (id) => todos.find(TodoId.make(id)),
 *     },
 *   })
 * }))
 * // : Layer<ModelRegistry, never, TodoRepo>
 * ```
 *
 * Descriptors are required to be dependency-free (`R = never`): a `hydrate`
 * that still tries to look services up per call is a compile error, steering
 * every registry to the resolve-once shape.
 *
 * The shape holds data-plus-closures — *what* hydrates — never the hydration
 * fold itself. How hydration results are interpreted (`Option.none` → synthetic
 * `Delete`, unknown model → drop, encode failure → skip, batching) is the
 * kernel's internal hydrator, deliberately not substitutable through this tag.
 */
export interface ResolvedModel {
  /** Current entity for one id; `Option.none` ⇒ gone or access lost. */
  readonly hydrate: (
    id: ModelId,
    syncGroups: ReadonlyArray<SyncGroup>
  ) => Effect.Effect<Option.Option<unknown>>
  /** Optional batch variant — one call per model instead of one per event. */
  readonly hydrateMany?: (
    ids: ReadonlyArray<ModelId>,
    syncGroups: ReadonlyArray<SyncGroup>
  ) => Effect.Effect<ReadonlyMap<ModelId, unknown>>
  /** Encode a hydrated entity to its wire form via the descriptor's schema. */
  readonly encode: (value: unknown) => Effect.Effect<unknown, Schema.SchemaError>
  /**
   * One declared partial-index fetch, wire-encoded — present iff the descriptor
   * declares `indexes`. Returns `undefined` for an undeclared `indexKey` (the caller
   * fails the batch loudly); `Option.none` ⇒ visibility refused (`Forbidden` on the
   * wire); `Option.some(rows)` ⇒ the subset's full membership, already encoded via
   * the descriptor schema. A row that fails to encode is a defect: the registry's
   * schema disagrees with its own repo — a config bug, not a runtime condition.
   */
  readonly indexFetch?: (
    indexKey: string,
    keyValue: string,
    syncGroups: ReadonlyArray<SyncGroup>
  ) => Effect.Effect<Option.Option<ReadonlyArray<unknown>>> | undefined
}

export interface ModelRegistryShape {
  readonly models: ReadonlyMap<string, ResolvedModel>
}

const resolve = (registry: Record<string, ModelDescriptor<string, any, never>>): ModelRegistryShape => {
  const models = new Map<string, ResolvedModel>()
  for (const [name, descriptor] of Object.entries(registry)) {
    // Canonical JSON codec: the entity's wire form is guaranteed Json, so types
    // whose plain encoded form isn't JSON-native (Date, Uint8Array, ...) get an
    // explicit serialization instead of whatever JSON.stringify improvises.
    const encodeEntity = Schema.encodeEffect(Schema.toCodecJson(descriptor.schema))
    const indexes = descriptor.indexes
    models.set(name, {
      hydrate: descriptor.hydrate,
      encode: (value) => encodeEntity(value),
      ...(descriptor.hydrateMany !== undefined ? { hydrateMany: descriptor.hydrateMany } : {}),
      ...(indexes !== undefined
        ? {
            indexFetch: (
              indexKey: string,
              keyValue: string,
              syncGroups: ReadonlyArray<SyncGroup>
            ) => {
              const fetch = indexes[indexKey]
              if (fetch === undefined) return undefined
              return fetch(keyValue, syncGroups).pipe(
                Effect.flatMap(
                  Option.match({
                    onNone: () => Effect.succeedNone,
                    onSome: (rows) =>
                      Effect.forEach(rows, (row) => encodeEntity(row)).pipe(Effect.orDie, Effect.asSome)
                  })
                )
              )
            }
          }
        : {})
    })
  }
  return { models }
}

export class ModelRegistry extends Context.Service<ModelRegistry, ModelRegistryShape>()(
  "live-collection-server/ModelRegistry"
) {
  /**
   * Lift a registry build effect into the kernel's layer graph. The effect's
   * requirements (the repos it yields) become the layer's requirements.
   */
  static readonly layer = <R>(
    build: Effect.Effect<Record<string, ModelDescriptor<string, any, never>>, never, R>
  ): Layer.Layer<ModelRegistry, never, R> =>
    Layer.effect(ModelRegistry, Effect.map(build, resolve))
}
