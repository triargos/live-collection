import { Context, Effect, Layer, type Option, type ParseResult, Schema } from "effect"
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
  readonly encode: (value: unknown) => Effect.Effect<unknown, ParseResult.ParseError>
}

export interface ModelRegistryShape {
  readonly models: ReadonlyMap<string, ResolvedModel>
}

const resolve = (registry: Record<string, ModelDescriptor<string, any, never>>): ModelRegistryShape => {
  const models = new Map<string, ResolvedModel>()
  for (const [name, descriptor] of Object.entries(registry)) {
    // Encodes to the schema's own Encoded form, which v3 keeps JSON-native for the
    // common cases (`Schema.Date` encodes to an ISO string here, unlike v4's
    // Date-instance-only `Schema.Date` — which is why v4 needs an explicit Json codec
    // and this does not). A descriptor schema whose Encoded form is *not* JSON-native
    // (`DateFromSelf`, `Uint8Array`) still reaches the wire as whatever JSON.stringify
    // improvises; declare a JSON-native encoded type instead of relying on that.
    const encodeEntity = Schema.encode(descriptor.schema)
    models.set(name, {
      hydrate: descriptor.hydrate,
      encode: (value) => encodeEntity(value),
      ...(descriptor.hydrateMany !== undefined ? { hydrateMany: descriptor.hydrateMany } : {})
    })
  }
  return { models }
}

export class ModelRegistry extends Context.Tag("live-collection-server/ModelRegistry")<
  ModelRegistry,
  ModelRegistryShape
>() {
  /**
   * Lift a registry build effect into the kernel's layer graph. The effect's
   * requirements (the repos it yields) become the layer's requirements.
   */
  static readonly layer = <R>(
    build: Effect.Effect<Record<string, ModelDescriptor<string, any, never>>, never, R>
  ): Layer.Layer<ModelRegistry, never, R> =>
    Layer.effect(ModelRegistry, Effect.map(build, resolve))
}
