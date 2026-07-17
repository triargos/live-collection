import { Context, Effect, Exit, Layer, Option, Scope } from "effect"
import { type CollectionKey, serializeKey } from "./collection-key.js"

/**
 * The collection lifetime table. It returns one canonical instance per key and owns the
 * child scope that tears that instance down. Sync routing belongs to SyncBroker; this
 * service only handles instance deduplication and selective disposal.
 */
export interface CollectionRegistryShape {
  readonly getOrCreate: <A, R>(args: {
    readonly key: CollectionKey<A>
    readonly make: Effect.Effect<A, never, R>
  }) => Effect.Effect<A, never, Exclude<R, Scope.Scope>>
  readonly dispose: (key: CollectionKey<unknown>) => Effect.Effect<void>
  readonly disposeScope: (scope: string) => Effect.Effect<void>
  readonly disposeAllScoped: () => Effect.Effect<void>
  readonly disposeAll: () => Effect.Effect<void>
}

/** Build the scope-backed lifetime table used by the default layer. */
export const makeRegistry: Effect.Effect<CollectionRegistryShape, never, Scope.Scope> = Effect.gen(function* () {
  const registryScope = yield* Effect.scope
  const entries = new Map<
    string,
    {
      readonly collection: unknown
      readonly childScope: Scope.Closeable
      readonly key: CollectionKey<unknown>
    }
  >()

  const getOrCreate = <A, R>({
    key,
    make,
  }: {
    readonly key: CollectionKey<A>
    readonly make: Effect.Effect<A, never, R>
  }): Effect.Effect<A, never, Exclude<R, Scope.Scope>> =>
    Effect.gen(function* () {
      const id = serializeKey(key)
      const existing = entries.get(id)
      if (existing !== undefined) return existing.collection as A
      const childScope = yield* Scope.fork(registryScope, "sequential")
      const collection = yield* Scope.provide(make, childScope)
      entries.set(id, { collection, childScope, key })
      return collection
    })

  const evict = (args: {
    readonly id: string
    readonly entry: { readonly childScope: Scope.Closeable }
  }): Effect.Effect<void> =>
    Effect.suspend(() => {
      entries.delete(args.id)
      return Scope.close(args.entry.childScope, Exit.void)
    })

  const dispose = (key: CollectionKey<unknown>): Effect.Effect<void> =>
    Effect.suspend(() => {
      const id = serializeKey(key)
      const entry = entries.get(id)
      return entry === undefined ? Effect.void : evict({ id, entry })
    })

  const disposeScope = (scope: string): Effect.Effect<void> =>
    Effect.forEach(
      [...entries].filter(([, entry]) => Option.getOrNull(entry.key.scope) === scope),
      ([id, entry]) => evict({ id, entry }),
      { discard: true },
    )

  const disposeAllScoped = (): Effect.Effect<void> =>
    Effect.forEach(
      [...entries].filter(([, entry]) => Option.isSome(entry.key.scope)),
      ([id, entry]) => evict({ id, entry }),
      { discard: true },
    )

  const disposeAll = (): Effect.Effect<void> =>
    Effect.forEach([...entries], ([id, entry]) => evict({ id, entry }), { discard: true })

  return CollectionRegistry.of({ getOrCreate, dispose, disposeScope, disposeAllScoped, disposeAll })
})

export class CollectionRegistry extends Context.Service<
  CollectionRegistry,
  CollectionRegistryShape
>()("CollectionRegistry") {
  static readonly layer = Layer.effect(CollectionRegistry, makeRegistry)
}
