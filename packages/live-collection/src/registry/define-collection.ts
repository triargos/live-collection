import { Effect, Effectable, Scope } from "effect"
import { CollectionRegistry } from "./collection-registry.js"
import { type CollectionKey, globalKey, scopedKey } from "./collection-key.js"

/**
 * A mountable handle for one collection instance — the typed skin over the untyped
 * {@link CollectionRegistry}. It is yieldable: `yield* ref` mounts the collection via the
 * registry (building once, caching by key) and returns the *canonical* instance. It also
 * carries {@link MountRef.key} so the dispatch resolver and `registry.getById` can locate
 * what's mounted without rebuilding.
 *
 * A ref is identity-*by-key*, not by object: `webhookCollection(orgId)` mints a fresh, inert
 * ref every call (just a key + an unstarted `make`), and the registry collapses them to one
 * instance. So it is safe to call inline in a render or a lookup.
 *
 * `R` is the *raw* requirement of `make` — it includes `Scope` because `make` declares its
 * teardown with `Effect.addFinalizer`. The registry provides the child scope and discharges
 * that requirement, so the yielded effect surfaces `CollectionRegistry | Exclude<R, Scope>`
 * (the same Exclude trick `getOrCreate` uses). Construction is internal — apps reach a ref
 * only through {@link defineCollection}.
 */
export class MountRef<A, R> extends Effectable.Class<
  A,
  never,
  CollectionRegistry | Exclude<R, Scope.Scope>
> {
  readonly key: CollectionKey<A>
  private readonly make: Effect.Effect<A, never, R>

  constructor(args: {
    readonly key: CollectionKey<A>
    readonly make: Effect.Effect<A, never, R>
  }) {
    super()
    this.key = args.key
    this.make = args.make
  }

  commit(): Effect.Effect<A, never, CollectionRegistry | Exclude<R, Scope.Scope>> {
    // Capture fields out here: inside an Effect.gen `this` is the generator's, not the
    // instance's. The registry caches by key, so repeat mounts collapse to one instance.
    const { key, make } = this
    return CollectionRegistry.pipe(
      Effect.flatMap((registry) => registry.getOrCreate({ key, make })),
    )
  }
}

/**
 * One entity's collection definition — the per-entity input an app writes once.
 *
 * `scopeOf` is the app-owned map from its domain args to the generic `scope` key (e.g.
 * `(orgId) => orgId`); this is the *only* place the app's notion of "workspace" appears — the
 * library never learns the word. Omit `scopeOf` for a global collection (one instance app-wide).
 * `Args` flows uniformly into both `scopeOf` and `make`.
 */
export interface CollectionDef<A, Args, R> {
  readonly entity: string
  readonly scopeOf?: (args: Args) => string
  readonly make: (args: Args) => Effect.Effect<A, never, R>
}

/** The typed entry point an app calls per request: `webhookCollection(orgId)`. */
export type MountCollection<A, Args, R> = (args: Args) => MountRef<A, R>

/**
 * Turn a per-entity {@link CollectionDef} into the typed entry point. Present `scopeOf` ⇒ a
 * scoped key (`scopedKey`); absent ⇒ a global key (`globalKey`). The entity name is written
 * exactly once, so the key and the `make` can never drift to different entities.
 *
 * Two overloads, because the two forms have genuinely different call shapes: a global def
 * takes no args, so its entry point is `() => MountRef`; a scoped def's `scopeOf`/`make` pin
 * `Args`, so its entry point is `(args) => MountRef`. (One signature with `scopeOf?` can't
 * express "no `scopeOf` ⇒ no arg" — `Args` would infer to `unknown` and force a phantom arg.)
 */
export function defineCollection<A, R>(def: {
  readonly entity: string
  readonly make: () => Effect.Effect<A, never, R>
}): MountCollection<A, void, R>
export function defineCollection<A, Args, R>(def: {
  readonly entity: string
  readonly scopeOf: (args: Args) => string
  readonly make: (args: Args) => Effect.Effect<A, never, R>
}): MountCollection<A, Args, R>
export function defineCollection<A, Args, R>(
  def: CollectionDef<A, Args, R>,
): MountCollection<A, Args, R> {
  const scopeOf = def.scopeOf
  return (args) =>
    new MountRef({
      key:
        scopeOf === undefined
          ? globalKey<A>(def.entity)
          : scopedKey<A>({ entity: def.entity, scope: scopeOf(args) }),
      make: def.make(args),
    })
}
