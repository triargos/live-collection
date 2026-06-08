import {
  Context,
  Effect,
  ExecutionStrategy,
  Exit,
  Layer,
  Option,
  Scope,
} from "effect";
import { type CollectionKey, serializeKey } from "./collection-key.js";

/**
 * The registry's interface — the seam. A long-lived, generic cache of live collection
 * handles keyed by {@link CollectionKey}. It hands out the *canonical* instance for a key
 * (every caller for a given key gets the same object) and owns teardown. It knows nothing
 * about entities, workspaces, or TanStack: a collection's teardown is whatever finalizers
 * its `make` registered.
 */
export interface CollectionRegistryShape {
  /**
   * The canonical instance for `key`, building it on first request and caching it.
   * `make` declares teardown with `Effect.addFinalizer` (so it requires `Scope`); the
   * registry provides a per-collection child scope and discharges that requirement, so
   * `Scope` never leaks into the caller's `R`.
   */
  readonly getOrCreate: <A, R>(args: {
    readonly key: CollectionKey<A>;
    readonly make: Effect.Effect<A, never, R>;
  }) => Effect.Effect<A, never, Exclude<R, Scope.Scope>>;
  /** The instance for `key` if mounted, else `None`. Never builds — a peek, not a get. */
  readonly getById: <A>(
    key: CollectionKey<A>,
  ) => Effect.Effect<Option.Option<A>>;
  /**
   * Every mounted instance for `entity`, across all scopes — one per workspace, plus the global
   * instance if it's mounted — each paired with its {@link CollectionKey} (so callers can read the
   * scope). Empty when none are. Use this to fan over a model rather than a single scope: a `Delete`
   * whose id may live in any scope reads `.collection`; a snapshot reads `.key.scope`.
   */
  readonly getByEntity: <A>(
    entity: string,
  ) => Effect.Effect<ReadonlyArray<{ readonly key: CollectionKey<A>; readonly collection: A }>>;
  /** Tear down and evict one collection. A no-op if it isn't mounted. */
  readonly dispose: (key: CollectionKey<unknown>) => Effect.Effect<void>;
  /** Tear down and evict every collection whose `scope` equals `scope` (globals untouched). */
  readonly disposeScope: (scope: string) => Effect.Effect<void>;
  /** Tear down and evict every *scoped* collection, leaving globals mounted (workspace reset). */
  readonly disposeAllScoped: () => Effect.Effect<void>;
  /** Tear down and evict *every* collection, globals included (logout). */
  readonly disposeAll: () => Effect.Effect<void>;
}

/**
 * The default adapter. Lifetimes are modeled with `Scope`, not manual bookkeeping: each
 * collection is built in its own child scope forked from the registry's layer scope, so
 *
 * - {@link CollectionRegistryShape.dispose} closes one child scope — selective teardown a
 *   single shared scope (LIFO, all-or-nothing) could not express;
 * - releasing the layer closes the parent, which closes every surviving child — an
 *   automatic backstop, with no finalizer loop of our own.
 */
export const makeRegistry: Effect.Effect<CollectionRegistryShape, never, Scope.Scope> =
  Effect.gen(function* () {
    const registryScope = yield* Effect.scope;
    const entries = new Map<
      string,
      {
        readonly collection: unknown;
        readonly childScope: Scope.CloseableScope;
        readonly key: CollectionKey<unknown>;
      }
    >();

    const getOrCreate = <A, R>({
      key,
      make,
    }: {
      readonly key: CollectionKey<A>;
      readonly make: Effect.Effect<A, never, R>;
    }): Effect.Effect<A, never, Exclude<R, Scope.Scope>> =>
      Effect.gen(function* () {
        const id = serializeKey(key);
        const existing = entries.get(id);
        // Type recovery from the heterogeneous store — sound by construction: the key's
        // phantom and the stored value share `A` because both come from one `make` call.
        // Not an IO decode; the value is an in-process object we put here ourselves.
        if (existing !== undefined) return existing.collection as A;
        const childScope = yield* Scope.fork(
          registryScope,
          ExecutionStrategy.sequential,
        );
        const collection = yield* Scope.extend(make, childScope);
        entries.set(id, { collection, childScope, key });
        return collection;
      });

    const getById = <A>(
      key: CollectionKey<A>,
    ): Effect.Effect<Option.Option<A>> =>
      Effect.sync(() => {
        const existing = entries.get(serializeKey(key));
        return existing === undefined
          ? Option.none<A>()
          : Option.some(existing.collection as A); // see getOrCreate note
      });

    const getByEntity = <A>(
      entity: string,
    ): Effect.Effect<ReadonlyArray<{ readonly key: CollectionKey<A>; readonly collection: A }>> =>
      Effect.sync(() =>
        [...entries.values()]
          .filter((e) => e.key.entity === entity)
          .map((e) => ({ key: e.key as CollectionKey<A>, collection: e.collection as A })), // see getOrCreate note
      );

    const evict = (args: {
      readonly id: string;
      readonly entry: { readonly childScope: Scope.CloseableScope };
    }): Effect.Effect<void> =>
      Effect.suspend(() => {
        entries.delete(args.id);
        return Scope.close(args.entry.childScope, Exit.void);
      });

    const dispose = (key: CollectionKey<unknown>): Effect.Effect<void> =>
      Effect.suspend(() => {
        const id = serializeKey(key);
        const entry = entries.get(id);
        return entry === undefined ? Effect.void : evict({ id, entry });
      });

    const disposeScope = (scope: string): Effect.Effect<void> =>
      Effect.forEach(
        [...entries].filter(([, e]) => Option.getOrNull(e.key.scope) === scope),
        ([id, entry]) => evict({ id, entry }),
        { discard: true },
      );

    const disposeAllScoped = (): Effect.Effect<void> =>
      Effect.forEach(
        [...entries].filter(([, e]) => Option.isSome(e.key.scope)),
        ([id, entry]) => evict({ id, entry }),
        { discard: true },
      );

    const disposeAll = (): Effect.Effect<void> =>
      Effect.forEach([...entries], ([id, entry]) => evict({ id, entry }), {
        discard: true,
      });

    return CollectionRegistry.of({
      getOrCreate,
      getById,
      getByEntity,
      dispose,
      disposeScope,
      disposeAllScoped,
      disposeAll,
    });
  });

/** The seam: `yield* CollectionRegistry`. Implementation is {@link CollectionRegistry.layer}. */
export class CollectionRegistry extends Context.Tag("CollectionRegistry")<
  CollectionRegistry,
  CollectionRegistryShape
>() {
  static readonly layer = Layer.scoped(CollectionRegistry, makeRegistry);
}
