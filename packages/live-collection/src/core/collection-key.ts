import { Option } from "effect"

/** One partial-index subset's address within a collection: `indexKey = keyValue`. */
export interface SubsetKey {
  readonly indexKey: string
  readonly keyValue: string
}

/**
 * The structured identity of a live collection in the registry — `entity` (the model
 * name) and `scope` (`None` for a global collection, `Some` for one scoped to a
 * workspace/org/etc.) — which are exactly the dimensions disposal matches on. There is
 * deliberately **no string grammar**: the library never parses an id, so there is no
 * separator, no glob, no escaping.
 *
 * `subset` is `Some` only for a **coverage mark** — the durable "this partial subset's
 * rows reflect the server through N" record — never for a registry instance: partial
 * collections share one instance per `(entity, scope)` and track subsets as marks.
 *
 * `A` is a phantom type parameter carrying the collection's instance type. Keys are
 * minted by `defineCollection`, so key ↔ instance-type stays 1:1 by construction.
 */
export interface CollectionKey<A> {
  readonly entity: string
  readonly scope: Option.Option<string>
  readonly subset: Option.Option<SubsetKey>
  readonly _A?: A // phantom; never assigned, zero runtime cost
}

/** A global collection — one instance app-wide, no scope suffix (e.g. the current user). */
export const globalKey = <A>(entity: string): CollectionKey<A> => ({
  entity,
  scope: Option.none(),
  subset: Option.none()
})

/** A scoped collection — one instance per scope key (e.g. `webhook` within an org). */
export const scopedKey = <A>(args: {
  readonly entity: string
  readonly scope: string
}): CollectionKey<A> => ({
  entity: args.entity,
  scope: Option.some(args.scope),
  subset: Option.none()
})

/** A partial subset's coverage mark — not a registry instance (see {@link CollectionKey}). */
export const subsetKey = <A>(args: {
  readonly entity: string
  readonly scope: Option.Option<string>
  readonly subset: SubsetKey
}): CollectionKey<A> => ({
  entity: args.entity,
  scope: args.scope,
  subset: Option.some(args.subset)
})

/**
 * An injective string form used only as `Map`/record keys. It is **never parsed back**
 * — all disposal logic reads the structured fields — so the encoding has no contract
 * beyond being collision-free over `(entity, scope, subset)` — and one freeze:
 *
 * COMPAT — the `subset: None` spelling is byte-identical to the pre-partial-index
 * two-element form. Existing clients' durable last-applied marks are stored under it;
 * changing it would orphan them all (a silent global reset), so it stays.
 */
export const serializeKey = (key: CollectionKey<unknown>): string =>
  Option.match(key.subset, {
    onNone: () => JSON.stringify([key.entity, Option.getOrNull(key.scope)]),
    onSome: (subset) =>
      JSON.stringify([key.entity, Option.getOrNull(key.scope), subset.indexKey, subset.keyValue])
  })
