import { Option } from "effect"

/**
 * The structured identity of a live collection in the registry. Two dimensions and
 * nothing else — `entity` (the model name) and `scope` (`None` for a global collection,
 * `Some` for one scoped to a workspace/org/etc.) — which are exactly the dimensions
 * disposal matches on. There is deliberately **no string grammar**: the library never
 * parses an id, so there is no separator, no glob, no escaping (the same structure-over-
 * sentinels choice the protocol made for resync targets).
 *
 * `A` is a phantom carrying the decoded entity type, so {@link CollectionRegistry.getById}
 * recovers it without an unchecked decode. Keys are minted only by the collection factory
 * (`defineCollection`), so key ↔ entity-type stays 1:1 by construction.
 */
export interface CollectionKey<A> {
  readonly entity: string
  readonly scope: Option.Option<string>
  readonly _A?: A // phantom; never assigned, zero runtime cost
}

/** A global collection — one instance app-wide, no scope suffix (e.g. the current user). */
export const globalKey = <A>(entity: string): CollectionKey<A> => ({
  entity,
  scope: Option.none()
})

/** A scoped collection — one instance per scope key (e.g. `webhook` within an org). */
export const scopedKey = <A>(args: {
  readonly entity: string
  readonly scope: string
}): CollectionKey<A> => ({
  entity: args.entity,
  scope: Option.some(args.scope)
})

/**
 * An injective string form used only as the registry `Map` key. It is **never parsed
 * back** — all disposal logic reads the structured fields — so the encoding has no
 * contract beyond being collision-free over `(entity, scope)`.
 */
export const serializeKey = (key: CollectionKey<unknown>): string =>
  JSON.stringify([key.entity, Option.getOrNull(key.scope)])
