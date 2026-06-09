import { Effect, type ManagedRuntime, Option, Schema, type Scope } from "effect"
import {
  createCollection,
  type DeleteMutationFnParams,
  type InsertMutationFnParams,
  type UpdateMutationFnParams,
} from "@tanstack/db"
import { persistedCollectionOptions } from "@tanstack/db-sqlite-persistence-core"
import type { ModelId } from "@triargos/live-collection-protocol"
import type { SyncWrite } from "../dispatch/sync-write.js"
import type { LiveCollection } from "../persistence/live-collection.js"
import { liveCollectionOptions } from "../persistence/live-collection-options.js"
import { deriveSchemaVersion } from "../persistence/schema-version.js"
import type { LiveRuntime } from "../runtime/live-runtime.js"
import { type CollectionKey, globalKey, scopedKey, serializeKey } from "./collection-key.js"

/**
 * Everything the sync loop needs to drive one model, carried *on* the collection handle (DEC-R5) so
 * the {@link SyncMap} is a literal `{ ModelName: collection }` with no duplicated `schema`/`scopeOf`.
 * `scopeOf` is `(entity) => scope` (DEC-R6): `None` ⇒ global (one instance), `Some` ⇒ the dispatcher
 * reads the scope straight off the event. `listFn` is the cold/resync snapshot source — already
 * **bridged to `R = never`** here (the app's `services` runtime is provided into it at define time), so
 * the loop yields it directly with no app-service dependency.
 */
export interface ModelMeta<T extends object> {
  readonly entity: string
  readonly schema: Schema.Schema<T, any, never>
  readonly getKey: (entity: T) => ModelId
  readonly scopeOf: Option.Option<(entity: T) => string>
  readonly listFn: (scope: Option.Option<string>) => Effect.Effect<ReadonlyArray<T>>
}

/** A global collection handle: `webhookCollection()` mounts/returns the single instance. */
export type GlobalHandle<T extends object> = (() => LiveCollection<T>) & { readonly _meta: ModelMeta<T> }
/** A scoped collection handle: `webhookCollection(orgId)` mounts/returns the per-scope instance. */
export type ScopedHandle<T extends object> = ((scope: string) => LiveCollection<T>) & {
  readonly _meta: ModelMeta<T>
}
export type Handle<T extends object> = GlobalHandle<T> | ScopedHandle<T>

/**
 * The explicit model→collection wiring passed to `useLiveSync`/`syncLoop` (DEC-R5). Keyed by wire
 * model name; the value is the collection handle, whose `_meta` drives decode/route/snapshot. Only
 * `_meta` is read here — the loop reaches instances through the registry, never by calling the handle.
 */
export type SyncMap = Record<string, { readonly _meta: ModelMeta<any> }>

/**
 * Optional **optimistic write path** (A.10). These are TanStack DB's native mutation params, but
 * **Effect-returning** with the app's `R` (discharged by `services`) — so the app writes a pure Effect
 * (`yield* SomeApi`) with no `runPromise` boilerplate.
 *
 * The handler's only job is the **server call**: `onInsert`/`onUpdate` call your backend and **return
 * the server-confirmed row**; `onDelete` calls your backend and returns `void`. The library reconciles
 * for you — it folds the returned row into the synced baseline (`writeSynced`), or removes the row by
 * key (`deleteSynced`), in {@link bridge} **before the mutation resolves** (Model B). Apps never touch
 * `collection.utils`: the synced store holds the row at the instant TanStack drops the completed
 * optimistic transaction, so there is no flicker, and the eventual SSE echo of the same row is an
 * idempotent `writeSynced`. A failed Effect rejects the mutation ⇒ TanStack rolls the optimistic write
 * back (the reconcile runs only on success). Ids are the app's (client-minted recommended: the
 * self-echo stays idempotent and no temp-id swap is needed — DEC-8).
 *
 * Insert/update must return `T` (no `void` opt-out): without the confirmed row the library can't
 * reconcile, and an unreconciled optimistic row flickers. One mutation per transaction — the library
 * reconciles `mutations[0]`, and a batched transaction dies with {@link BatchedMutationsUnsupported}
 * before the server is called (DEC-W2); array-batch reconcile is a future pass.
 */
interface MutationHandlers<T extends object, R> {
  readonly onInsert?: (params: InsertMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<T, unknown, R>
  readonly onUpdate?: (params: UpdateMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<T, unknown, R>
  readonly onDelete?: (params: DeleteMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<void, unknown, R>
}

/**
 * Defect raised when a bridged mutation handler receives a transaction with more than one mutation.
 * The library reconciles exactly `mutations[0]`'s confirmed row (DEC-W2), so a batch would silently
 * lose rows 2..n the instant the optimistic transaction drops — fail the whole transaction loudly
 * instead, before any server call. Split the writes, or wait for the batch-reconcile pass.
 */
export class BatchedMutationsUnsupported extends Schema.TaggedError<BatchedMutationsUnsupported>()(
  "BatchedMutationsUnsupported",
  { entity: Schema.String, mutationCount: Schema.Number },
) {}

/**
 * The app-services runtime that discharges the `R` of `listFn` + the mutation handlers (DEC-A10, the
 * elternportal `runtime` analogue). **Required iff `R ≠ never`**; a collection whose `listFn`/handlers
 * need no services omits it. `LiveRuntime` stays infra-only (non-generic) — `R` lives only here.
 */
type ServicesOf<R> = [R] extends [never]
  ? { readonly services?: ManagedRuntime.ManagedRuntime<never, never> }
  : { readonly services: ManagedRuntime.ManagedRuntime<R, never> }

interface GlobalBase<T extends object, R> {
  readonly runtime: LiveRuntime
  readonly entity: string
  readonly schema: Schema.Schema<T, any, never>
  readonly getKey: (entity: T) => ModelId
  readonly listFn: Effect.Effect<ReadonlyArray<T>, never, R>
}
interface ScopedBase<T extends object, R> {
  readonly runtime: LiveRuntime
  readonly entity: string
  readonly schema: Schema.Schema<T, any, never>
  readonly getKey: (entity: T) => ModelId
  readonly scopeOf: (entity: T) => string
  readonly listFn: (scope: string) => Effect.Effect<ReadonlyArray<T>, never, R>
}
type GlobalConfig<T extends object, R> = GlobalBase<T, R> & MutationHandlers<T, R> & ServicesOf<R>
type ScopedConfig<T extends object, R> = ScopedBase<T, R> & MutationHandlers<T, R> & ServicesOf<R>

/**
 * Define one model's collection. Returns a **runtime-bound, registry-backed handle** (DEC-R2): calling
 * it mounts through the registry (sync, cached by `(entity, scope)`) and returns the **native**
 * `LiveCollection<T>` — pass that straight to `useLiveQuery`. Calling the handle inline in render is
 * cheap and referentially stable (a `Map.get` after first mount).
 *
 * Two overloads (mirrors the global/scoped split): `scopeOf` present ⇒ scoped `(scope) => Collection`;
 * absent ⇒ global `() => Collection`. The model name is written once, so the registry key and the
 * persisted table id (`serializeKey(key)`) can never drift.
 *
 * `R` is inferred from `listFn` + the optional mutation handlers; `services` discharges it (A.10).
 */
export function defineCollection<T extends object, R = never>(config: GlobalConfig<T, R>): GlobalHandle<T>
export function defineCollection<T extends object, R = never>(config: ScopedConfig<T, R>): ScopedHandle<T>
export function defineCollection<T extends object, R = never>(
  config: GlobalConfig<T, R> | ScopedConfig<T, R>,
): Handle<T> {
  const { runtime, entity, schema, getKey } = config
  const scopeOf = "scopeOf" in config ? config.scopeOf : undefined
  const schemaVersion = deriveSchemaVersion(schema)

  // The `services` ManagedRuntime IS the executor for everything carrying the app's `R` — handlers run
  // ON it (`runPromise`), the loop-facing listFn runs WITH it (`Effect.provide`). It is never forced to
  // build synchronously: the runtime builds lazily on first use and memoizes, so async-constructing
  // layers work and a disposed runtime fails loudly instead of serving finalized services. When
  // `services` is absent, `R` is `never` (see ServicesOf), so running on the default runtime is sound —
  // the casts below are that contract, stated once per seam. (DEC-W3)
  const services = config.services as ManagedRuntime.ManagedRuntime<R, never> | undefined

  // Bridge an Effect handler (with R) to the native TanStack handler (a Promise): run the handler, then
  // reconcile its result into the synced baseline (Model B) — both before the Promise resolves, so the
  // synced row is in place when TanStack drops the completed optimistic tx. `flatMap` short-circuits on
  // failure ⇒ the reconcile never runs and the rejection rolls the optimistic mutation back. A batched
  // transaction dies before the handler runs — only `mutations[0]` would be reconciled (DEC-W2).
  const bridge =
    <P extends { readonly transaction: { readonly mutations: ReadonlyArray<unknown> } }, A>(
      handler: (params: P) => Effect.Effect<A, unknown, R>,
      reconcile: (params: P, result: A) => Effect.Effect<void>,
    ) =>
    (params: P): Promise<void> => {
      const composed =
        params.transaction.mutations.length > 1
          ? Effect.die(new BatchedMutationsUnsupported({ entity, mutationCount: params.transaction.mutations.length }))
          : handler(params).pipe(Effect.flatMap((result) => reconcile(params, result)))
      return services
        ? services.runPromise(composed)
        : Effect.runPromise(composed as Effect.Effect<void, unknown>)
    }

  // Build the native collection. Sync (`createCollection`), with `persistence` a closed-over VALUE
  // (not a context dep) and only `Scope` required (for `cleanup`), which the registry discharges —
  // so the mount path is `Effect.runSync`-able with no async boundary (DEC-R8).
  const makeFor = (key: CollectionKey<LiveCollection<T>>): Effect.Effect<LiveCollection<T>, never, Scope.Scope> =>
    Effect.sync(
      () =>
        createCollection(
          persistedCollectionOptions<T, ModelId, never, SyncWrite<T>>({
            persistence: runtime.persistence,
            id: serializeKey(key),
            schemaVersion,
            ...liveCollectionOptions({ getKey }),
            // Omit absent handlers entirely (exactOptionalPropertyTypes forbids an explicit `undefined`).
            // Insert/update reconcile the returned confirmed row; delete reconciles by the mutation key.
            ...(config.onInsert ? { onInsert: bridge(config.onInsert, (p, row) => p.collection.utils.writeSynced(row)) } : {}),
            ...(config.onUpdate ? { onUpdate: bridge(config.onUpdate, (p, row) => p.collection.utils.writeSynced(row)) } : {}),
            ...(config.onDelete ? { onDelete: bridge(config.onDelete, (p) => p.collection.utils.deleteSynced(p.transaction.mutations[0]!.key)) } : {}),
          }),
        ) satisfies LiveCollection<T>,
    ).pipe(
      Effect.tap((collection) => Effect.addFinalizer(() => Effect.promise(() => collection.cleanup()))),
    )

  const mount = (key: CollectionKey<LiveCollection<T>>): LiveCollection<T> =>
    Effect.runSync(runtime.registry.getOrCreate({ key, make: makeFor(key) }))

  // Bridge listFn to `R = never` for the loop. The loop is itself an Effect fiber, so this stays in
  // Effect-land: `Effect.provide(services)` runs the listFn with the services runtime (the same
  // memoized runtime `runPromise` uses) without a promise detour — interruption of an in-flight
  // snapshot and cause structure are preserved.
  const provideServices = <A>(eff: Effect.Effect<A, never, R>): Effect.Effect<A> =>
    services ? eff.pipe(Effect.provide(services)) : (eff as Effect.Effect<A>)

  const meta: ModelMeta<T> = {
    entity,
    schema,
    getKey,
    scopeOf: Option.fromNullable(scopeOf),
    listFn:
      scopeOf === undefined
        ? () => provideServices((config as GlobalBase<T, R>).listFn)
        : (scope) =>
            Option.match(scope, {
              onNone: () => Effect.die(`[defineCollection] scoped "${entity}" snapshot with no scope`),
              onSome: (s) => provideServices((config as ScopedBase<T, R>).listFn(s)),
            }),
  }

  const handle =
    scopeOf === undefined
      ? () => mount(globalKey<LiveCollection<T>>(entity))
      : (scope: string) => mount(scopedKey<LiveCollection<T>>({ entity, scope }))

  return Object.assign(handle, { _meta: meta }) as Handle<T>
}
