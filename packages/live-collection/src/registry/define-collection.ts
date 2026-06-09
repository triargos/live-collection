import { Context, Effect, ManagedRuntime, Option, type Schema, type Scope } from "effect"
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
 * (`yield* SomeApi`) with no `runPromise` boilerplate. To reconcile, the handler calls
 * `collection.utils.writeSynced(confirmed)` **before it resolves** (Model B): the synced store then
 * holds the row at the instant TanStack drops the completed optimistic transaction, so there is no
 * flicker, and the eventual SSE echo of the same row is an idempotent `writeSynced`. A failed Effect
 * rejects the mutation ⇒ TanStack rolls the optimistic write back. Ids are the app's (client-minted
 * recommended: the self-echo stays idempotent and no temp-id swap is needed — DEC-8).
 */
interface MutationHandlers<T extends object, R> {
  readonly onInsert?: (params: InsertMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<void, unknown, R>
  readonly onUpdate?: (params: UpdateMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<void, unknown, R>
  readonly onDelete?: (params: DeleteMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<void, unknown, R>
}

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

  // Discharge the app's `R` once, at define time, by capturing the `services` runtime's context (the
  // elternportal pattern). `ServicesOf<R>` collapses to one branch per concrete `R`, but `R` is generic
  // in this body, so narrow the field to a single runtime type. When `services` is absent, `R` is
  // `never` (see ServicesOf) so an empty context is exactly `Context<R>`.
  const services = config.services as ManagedRuntime.ManagedRuntime<R, never> | undefined
  const servicesCtx: Context.Context<R> = services
    ? services.runSync(Effect.context<R>())
    : (Context.empty() as Context.Context<R>)

  // Bridge an Effect handler (with R) to the native TanStack handler (a Promise): provide the captured
  // context, then run on the default runtime. A rejection rolls the optimistic mutation back.
  const bridge =
    <P>(handler: (params: P) => Effect.Effect<void, unknown, R>) =>
    (params: P): Promise<void> =>
      Effect.runPromise(handler(params).pipe(Effect.provide(servicesCtx)))

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
            ...(config.onInsert ? { onInsert: bridge(config.onInsert) } : {}),
            ...(config.onUpdate ? { onUpdate: bridge(config.onUpdate) } : {}),
            ...(config.onDelete ? { onDelete: bridge(config.onDelete) } : {}),
          }),
        ) satisfies LiveCollection<T>,
    ).pipe(
      Effect.tap((collection) => Effect.addFinalizer(() => Effect.promise(() => collection.cleanup()))),
    )

  const mount = (key: CollectionKey<LiveCollection<T>>): LiveCollection<T> =>
    Effect.runSync(runtime.registry.getOrCreate({ key, make: makeFor(key) }))

  const meta: ModelMeta<T> = {
    entity,
    schema,
    getKey,
    scopeOf: Option.fromNullable(scopeOf),
    // Bridge listFn to `R = never` by providing the services context, so the loop yields it directly.
    listFn:
      scopeOf === undefined
        ? () => (config as GlobalBase<T, R>).listFn.pipe(Effect.provide(servicesCtx))
        : (scope) =>
            Option.match(scope, {
              onNone: () => Effect.die(`[defineCollection] scoped "${entity}" snapshot with no scope`),
              onSome: (s) => (config as ScopedBase<T, R>).listFn(s).pipe(Effect.provide(servicesCtx)),
            }),
  }

  const handle =
    scopeOf === undefined
      ? () => mount(globalKey<LiveCollection<T>>(entity))
      : (scope: string) => mount(scopedKey<LiveCollection<T>>({ entity, scope }))

  return Object.assign(handle, { _meta: meta }) as Handle<T>
}
