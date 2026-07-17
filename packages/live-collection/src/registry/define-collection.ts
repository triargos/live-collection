import { Effect, Fiber, type ManagedRuntime, Option, Schema, type Scope, Stream } from "effect"
import {
  createCollection,
  type DeleteMutationFnParams,
  type InsertMutationFnParams,
  type UpdateMutationFnParams,
} from "@tanstack/db"
import { persistedCollectionOptions } from "@tanstack/db-sqlite-persistence-core"
import { type ModelId, ModelName, type SyncId } from "@triargos/live-collection-protocol"
import { SyncBroker, SyncSignal } from "../client/sync-broker.js"
import type { SyncWrite } from "../dispatch/sync-write.js"
import type { LiveCollection } from "../persistence/live-collection.js"
import { liveCollectionOptions } from "../persistence/live-collection-options.js"
import { deriveSchemaVersion } from "../persistence/schema-version.js"
import type { LiveRuntime } from "../runtime/live-runtime.js"
import { type CollectionKey, globalKey, scopedKey, serializeKey } from "./collection-key.js"

/**
 * The per-model metadata each collection drain reads from its handle (`handle._meta`):
 * how to decode (`schema`), key (`getKey`), route (`entity` — the wire model name — plus
 * `scopeOf`), and snapshot (`listFn`) the model. Populated by {@link defineCollection};
 * apps never construct one.
 *
 * `scopeOf` is `None` for a global collection and `Some((entity) => scope)` for a scoped
 * one. `listFn` is self-contained — the app's `services` runtime was provided into it at
 * define time — so the drain runs it with no further dependencies.
 */
export interface ModelMeta<T extends object> {
  readonly entity: string
  readonly schema: Schema.Codec<T, any>
  readonly getKey: (entity: T) => ModelId
  readonly scopeOf: Option.Option<(entity: T) => string>
  readonly listFn: (scope: Option.Option<string>) => Effect.Effect<ReadonlyArray<T>>
}

/**
 * A global collection's handle: `settingsCollection()` mounts (or reuses) the single
 * app-wide instance and returns the native {@link LiveCollection}. Returned by
 * {@link defineCollection} when `scopeOf` is absent.
 */
export type GlobalHandle<T extends object> = (() => LiveCollection<T>) & { readonly _meta: ModelMeta<T> }
/**
 * A scoped collection's handle: `webhookCollection(orgId)` mounts (or reuses) that
 * scope's instance and returns the native {@link LiveCollection}. Returned by
 * {@link defineCollection} when `scopeOf` is present.
 */
export type ScopedHandle<T extends object> = ((scope: string) => LiveCollection<T>) & {
  readonly _meta: ModelMeta<T>
}
/** Either collection handle — global or scoped. */
export type Handle<T extends object> = GlobalHandle<T> | ScopedHandle<T>

/**
 * The optional **optimistic write path**. The handlers are TanStack DB's native mutation
 * hooks, but **Effect-returning** with the app's `R` (discharged by `services`) — write a
 * plain Effect (`yield* SomeApi`), no `runPromise` boilerplate.
 *
 * A handler's only job is the **server call**; the library reconciles for you. The row
 * `onInsert`/`onUpdate` return is folded into the synced baseline before the mutation
 * resolves, so when TanStack drops the optimistic transaction the confirmed row is
 * already in place (no flicker), and the eventual SSE echo of the same row is an
 * idempotent re-write. A failed Effect rejects the mutation and the optimistic write is
 * rolled back. Apps never touch `collection.utils`.
 *
 * Ids are the app's — client-minted ids are recommended (the echo stays idempotent, no
 * temp-id swap needed). One mutation per transaction: a batched transaction dies with
 * {@link BatchedMutationsUnsupported} before any server call.
 */
interface MutationHandlers<T extends object, R> {
  /**
   * Runs when an optimistic `collection.insert(...)` commits: send the row to your
   * backend and return the **server-confirmed row** (required — without it the library
   * can't reconcile and the optimistic row would flicker).
   */
  readonly onInsert?: (params: InsertMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<T, unknown, R>
  /**
   * Runs when an optimistic `collection.update(...)` commits: persist the change on
   * your backend and return the **server-confirmed row**.
   */
  readonly onUpdate?: (params: UpdateMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<T, unknown, R>
  /**
   * Runs when an optimistic `collection.delete(...)` commits: delete on your backend.
   * On success the library removes the row from the synced baseline by its key.
   */
  readonly onDelete?: (params: DeleteMutationFnParams<T, ModelId, SyncWrite<T>>) => Effect.Effect<void, unknown, R>
}

/**
 * Defect raised when an optimistic mutation handler receives a transaction holding more
 * than one mutation. The library reconciles exactly one confirmed row per transaction,
 * so a batch would silently lose every row after the first the instant the optimistic
 * transaction drops — instead the whole transaction fails loudly, before any server
 * call. Split the writes into one mutation per transaction.
 */
export class BatchedMutationsUnsupported extends Schema.TaggedErrorClass<BatchedMutationsUnsupported>()(
  "BatchedMutationsUnsupported",
  { entity: Schema.String, mutationCount: Schema.Number },
) {}

/**
 * The app-services `ManagedRuntime` that discharges the `R` inferred from `listFn` and
 * the mutation handlers. **Required exactly when `R ≠ never`** — a collection whose
 * Effects need no app services omits it. This keeps `LiveRuntime` infra-only: the app's
 * service requirements live on each collection, not on the shared runtime.
 */
type ServicesOf<R> = [R] extends [never]
  ? {
      /** App-services runtime — optional here because nothing in this config requires services. */
      readonly services?: ManagedRuntime.ManagedRuntime<never, never>
    }
  : {
      /**
       * The app-services `ManagedRuntime` that runs `listFn` and the mutation handlers —
       * it must provide every service they require.
       */
      readonly services: ManagedRuntime.ManagedRuntime<R, never>
    }

interface GlobalBase<T extends object, R> {
  /** The runtime from `makeLiveRuntime` — the registry and persistence this collection mounts against. */
  readonly runtime: LiveRuntime
  /**
   * The model's wire name, e.g. `"Webhook"` — must match what the backend emits. Written
   * once, it serves as the registry key, the persisted table id, and the event-routing
   * name, so they can never drift.
   */
  readonly entity: string
  /**
   * Decodes the model's entities at the boundary (live events, catchup, replay). A
   * schema change also changes the derived persisted-schema version, which dumps and
   * rebuilds the local table on next start.
   */
  readonly schema: Schema.Codec<T, any>
  /** Extracts the entity's primary key. */
  readonly getKey: (entity: T) => ModelId
  /** Fetches the current server truth — run on cold starts and resyncs to (re)build the local base. */
  readonly listFn: Effect.Effect<ReadonlyArray<T>, never, R>
}
interface ScopedBase<T extends object, R> {
  /** The runtime from `makeLiveRuntime` — the registry and persistence this collection mounts against. */
  readonly runtime: LiveRuntime
  /**
   * The model's wire name, e.g. `"Webhook"` — must match what the backend emits. Written
   * once, it serves as the registry key, the persisted table id, and the event-routing
   * name, so they can never drift.
   */
  readonly entity: string
  /**
   * Decodes the model's entities at the boundary (live events, catchup, replay). A
   * schema change also changes the derived persisted-schema version, which dumps and
   * rebuilds the local table on next start.
   */
  readonly schema: Schema.Codec<T, any>
  /** Extracts the entity's primary key. */
  readonly getKey: (entity: T) => ModelId
  /**
   * Reads the scope off an entity (e.g. `(w) => w.orgId`). Its presence makes the
   * collection **scoped** — one instance per scope, mounted with `handle(scope)` — and
   * lets each collection drain filter incoming upserts to its scope. This is the only
   * place your "workspace" notion meets the library.
   */
  readonly scopeOf: (entity: T) => string
  /** Fetches one scope's current server truth — run on cold starts and resyncs to (re)build that instance's base. */
  readonly listFn: (scope: string) => Effect.Effect<ReadonlyArray<T>, never, R>
}
type GlobalConfig<T extends object, R> = GlobalBase<T, R> & MutationHandlers<T, R> & ServicesOf<R>
type ScopedConfig<T extends object, R> = ScopedBase<T, R> & MutationHandlers<T, R> & ServicesOf<R>

/**
 * Define one synced model and get back its collection handle. Calling the handle mounts
 * the collection through the registry — synchronous, cached by `(entity, scope)`, and
 * referentially stable (a map lookup after the first mount), so calling it inline in
 * render is cheap — and returns the **native** `LiveCollection<T>`: pass it straight to
 * `useLiveQuery`, or call `.insert/.update/.delete` on it for optimistic writes.
 *
 * Two overloads, split by `scopeOf`:
 * - **absent** → a global collection, one app-wide instance: `handle()`.
 * - **present** → a scoped collection, one instance per scope: `handle(scope)`.
 *
 * `R` is inferred from `listFn` and the optional mutation handlers; pass `services` (an
 * app-owned `ManagedRuntime`) to discharge it.
 *
 * @example
 * ```ts
 * // collections.ts — module level, once per model
 * export const webhookCollection = defineCollection({
 *   runtime,
 *   entity: "Webhook",                       // the wire model name
 *   schema: Webhook,                         // decodes events at the boundary
 *   getKey: (w) => w.id,
 *   scopeOf: (w) => w.orgId,                 // scoped: one instance per organization
 *   listFn: (orgId) => Effect.flatMap(WebhookApi, (api) => api.list(orgId)),
 *   services,                                // ManagedRuntime providing WebhookApi
 *   // optimistic writes (optional): call the server, return the confirmed row
 *   onInsert: ({ transaction }) =>
 *     Effect.flatMap(WebhookApi, (api) => api.create(transaction.mutations[0].modified)),
 * })
 *
 * // in a component
 * const webhooks = webhookCollection(orgId)
 * const { data } = useLiveQuery((q) => q.from({ w: webhooks }))
 * webhooks.insert({ id: crypto.randomUUID(), orgId, url }) // optimistic, reconciled on confirm
 * ```
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
  // ON it (`runPromise`), the drain-facing listFn runs WITH it (`Effect.provide`). It is never forced to
  // build synchronously: the runtime builds lazily on first use and memoizes, so async-constructing
  // layers work and a disposed runtime fails loudly instead of serving finalized services. When
  // `services` is absent, `R` is `never` (see ServicesOf), so running on the default runtime is sound —
  // the casts below are that contract, stated once per seam.
  const services = config.services as ManagedRuntime.ManagedRuntime<R, never> | undefined

  // Bridge an Effect handler (with R) to the native TanStack handler (a Promise): run the handler, then
  // reconcile its result into the synced baseline — both before the Promise resolves, so the synced row
  // is in place when TanStack drops the completed optimistic tx. `flatMap` short-circuits on failure ⇒
  // the reconcile never runs and the rejection rolls the optimistic mutation back. A batched transaction
  // dies before the handler runs — only `mutations[0]` would be reconciled.
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
  // so the mount path is `Effect.runSync`-able with no async boundary.
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
      Effect.tap((collection) => {
        const modelName = ModelName.make(entity)
        const drain = Effect.gen(function* () {
          const broker = yield* SyncBroker
          const applied = (through: SyncId) => broker.markApplied({ modelName, scope: key.scope, through })
          yield* Stream.runForEach(broker.subscribe({ modelName, scope: key.scope }), (signal) =>
            SyncSignal.$match(signal, {
              Snapshot: ({ at }) =>
                meta.listFn(key.scope).pipe(
                  Effect.flatMap((rows) => collection.utils.replaceSynced(rows)),
                  Effect.andThen(applied(at)),
                ),
              Upsert: ({ syncId, data }) =>
                Schema.decodeUnknownEffect(schema)(data).pipe(
                  Effect.flatMap((row) => {
                    const outOfScope = Option.match(meta.scopeOf, {
                      onNone: () => false,
                      onSome: (getScope) =>
                        Option.match(key.scope, {
                          onNone: () => true,
                          onSome: (scope) => getScope(row) !== scope,
                        }),
                    })
                    return outOfScope ? Effect.void : collection.utils.writeSynced(row)
                  }),
                  Effect.catchTag("SchemaError", (error) =>
                    Effect.logWarning(
                      `[defineCollection] skipping undecodable ${entity} event #${syncId}: ${error.message}`,
                    ),
                  ),
                  Effect.andThen(applied(syncId)),
                ),
              Delete: ({ syncId, modelId }) =>
                collection.utils.deleteSynced(modelId).pipe(Effect.andThen(applied(syncId))),
            }),
          )
        })
        return Effect.sync(() => runtime.forkDrain(drain)).pipe(
          Effect.flatMap((fiber) =>
            Effect.addFinalizer(() =>
              Fiber.interrupt(fiber).pipe(Effect.andThen(Effect.promise(() => collection.cleanup()))),
            ),
          ),
        )
      }),
    )

  const mount = (key: CollectionKey<LiveCollection<T>>): LiveCollection<T> =>
    Effect.runSync(runtime.registry.getOrCreate({ key, make: makeFor(key) }))

  // Bridge listFn to `R = never` for the drain. The drain is itself an Effect fiber, so this stays in
  // Effect-land: `Effect.provide(services)` runs the listFn with the services runtime (the same
  // memoized runtime `runPromise` uses) without a promise detour — interruption of an in-flight
  // snapshot and cause structure are preserved.
  const provideServices = <A>(eff: Effect.Effect<A, never, R>): Effect.Effect<A> =>
    services
      ? services.contextEffect.pipe(Effect.flatMap((context) => eff.pipe(Effect.provide(context))))
      : (eff as Effect.Effect<A>)

  const meta: ModelMeta<T> = {
    entity,
    schema,
    getKey,
    scopeOf: Option.fromNullishOr(scopeOf),
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
