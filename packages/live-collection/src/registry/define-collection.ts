import { Effect, Option, type Schema, type Scope } from "effect"
import { createCollection } from "@tanstack/db"
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
 * reads the scope straight off the event. `listFn` is the cold/resync snapshot source — self-contained
 * (`R = never`; the app closes over its own API client).
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

interface GlobalConfig<T extends object> {
  readonly runtime: LiveRuntime
  readonly entity: string
  readonly schema: Schema.Schema<T, any, never>
  readonly getKey: (entity: T) => ModelId
  readonly listFn: Effect.Effect<ReadonlyArray<T>>
}
interface ScopedConfig<T extends object> {
  readonly runtime: LiveRuntime
  readonly entity: string
  readonly schema: Schema.Schema<T, any, never>
  readonly getKey: (entity: T) => ModelId
  readonly scopeOf: (entity: T) => string
  readonly listFn: (scope: string) => Effect.Effect<ReadonlyArray<T>>
}

/**
 * Define one model's collection. Returns a **runtime-bound, registry-backed handle** (DEC-R2): calling
 * it mounts through the registry (sync, cached by `(entity, scope)`) and returns the **native**
 * `LiveCollection<T>` — pass that straight to `useLiveQuery`. Calling the handle inline in render is
 * cheap and referentially stable (a `Map.get` after first mount).
 *
 * Two overloads (mirrors the global/scoped split): `scopeOf` present ⇒ scoped `(scope) => Collection`;
 * absent ⇒ global `() => Collection`. The model name is written once, so the registry key and the
 * persisted table id (`serializeKey(key)`) can never drift.
 */
export function defineCollection<T extends object>(config: GlobalConfig<T>): GlobalHandle<T>
export function defineCollection<T extends object>(config: ScopedConfig<T>): ScopedHandle<T>
export function defineCollection<T extends object>(
  config: GlobalConfig<T> | ScopedConfig<T>,
): Handle<T> {
  const { runtime, entity, schema, getKey } = config
  const scopeOf = "scopeOf" in config ? config.scopeOf : undefined
  const schemaVersion = deriveSchemaVersion(schema)

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
    listFn:
      scopeOf === undefined
        ? () => (config as GlobalConfig<T>).listFn
        : (scope) =>
            Option.match(scope, {
              onNone: () => Effect.die(`[defineCollection] scoped "${entity}" snapshot with no scope`),
              onSome: (config as ScopedConfig<T>).listFn,
            }),
  }

  const handle =
    scopeOf === undefined
      ? () => mount(globalKey<LiveCollection<T>>(entity))
      : (scope: string) => mount(scopedKey<LiveCollection<T>>({ entity, scope }))

  return Object.assign(handle, { _meta: meta }) as Handle<T>
}
