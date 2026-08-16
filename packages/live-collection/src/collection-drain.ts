import { Effect, Option, Ref, Schema, type Semaphore, Stream } from "effect"
import { ModelName } from "@triargos/live-collection-protocol"
import { SyncBroker, SyncSignal } from "./client/sync-broker.js"
import type { LiveCollection } from "./persistence/live-collection.js"
import type { SchemaVersion } from "./core/schema-version.js"
import type { ModelMeta } from "./define-collection.js"
import { type CoverageState, makePartialApplier, mergeCoverage, type PartialWrite, seedCoverage } from "./partial-coverage.js"

/**
 * One collection instance's sync loop: attach to the broker under
 * `(entity, scope, schemaVersion)` and apply each signal to the collection's synced
 * baseline. The broker drives the loop and acks each signal after `apply` returns —
 * this module only decides *how* a signal lands:
 *
 * - `Snapshot` — re-list the server truth and replace the whole table.
 * - `Upsert` — decode at the boundary and write; an undecodable event is logged and
 *   skipped, and an event whose entity belongs to another scope **deletes** any local
 *   row with that key (a row that moved scope must not linger here) — both count as
 *   deliberately handled, and the broker acks them all the same.
 * - `Delete` — remove by id.
 *
 * Runs forever; the registry forks it per instance and interrupts it on dispose.
 */
export const drainCollection = <T extends object>(args: {
  readonly meta: ModelMeta<T>
  readonly collection: LiveCollection<T>
  readonly scope: Option.Option<string>
  readonly schemaVersion: SchemaVersion
}): Effect.Effect<void, never, SyncBroker> =>
  Effect.gen(function* () {
    const { meta, collection, scope, schemaVersion } = args
    const modelName = ModelName.make(meta.entity)
    const broker = yield* SyncBroker
    // Mirror of the server registry's encode edge: decode through the canonical
    // JSON codec so entity fields whose plain encoded form isn't JSON-native
    // (Date, Uint8Array, ...) round-trip instead of failing on their stringified form.
    const decodeEntity = Schema.decodeUnknownEffect(Schema.toCodecJson(meta.schema))

    const outOfScope = (row: T): boolean =>
      Option.match(meta.scopeOf, {
        onNone: () => false,
        onSome: (getScope) =>
          Option.match(scope, {
            onNone: () => true,
            onSome: (s) => getScope(row) !== s,
          }),
      })

    yield* broker.attachSubscriber({
      modelName,
      scope,
      schemaVersion,
      apply: SyncSignal.$match({
        Snapshot: () =>
          meta.listFn(scope).pipe(Effect.flatMap((rows) => collection.utils.replaceSynced(rows))),
        Upsert: ({ syncId, data }) =>
          decodeEntity(data).pipe(
            Effect.flatMap((row) =>
              outOfScope(row)
                ? collection.utils.deleteSynced(meta.getKey(row))
                : collection.utils.writeSynced(row),
            ),
            Effect.catchTag("SchemaError", (error) =>
              Effect.logWarning(
                `[defineCollection] skipping undecodable ${meta.entity} event #${syncId}: ${error.message}`,
              ),
            ),
          ),
        Delete: ({ modelId }) => collection.utils.deleteSynced(modelId),
      }),
    })
  })

/**
 * A partial collection's sync loop. Before attaching, it hydrates the in-memory
 * coverage map from the durable subset marks — no signal can be applied against an
 * unhydrated map, because signals only flow to attached subscribers. Application
 * itself is `makePartialApplier` under the instance's gate; the returned covered
 * subsets become the broker's per-subset mark acks.
 */
export const drainPartialCollection = <T extends object>(args: {
  readonly meta: ModelMeta<T>
  readonly by: Record<string, (entity: T) => string>
  readonly write: PartialWrite<T>
  readonly schemaVersion: SchemaVersion
  readonly coverage: Ref.Ref<CoverageState>
  readonly gate: Semaphore.Semaphore
}): Effect.Effect<void, never, SyncBroker> =>
  Effect.gen(function* () {
    const { meta, by, write, schemaVersion, coverage, gate } = args
    const modelName = ModelName.make(meta.entity)
    const broker = yield* SyncBroker
    const marks = yield* broker.coveredSubsets({ modelName, scope: Option.none(), schemaVersion })
    yield* Ref.update(coverage, (current) => mergeCoverage(current, seedCoverage(marks)))

    const applier = makePartialApplier({
      entity: meta.entity,
      by,
      getKey: meta.getKey,
      decode: Schema.decodeUnknownEffect(Schema.toCodecJson(meta.schema)),
      write,
      coverage,
    })

    yield* broker.attachSubscriber({
      modelName,
      scope: Option.none(),
      schemaVersion,
      apply: (signal) => gate.withPermit(applier(signal)),
    })
  })
