import { Effect, Option, Schema, Stream } from "effect"
import { ModelName, type SyncId } from "@triargos/live-collection-protocol"
import { SyncBroker, SyncSignal } from "./client/sync-broker.js"
import type { LiveCollection } from "./persistence/live-collection.js"
import type { SchemaVersion } from "./core/schema-version.js"
import type { ModelMeta } from "./define-collection.js"

/**
 * One collection instance's sync loop: subscribe to the broker under
 * `(entity, scope, schemaVersion)` and sequentially apply each signal to the
 * collection's synced baseline, acking the cursor after every application.
 *
 * - `Snapshot` — re-list the server truth and replace the whole table.
 * - `Upsert` — decode at the boundary and write; an undecodable event is logged and
 *   skipped (but still acked — the cursor must advance past a poison event), and an
 *   event whose entity belongs to another scope is dropped silently.
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
    const applied = (through: SyncId) => broker.markApplied({ modelName, scope, schemaVersion, through })

    yield* Stream.runForEach(broker.subscribe({ modelName, scope, schemaVersion }), (signal) =>
      SyncSignal.$match(signal, {
        Snapshot: ({ at }) =>
          meta.listFn(scope).pipe(
            Effect.flatMap((rows) => collection.utils.replaceSynced(rows)),
            Effect.andThen(applied(at)),
          ),
        Upsert: ({ syncId, data }) =>
          Schema.decodeUnknownEffect(meta.schema)(data).pipe(
            Effect.flatMap((row) => {
              const outOfScope = Option.match(meta.scopeOf, {
                onNone: () => false,
                onSome: (getScope) =>
                  Option.match(scope, {
                    onNone: () => true,
                    onSome: (s) => getScope(row) !== s,
                  }),
              })
              return outOfScope ? Effect.void : collection.utils.writeSynced(row)
            }),
            Effect.catchTag("SchemaError", (error) =>
              Effect.logWarning(
                `[defineCollection] skipping undecodable ${meta.entity} event #${syncId}: ${error.message}`,
              ),
            ),
            Effect.andThen(applied(syncId)),
          ),
        Delete: ({ syncId, modelId }) =>
          collection.utils.deleteSynced(modelId).pipe(Effect.andThen(applied(syncId))),
      }),
    )
  })
