import { Effect, Option, Schema, Stream } from "effect"
import { ModelName } from "@triargos/live-collection-protocol"
import { SyncBroker, SyncSignal } from "./client/sync-broker.js"
import type { LiveCollection } from "./persistence/live-collection.js"
import type { SchemaVersion } from "./core/schema-version.js"
import type { ModelMeta } from "./define-collection.js"

/**
 * One collection instance's sync loop: attach to the broker under
 * `(entity, scope, schemaVersion)` and apply each signal to the collection's synced
 * baseline. The broker drives the loop and acks each signal after `apply` returns —
 * this module only decides *how* a signal lands:
 *
 * - `Snapshot` — re-list the server truth and replace the whole table.
 * - `Upsert` — decode at the boundary and write; an undecodable event is logged and
 *   skipped, and an event whose entity belongs to another scope is dropped silently
 *   (both count as deliberately handled — the broker acks them all the same).
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
            Effect.flatMap((row) => (outOfScope(row) ? Effect.void : collection.utils.writeSynced(row))),
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
