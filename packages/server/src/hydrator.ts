import { Effect, Option, Result } from "effect"
import {
  entityKey,
  HydratedSyncEventEnvelope,
  type ModelId,
  ModelName,
  narrowModelName,
  type SyncEvent,
  type SyncGroup
} from "@triargos/live-collection-protocol"
import type { ModelRegistryShape, ResolvedModel } from "./model-registry.js"

/**
 * Internal to `SyncFeed` — deliberately not exported from the package, so there
 * is no second, bypassable path around the hydration invariants.
 *
 * Turns at-rest events into wire envelopes against the resolved model registry:
 * - `Insert`/`Update` gain the entity's *current* encoded data, batched per
 *   model through `hydrateMany` when available (one call per model, not one per
 *   event — the catchup N+1 killer), falling back to per-id `hydrate`.
 * - `Option.none()` from hydration (entity gone, or the caller's access lost)
 *   downgrades the event to a `Delete` — hydration is the second, authoritative
 *   visibility check.
 * - An unknown model name is logged and dropped, never fatal.
 * - An encode failure is logged and the event skipped — never downgraded to a
 *   `Delete` (that would wrongly remove client data) and never a stream kill.
 * - `Delete` and `Resync` pass through untouched.
 *
 * Output preserves the input's `syncId` order.
 */

type Resolution =
  | { readonly _tag: "Data"; readonly data: unknown }
  | { readonly _tag: "Gone" }
  | { readonly _tag: "Skip" }

const gone: Resolution = { _tag: "Gone" }

export interface Hydrator {
  readonly hydrateEvents: (args: {
    readonly events: ReadonlyArray<SyncEvent>
    readonly syncGroups: ReadonlyArray<SyncGroup>
  }) => Effect.Effect<ReadonlyArray<HydratedSyncEventEnvelope>>
}

export const makeHydrator = (registry: ModelRegistryShape): Hydrator => {
  const knownNames = [...registry.models.keys()]

  const resolveModel = (
    name: string,
    model: ResolvedModel,
    ids: ReadonlyArray<ModelId>,
    syncGroups: ReadonlyArray<SyncGroup>
  ): Effect.Effect<ReadonlyMap<ModelId, Resolution>> =>
    Effect.gen(function* () {
      const present: ReadonlyMap<ModelId, unknown> = model.hydrateMany
        ? yield* model.hydrateMany(ids, syncGroups)
        : new Map(
            (yield* Effect.forEach(ids, (id) =>
              model.hydrate(id, syncGroups).pipe(Effect.map((row) => [id, row] as const))
            )).flatMap(([id, row]) => (Option.isSome(row) ? [[id, row.value] as const] : []))
          )

      const resolutions = new Map<ModelId, Resolution>()
      for (const id of ids) {
        if (!present.has(id)) {
          resolutions.set(id, gone)
          continue
        }
        const encoded = yield* model.encode(present.get(id)).pipe(
          Effect.map((data): Resolution => ({ _tag: "Data", data })),
          Effect.catch((error) =>
            Effect.logWarning(`Skipping ${name}/${id}: hydrated entity failed to encode`, error).pipe(
              Effect.as<Resolution>({ _tag: "Skip" })
            )
          )
        )
        resolutions.set(id, encoded)
      }
      return resolutions
    })

  const hydrateEvents: Hydrator["hydrateEvents"] = Effect.fn("SyncFeed.hydrateEvents")(
    function* (args) {
      // 1. Gather the entity ids each known model must hydrate.
      const wanted = new Map<string, Set<ModelId>>()
      for (const event of args.events) {
        if (event._tag !== "Insert" && event._tag !== "Update") continue
        const known = narrowModelName(knownNames, event.modelName)
        if (Result.isFailure(known)) continue
        const ids = wanted.get(known.success) ?? new Set<ModelId>()
        ids.add(event.modelId)
        wanted.set(known.success, ids)
      }

      // 2. Resolve each model's ids in one batch.
      const resolutions = new Map<string, Resolution>()
      for (const [name, ids] of wanted) {
        const model = registry.models.get(name)
        if (model === undefined) continue
        const resolved = yield* resolveModel(name, model, [...ids], args.syncGroups)
        for (const [id, resolution] of resolved) {
          resolutions.set(entityKey(ModelName.make(name), id), resolution)
        }
      }

      // 3. Assemble envelopes in the input's syncId order.
      const output: Array<HydratedSyncEventEnvelope> = []
      for (const event of args.events) {
        if (event._tag === "Resync") {
          output.push(HydratedSyncEventEnvelope.cases.Resync.make(event))
          continue
        }
        const known = narrowModelName(knownNames, event.modelName)
        if (Result.isFailure(known)) {
          yield* Effect.logDebug(`Skipping event for unknown model ${event.modelName}`)
          continue
        }
        const fields = {
          syncId: event.syncId,
          modelName: event.modelName,
          modelId: event.modelId,
          syncGroups: event.syncGroups,
          createdAt: event.createdAt
        }
        if (event._tag === "Delete") {
          output.push(HydratedSyncEventEnvelope.cases.Delete.make(fields))
          continue
        }
        const resolution = resolutions.get(entityKey(event.modelName, event.modelId)) ?? gone
        switch (resolution._tag) {
          case "Data":
            output.push(HydratedSyncEventEnvelope.cases[event._tag].make({ ...fields, data: resolution.data }))
            break
          case "Gone":
            output.push(HydratedSyncEventEnvelope.cases.Delete.make(fields))
            break
          case "Skip":
            break
        }
      }
      return output
    }
  )

  return { hydrateEvents }
}
