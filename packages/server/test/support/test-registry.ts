import { Context, Effect, Layer, Option, Ref, Schema } from "effect"
import type { ModelId, SyncGroup } from "@triargos/live-collection-protocol"
import { defineModelRegistry } from "@triargos/live-collection-protocol"

/**
 * A minimal app-side world for kernel tests: one synced model ("Note") hydrated
 * from an in-test Ref-backed repo, provided to `SyncFeed.layer` through the
 * descriptors' `R` — the same wiring a real app uses with its database repos.
 */

export const NoteId = Schema.String.pipe(Schema.brand("NoteId"))
export type NoteId = typeof NoteId.Type

export const Note = Schema.Struct({
  id: NoteId,
  title: Schema.String
})
export type Note = typeof Note.Type

export interface NoteRepoShape {
  readonly upsert: (note: Note) => Effect.Effect<void>
  readonly remove: (id: NoteId) => Effect.Effect<void>
  readonly find: (id: NoteId) => Effect.Effect<Option.Option<Note>>
  /** Number of hydration lookups served — asserts the batch path (no N+1). */
  readonly lookupCount: Effect.Effect<number>
}

const makeNoteRepo: Effect.Effect<NoteRepoShape> = Effect.gen(function* () {
  const rows = yield* Ref.make<ReadonlyMap<NoteId, Note>>(new Map())
  const lookups = yield* Ref.make(0)
  return {
    upsert: (note) => Ref.update(rows, (map) => new Map(map).set(note.id, note)),
    remove: (id) =>
      Ref.update(rows, (map) => {
        const next = new Map(map)
        next.delete(id)
        return next
      }),
    find: (id) =>
      Ref.update(lookups, (n) => n + 1).pipe(
        Effect.andThen(Ref.get(rows)),
        Effect.map((map) => Option.fromNullishOr(map.get(id)))
      ),
    lookupCount: Ref.get(lookups)
  }
})

export class NoteRepo extends Context.Service<NoteRepo, NoteRepoShape>()("test/NoteRepo") {
  static readonly layerMemory: Layer.Layer<NoteRepo> = Layer.effect(NoteRepo, makeNoteRepo)
}

/** The registry build effect — repos resolved once, descriptors close over them. */
export const testRegistry = Effect.gen(function* () {
  const repo = yield* NoteRepo
  return defineModelRegistry({
    Note: {
      modelName: "Note",
      schema: Note,
      hydrate: (id: ModelId) => repo.find(NoteId.make(id))
    }
  })
})

/** Same model, but hydrated through the batch path — one repo pass per model. */
export const testRegistryBatched = Effect.gen(function* () {
  const repo = yield* NoteRepo
  return defineModelRegistry({
    Note: {
      modelName: "Note",
      schema: Note,
      hydrate: (id: ModelId) => repo.find(NoteId.make(id)),
      hydrateMany: (ids: ReadonlyArray<ModelId>, _syncGroups: ReadonlyArray<SyncGroup>) =>
        Effect.gen(function* () {
          const found = new Map<ModelId, Note>()
          for (const id of ids) {
            const row = yield* repo.find(NoteId.make(id))
            if (Option.isSome(row)) found.set(id, row.value)
          }
          return found
        })
    }
  })
})
