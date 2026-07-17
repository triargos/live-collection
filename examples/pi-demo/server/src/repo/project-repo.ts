import { Context, Effect, Layer, Option, Ref } from "effect"
import {
  type Project,
  ProjectId,
  ProjectNotFound,
  type SessionCode,
} from "@pi-demo/shared"

export interface ProjectRepoShape {
  readonly list: (session: SessionCode) => Effect.Effect<ReadonlyArray<Project>>
  readonly find: (id: ProjectId) => Effect.Effect<Option.Option<Project>>
  readonly upsert: (
    row: Project,
  ) => Effect.Effect<{ readonly row: Project; readonly kind: "Insert" | "Update" }>
  readonly remove: (id: ProjectId) => Effect.Effect<Project, ProjectNotFound>
}

const makeMemory: Effect.Effect<ProjectRepoShape> = Effect.gen(function* () {
  const rows = yield* Ref.make<ReadonlyMap<ProjectId, Project>>(new Map())
  return {
    list: (session) =>
      Ref.get(rows).pipe(
        Effect.map((map) => Array.from(map.values()).filter((row) => row.sessionId === session)),
      ),
    find: (id) => Ref.get(rows).pipe(Effect.map((map) => Option.fromNullishOr(map.get(id)))),
    upsert: (row) =>
      Ref.modify(rows, (map) => {
        const kind = map.has(row.id) ? "Update" as const : "Insert" as const
        const next = new Map(map)
        next.set(row.id, row)
        return [{ row, kind }, next]
      }),
    remove: (id) =>
      Ref.modify(rows, (map) => {
        const found = map.get(id)
        if (found === undefined) return [Option.none<Project>(), map]
        const next = new Map(map)
        next.delete(id)
        return [Option.some(found), next]
      }).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => new ProjectNotFound({ id }),
            onSome: Effect.succeed,
          }),
        ),
      ),
  }
})

export class ProjectRepo extends Context.Service<ProjectRepo, ProjectRepoShape>()("pi-demo/ProjectRepo") {
  static readonly layerMemory: Layer.Layer<ProjectRepo> = Layer.effect(ProjectRepo, makeMemory)
}
