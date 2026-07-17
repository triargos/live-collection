import { Effect, Option, Result, Schema } from "effect"
import { Project, ProjectId, Todo, TodoId } from "@pi-demo/shared"
import {
  defineModelRegistry,
  HydratedDelete,
  HydratedInsert,
  HydratedResync,
  HydratedUpdate,
  type HydratedSyncEventEnvelope,
  type ModelId,
  narrowModelName,
  type SyncContext,
  type SyncEvent,
} from "@triargos/live-collection-protocol"
import { ProjectRepo } from "../repo/project-repo.js"
import { TodoRepo } from "../repo/todo-repo.js"

export const modelNames = ["Project", "Todo"] as const

export const hydrateEvents = (args: {
  readonly events: ReadonlyArray<SyncEvent>
  readonly ctx: SyncContext
}): Effect.Effect<ReadonlyArray<HydratedSyncEventEnvelope>, never, ProjectRepo | TodoRepo> =>
  Effect.gen(function* () {
    const projects = yield* ProjectRepo
    const todos = yield* TodoRepo

    const registry = defineModelRegistry({
      Project: {
        modelName: "Project",
        schema: Project,
        hydrate: (id: ModelId, _ctx: SyncContext) => projects.find(ProjectId.make(id)),
      },
      Todo: {
        modelName: "Todo",
        schema: Todo,
        hydrate: (id: ModelId, _ctx: SyncContext) => todos.find(TodoId.make(id)),
      },
    })

    const output: Array<HydratedSyncEventEnvelope> = []
    for (const event of args.events) {
      if (event._tag === "Resync") {
        output.push(HydratedResync.make(event))
        continue
      }

      const known = narrowModelName(modelNames, event.modelName)
      if (Result.isFailure(known)) {
        yield* Effect.logDebug(`Skipping unknown model ${event.modelName}`)
        continue
      }

      const fields = {
        syncId: event.syncId,
        modelName: event.modelName,
        modelId: event.modelId,
        syncGroups: event.syncGroups,
        createdAt: event.createdAt,
      }
      if (event._tag === "Delete") {
        output.push(HydratedDelete.make(fields))
        continue
      }

      const data = yield* (known.success === "Project"
        ? registry.Project.hydrate(event.modelId, args.ctx).pipe(
            Effect.flatMap(Option.match({
              onNone: () => Effect.succeed(Option.none<unknown>()),
              onSome: (row) => Schema.encodeEffect(Project)(row).pipe(Effect.map(Option.some)),
            })),
          )
        : registry.Todo.hydrate(event.modelId, args.ctx).pipe(
            Effect.flatMap(Option.match({
              onNone: () => Effect.succeed(Option.none<unknown>()),
              onSome: (row) => Schema.encodeEffect(Todo)(row).pipe(Effect.map(Option.some)),
            })),
          ))
      if (Option.isNone(data)) {
        output.push(HydratedDelete.make(fields))
        continue
      }
      output.push(
        event._tag === "Insert"
          ? HydratedInsert(Schema.Unknown).make({ ...fields, data: data.value })
          : HydratedUpdate(Schema.Unknown).make({ ...fields, data: data.value }),
      )
    }
    return output
  }).pipe(Effect.orDie)
