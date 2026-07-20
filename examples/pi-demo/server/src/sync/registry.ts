import { Effect } from "effect"
import { Project, ProjectId, Todo, TodoId } from "@pi-demo/shared"
import { defineModelRegistry } from "@triargos/live-collection-protocol"
import { ModelRegistry } from "@triargos/live-collection-server"
import { ProjectRepo } from "../repo/project-repo.js"
import { TodoRepo } from "../repo/todo-repo.js"

/**
 * The only sync code this app owns: how each synced model is validated and
 * hydrated. Repos are resolved once at layer construction; descriptors are
 * plain closures over them. Everything else — squash, batching, none→Delete,
 * epoch, SSE framing — lives in `@triargos/live-collection-server`.
 *
 * Visibility here is per-session groups checked at the event level; the repos
 * hold no cross-session rows under one id, so hydration needs no second group
 * check (the `syncGroups` argument is unused).
 */
export const RegistryLayer = ModelRegistry.layer(
  Effect.gen(function* () {
    const projects = yield* ProjectRepo
    const todos = yield* TodoRepo
    return defineModelRegistry({
      Project: {
        modelName: "Project",
        schema: Project,
        hydrate: (id) => projects.find(ProjectId.make(id))
      },
      Todo: {
        modelName: "Todo",
        schema: Todo,
        hydrate: (id) => todos.find(TodoId.make(id))
      }
    })
  })
)
