import { Schema } from "effect"
import { deriveGroup, ModelId, ModelName, type SyncGroup } from "@triargos/live-collection-protocol"

/**
 * The demo domain: projects and their todos. `Todo.projectId` is the foreign key the
 * frontend joins on (`useLiveQuery` join — the feature this demo showcases).
 *
 * Rows stay JSON-plain (ISO-string timestamps, no `Date`): they are what the sync wire
 * carries and what the client persists locally.
 */

/**
 * A session is a capability, not a record: whoever holds the code sees the session's
 * data. There is no server-side session store — the code exists purely as the sync
 * group (`session:<code>`) its rows and events are tagged with.
 */
export const SessionCode = Schema.String.pipe(
  Schema.pattern(/^[A-Z0-9]{6}$/),
  Schema.brand("SessionCode"),
)
export type SessionCode = typeof SessionCode.Type

// No 0/O/1/I — codes get read aloud and typed across devices.
const CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

export const randomSessionCode = (): SessionCode => {
  const bytes = crypto.getRandomValues(new Uint8Array(6))
  let code = ""
  for (const byte of bytes) code += CODE_ALPHABET[byte % CODE_ALPHABET.length]
  return SessionCode.make(code)
}

/** The sync group all of a session's events are tagged with — the delivery filter. */
export const sessionGroup = (code: SessionCode): SyncGroup => deriveGroup(["session", code])

export const ProjectId = Schema.NonEmptyString.pipe(Schema.brand("ProjectId"))
export type ProjectId = typeof ProjectId.Type

export const TodoId = Schema.NonEmptyString.pipe(Schema.brand("TodoId"))
export type TodoId = typeof TodoId.Type

export const Project = Schema.Struct({
  id: ProjectId,
  sessionId: SessionCode,
  name: Schema.NonEmptyString,
  /** Hex color for the project badge in the UI. */
  color: Schema.String,
  createdAt: Schema.String,
})
export type Project = typeof Project.Type

export const Todo = Schema.Struct({
  id: TodoId,
  sessionId: SessionCode,
  projectId: ProjectId,
  title: Schema.NonEmptyString,
  completed: Schema.Boolean,
  createdAt: Schema.String,
})
export type Todo = typeof Todo.Type

// getKey boundary mappers (raw id → branded ModelId) — the one place `.make` is allowed.
export const projectKey = (p: Project): ModelId => ModelId.make(p.id)
export const todoKey = (t: Todo): ModelId => ModelId.make(t.id)

/** Wire model names — the `entity` of each `defineCollection` and the registry keys on the server. */
export const PROJECT_MODEL = ModelName.make("Project")
export const TODO_MODEL = ModelName.make("Todo")


