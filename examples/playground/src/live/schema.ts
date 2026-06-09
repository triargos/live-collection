import { Schema } from "effect"
import { ModelId } from "@triargos/live-collection-protocol"

/** A workspace-scoped entity (keyed by `orgId`) — proves the scoped collection path. */
export const Webhook = Schema.Struct({
  id: Schema.String,
  orgId: Schema.String,
  url: Schema.String,
})
export type Webhook = typeof Webhook.Type

/** A global entity (no scope) — proves the global collection path. */
export const Project = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
})
export type Project = typeof Project.Type

// getKey is a boundary mapper (raw id → branded ModelId), the one place `.make` is allowed (CLAUDE.md).
export const webhookKey = (w: Webhook): ModelId => ModelId.make(w.id)
export const projectKey = (p: Project): ModelId => ModelId.make(p.id)
