import { Effect, Schema } from "effect"
import { useLiveQuery } from "@tanstack/react-db"
import { ModelId } from "@triargos/live-collection-protocol"
import { defineCollection, type LiveRuntime } from "@triargos/live-collection"
import { useLiveSync } from "../src/index.js"

// Compile-time only — never executed (`runtime` is a phantom param). Proves the load-bearing claim
// (DEC-R1): a `defineCollection` handle returns a NATIVE TanStack collection that `useLiveQuery`
// consumes with no wrapper, surfacing the entity rows.
const Webhook = Schema.Struct({ id: Schema.String, orgId: Schema.String })
type Webhook = typeof Webhook.Type

export function _typeCheck(runtime: LiveRuntime, orgId: string): void {
  const webhookCollection = defineCollection({
    runtime,
    entity: "Webhook",
    schema: Webhook,
    getKey: (w) => ModelId.make(w.id),
    scopeOf: (w) => w.orgId,
    listFn: () => Effect.succeed<ReadonlyArray<Webhook>>([]),
  })

  // The explicit models array — the wire name comes from each handle's `_meta.entity` (DEC-R5 amendment).
  useLiveSync(runtime)

  // Direct-collection overload — the native read: data is the entity rows, no wrapper.
  const direct = useLiveQuery(() => webhookCollection(orgId), [orgId])
  const _direct: ReadonlyArray<Webhook> | undefined = direct.data
  void _direct

  // Query-builder overload — native joins/filters: from({ w: collection }) accepts our collection.
  const joined = useLiveQuery((q) => q.from({ w: webhookCollection(orgId) }), [orgId])
  void joined.data
}
