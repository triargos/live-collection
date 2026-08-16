import { Schema } from "effect"
import { ModelId } from "@triargos/live-collection-protocol"
import { defineCollection, type LiveRuntime } from "@triargos/live-collection"
import { type SubsetStatus, usePartialLoad } from "../src/index.js"

// Compile-time only — never executed. Proves the `SubsetOf` surface: exactly one
// declared index key, value typed from the extractor, nothing else accepted.
const Value = Schema.Struct({ id: Schema.String, templateId: Schema.String, ownerId: Schema.String })
type Value = typeof Value.Type

export function _typeCheck(runtime: LiveRuntime): void {
  const values = defineCollection({
    runtime,
    entity: "Value",
    schema: Value,
    getKey: (v) => ModelId.make(v.id),
    partial: {
      by: {
        templateId: (v: Value) => v.templateId,
        ownerId: (v: Value) => v.ownerId,
      },
    },
  })
  const collection = values()

  const status: SubsetStatus = usePartialLoad(collection, { templateId: "t1" })
  void status
  usePartialLoad(collection, { ownerId: "u1" })

  // @ts-expect-error — not a declared index key
  usePartialLoad(collection, { nope: "x" })

  // @ts-expect-error — exactly one index key per subset
  usePartialLoad(collection, { templateId: "t1", ownerId: "u1" })

  // The generated ensures are typed on utils as well.
  const _load: (value: string) => Promise<void> = collection.utils.loadByOwnerId
  void _load
}
