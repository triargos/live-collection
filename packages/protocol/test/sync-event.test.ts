import { Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { HydratedSyncEvent } from "../src/sync-event.js"

const Person = Schema.Struct({ id: Schema.String, name: Schema.String })
const Wire = HydratedSyncEvent(Person)

const wireBase = {
  modelName: "Webhook",
  modelId: "wh-1",
  syncGroups: ["organization:a"],
  syncId: "42",
  createdAt: "2026-06-01T00:00:00.000Z"
}

// The only custom logic in sync-event.ts is the generic: HydratedInsert/Update must
// parse `data` against the supplied entity schema T, not leave it as `Schema.Unknown`.
// Everything else (union discrimination, required fields, Delete-has-no-data) is the
// shape of the structs we declared — Effect.Schema's job, not ours, and exercised
// transitively by the squash tests.
describe("HydratedSyncEvent<T> threads the entity schema into data", () => {
  it("parses data against T", () => {
    const insert = Schema.decodeUnknownSync(Wire)({
      _tag: "Insert",
      ...wireBase,
      data: { id: "wh-1", name: "Alice" }
    })
    assert.strictEqual(insert._tag, "Insert")
    assert.deepStrictEqual((insert as { data: unknown }).data, { id: "wh-1", name: "Alice" })
  })

  it("rejects data that violates T — proving T is applied, not Unknown", () => {
    // If `data` were left opaque, `name: 42` would slip through. It must not.
    assert.throws(() =>
      Schema.decodeUnknownSync(Wire)({
        _tag: "Insert",
        ...wireBase,
        data: { id: "wh-1", name: 42 }
      })
    )
  })
})
