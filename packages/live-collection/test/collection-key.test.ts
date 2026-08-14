import { Option } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { globalKey, scopedKey, serializeKey, subsetKey } from "../src/core/collection-key.js"

describe("serializeKey", () => {
  // SPELLING FREEZE — these literals are the at-rest key spellings every existing
  // client's durable last-applied marks live under. Byte-identical forever; a change
  // here is a silent global reset for every deployed app.
  it("keeps the frozen global/scoped spellings byte-identical", () => {
    assert.strictEqual(serializeKey(globalKey("User")), '["User",null]')
    assert.strictEqual(serializeKey(scopedKey({ entity: "Webhook", scope: "org-1" })), '["Webhook","org-1"]')
  })

  it("subset keys extend the tuple with indexKey and keyValue", () => {
    assert.strictEqual(
      serializeKey(
        subsetKey({
          entity: "SelectionTemplateValue",
          scope: Option.none(),
          subset: { indexKey: "templateId", keyValue: "t1" },
        }),
      ),
      '["SelectionTemplateValue",null,"templateId","t1"]',
    )
    assert.strictEqual(
      serializeKey(
        subsetKey({
          entity: "Webhook",
          scope: Option.some("org-1"),
          subset: { indexKey: "channel", keyValue: "c9" },
        }),
      ),
      '["Webhook","org-1","channel","c9"]',
    )
  })

  it("stays injective across the two forms", () => {
    const spellings = [
      serializeKey(globalKey("A")),
      serializeKey(scopedKey({ entity: "A", scope: "B" })),
      serializeKey(subsetKey({ entity: "A", scope: Option.none(), subset: { indexKey: "B", keyValue: "C" } })),
      serializeKey(subsetKey({ entity: "A", scope: Option.some("B"), subset: { indexKey: "C", keyValue: "D" } })),
    ]
    assert.strictEqual(new Set(spellings).size, spellings.length)
  })
})
