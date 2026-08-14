import { Option, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { HydrateBatchRequest, HydrateBatchResponse } from "../src/hydrate-batch.js"

const request = {
  modelName: "SelectionTemplateValue",
  indexKey: "templateId",
  keyValue: "t1"
}

describe("HydrateBatchRequest", () => {
  it("round-trips a batch of index values", () => {
    const decoded = Schema.decodeUnknownSync(HydrateBatchRequest)({ requests: [request] })
    assert.deepStrictEqual(Schema.encodeSync(HydrateBatchRequest)(decoded), { requests: [request] })
  })

  it("rejects an empty batch", () => {
    assert.throws(() => Schema.decodeUnknownSync(HydrateBatchRequest)({ requests: [] }))
  })
})

describe("HydrateBatchResponse", () => {
  it("keeps Forbidden distinct from empty Members", () => {
    const decoded = Schema.decodeUnknownSync(HydrateBatchResponse)({
      results: [
        { _tag: "Members", request, rows: [] },
        { _tag: "Forbidden", request }
      ],
      lastSyncId: "42"
    })
    assert.strictEqual(decoded.results[0]!._tag, "Members")
    assert.strictEqual(decoded.results[1]!._tag, "Forbidden")
    // Empty membership is a valid, claimable result — not a refusal.
    assert.notStrictEqual(decoded.results[0]!._tag, decoded.results[1]!._tag)
  })

  it("epoch is optional on the wire — absent decodes to None, present survives the round trip", () => {
    const bare = Schema.decodeUnknownSync(HydrateBatchResponse)({ results: [], lastSyncId: "1" })
    assert.isTrue(Option.isNone(bare.epoch))
    assert.deepStrictEqual(Schema.encodeSync(HydrateBatchResponse)(bare), {
      results: [],
      lastSyncId: "1"
    })

    const stamped = Schema.decodeUnknownSync(HydrateBatchResponse)({
      results: [],
      lastSyncId: "1",
      epoch: "e-1"
    })
    assert.deepStrictEqual(stamped.epoch, Option.some("e-1"))
    assert.deepStrictEqual(Schema.encodeSync(HydrateBatchResponse)(stamped), {
      results: [],
      lastSyncId: "1",
      epoch: "e-1"
    })
  })

  it("rows travel as opaque wire values", () => {
    const rows = [{ id: "v1", templateId: "t1", createdAt: "2026-06-01T00:00:00.000Z" }]
    const decoded = Schema.decodeUnknownSync(HydrateBatchResponse)({
      results: [{ _tag: "Members", request, rows }],
      lastSyncId: "7"
    })
    const members = decoded.results[0]!
    assert.strictEqual(members._tag, "Members")
    assert.deepStrictEqual((members as { rows: ReadonlyArray<unknown> }).rows, rows)
  })
})
