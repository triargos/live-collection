import { Order, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { compareSyncId, SyncId } from "../src/ids.js"

const decodeSyncId = Schema.decodeUnknownEither(SyncId)

describe("SyncId", () => {
  it("accepts canonical decimal strings", () => {
    for (const raw of ["0", "1", "42", "9007199254740993000000"]) {
      assert.isTrue(decodeSyncId(raw)._tag === "Right", `expected ${raw} to decode`)
    }
  })

  it("rejects leading zeros, signs, and non-digits", () => {
    for (const raw of ["00", "01", "-1", "+1", "1.0", "", "12a", " 12"]) {
      assert.strictEqual(decodeSyncId(raw)._tag, "Left", `expected ${raw} to be rejected`)
    }
  })
})

describe("compareSyncId", () => {
  const sid = (s: string) => Schema.decodeUnknownSync(SyncId)(s)

  it("orders by numeric magnitude, not lexicographically", () => {
    // lexicographic would put "100" < "99"; numeric must not.
    assert.strictEqual(compareSyncId(sid("99"), sid("100")), -1)
    assert.strictEqual(compareSyncId(sid("100"), sid("99")), 1)
    assert.strictEqual(compareSyncId(sid("42"), sid("42")), 0)
  })

  it("orders very large cursors beyond Number.MAX_SAFE_INTEGER", () => {
    assert.strictEqual(
      compareSyncId(sid("9007199254740993"), sid("9007199254740994")),
      -1
    )
  })

  it("advances a durable cursor via Order.max (gap-tolerant)", () => {
    const advance = Order.max(compareSyncId)
    assert.strictEqual(advance(sid("5"), sid("9")), sid("9"))
    assert.strictEqual(advance(sid("9"), sid("5")), sid("9")) // out-of-order delivery
    assert.strictEqual(advance(sid("5"), sid("1000")), sid("1000")) // non-contiguous gap
  })
})
