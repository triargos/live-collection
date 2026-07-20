import { Schema } from "effect"
import * as fc from "effect/testing/FastCheck"
import { assert, describe, it } from "@effect/vitest"
import { deriveGroup, intersects, SyncGroup } from "../src/sync-group.js"

const decode = Schema.decodeUnknownResult(SyncGroup)
const g = (s: string): SyncGroup => Schema.decodeUnknownSync(SyncGroup)(s)

const groupArb: fc.Arbitrary<SyncGroup> = fc
  .stringMatching(/^[a-z0-9:-]{1,32}$/)
  .map(g)

describe("SyncGroup schema", () => {
  it("accepts any non-empty string — structure is an app convention, not grammar", () => {
    for (const raw of ["user:bob", "organization:abc:channel:xyz", "a", "playground", "a::b"]) {
      assert.strictEqual(decode(raw)._tag, "Success", `expected ${raw} to decode`)
    }
  })

  it("rejects the empty string", () => {
    assert.strictEqual(decode("")._tag, "Failure")
  })
})

describe("deriveGroup", () => {
  it("joins segments with ':' and equals the equivalent literal", () => {
    assert.strictEqual(deriveGroup(["organization", "abc"]), g("organization:abc"))
    assert.strictEqual(deriveGroup(["organization", "abc", "channel", "xyz"]), g("organization:abc:channel:xyz"))
  })
})

describe("intersects (literal overlap, ACL-critical)", () => {
  it("is true iff some literal group is shared", () => {
    assert.isTrue(intersects([g("user:bob")], [g("user:bob"), g("org:a")]))
    assert.isFalse(intersects([g("user:bob")], [g("user:alice")]))
  })

  it("is NEVER hierarchical — a structurally nested name does not intersect its prefix", () => {
    assert.isFalse(intersects([g("organization:a")], [g("organization:a:channel:x")]))
  })

  it("is symmetric and overlap-exact", () => {
    fc.assert(
      fc.property(
        fc.array(groupArb, { maxLength: 5 }),
        fc.array(groupArb, { maxLength: 5 }),
        (a, b) => {
          assert.strictEqual(intersects(a, b), intersects(b, a))
          const overlap = a.some((x) => b.includes(x))
          assert.strictEqual(intersects(a, b), overlap)
        }
      )
    )
  })
})
