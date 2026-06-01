import { FastCheck as fc, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import {
  deriveGroup,
  intersects,
  isUnder,
  parseGroup,
  SyncGroup
} from "../src/sync-group.js"

const decode = Schema.decodeUnknownEither(SyncGroup)
const g = (s: string): SyncGroup => Schema.decodeUnknownSync(SyncGroup)(s)

// Arbitrary literal groups: 1-4 segments of colon-free, non-empty tokens.
const segment = fc.stringMatching(/^[a-z0-9-]{1,8}$/)
const groupArb: fc.Arbitrary<SyncGroup> = fc
  .array(segment, { minLength: 1, maxLength: 4 })
  .map((segments) => g(segments.join(":")))

describe("SyncGroup schema", () => {
  it("accepts well-formed colon paths", () => {
    for (const raw of ["user:bob", "organization:abc:channel:xyz", "a"]) {
      assert.strictEqual(decode(raw)._tag, "Right", `expected ${raw} to decode`)
    }
  })

  it("rejects empty segments and the empty string", () => {
    for (const raw of ["", ":", "a:", ":a", "a::b", "organization:"]) {
      assert.strictEqual(decode(raw)._tag, "Left", `expected ${raw} rejected`)
    }
  })
})

describe("deriveGroup / parseGroup round-trip", () => {
  it("parseGroup ∘ deriveGroup === identity on segments", () => {
    fc.assert(
      fc.property(fc.array(segment, { minLength: 1, maxLength: 5 }), (segments) => {
        const derived = deriveGroup(segments as [string, ...string[]])
        assert.deepStrictEqual([...parseGroup(derived).segments], segments)
      })
    )
  })

  it("deriveGroup ∘ parseGroup === identity on groups", () => {
    fc.assert(
      fc.property(groupArb, (group) => {
        assert.strictEqual(deriveGroup(parseGroup(group).segments), group)
      })
    )
  })
})

describe("intersects (literal overlap, ACL-critical)", () => {
  it("is true iff some literal group is shared", () => {
    assert.isTrue(intersects([g("user:bob")], [g("user:bob"), g("org:a")]))
    assert.isFalse(intersects([g("user:bob")], [g("user:alice")]))
  })

  it("is NEVER hierarchical — a child does not intersect its parent", () => {
    // org:a:channel:x is "under" org:a, but intersects must stay literal.
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

describe("isUnder (segment-prefix, incl. equality)", () => {
  it("is reflexive", () => {
    fc.assert(fc.property(groupArb, (group) => assert.isTrue(isUnder(group, group))))
  })

  it("matches a deeper child but not a sibling-prefix", () => {
    assert.isTrue(isUnder(g("organization:abc"), g("organization:abc:channel:xyz")))
    assert.isFalse(isUnder(g("organization:abc"), g("organization:abcd")))
    assert.isFalse(isUnder(g("organization:abc:channel:xyz"), g("organization:abc")))
  })

  it("any group is under each of its own prefixes", () => {
    fc.assert(
      fc.property(fc.array(segment, { minLength: 1, maxLength: 5 }), (segments) => {
        const full = deriveGroup(segments as [string, ...string[]])
        for (let i = 1; i <= segments.length; i++) {
          const prefix = deriveGroup(segments.slice(0, i) as [string, ...string[]])
          assert.isTrue(isUnder(prefix, full))
        }
      })
    )
  })
})
