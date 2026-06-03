import { describe, expect, it } from "vitest"
import { Schema } from "effect"
import { deriveSchemaVersion } from "../src/persistence/schema-version.js"

// Pure function over Effect Schema → regular vitest (no Effect runtime). We assert the *relation*
// between schemas (same ⇒ equal version, any structural change ⇒ different version), not the exact
// hash value — so the test survives an Effect AST-format change as long as the discrimination holds.
const Base = Schema.Struct({ id: Schema.String, name: Schema.String, age: Schema.Number })

describe("deriveSchemaVersion", () => {
  it("is deterministic — the same schema yields the same version", () => {
    expect(deriveSchemaVersion(Base)).toBe(deriveSchemaVersion(Base))
    // A structurally-identical but separately-constructed schema agrees.
    const Same = Schema.Struct({ id: Schema.String, name: Schema.String, age: Schema.Number })
    expect(deriveSchemaVersion(Same)).toBe(deriveSchemaVersion(Base))
  })

  it("changes when a field is added or removed", () => {
    const Added = Schema.Struct({ id: Schema.String, name: Schema.String, age: Schema.Number, ok: Schema.Boolean })
    const Removed = Schema.Struct({ id: Schema.String, name: Schema.String })
    expect(deriveSchemaVersion(Added)).not.toBe(deriveSchemaVersion(Base))
    expect(deriveSchemaVersion(Removed)).not.toBe(deriveSchemaVersion(Base))
  })

  it("changes when a field is renamed", () => {
    const Renamed = Schema.Struct({ id: Schema.String, fullName: Schema.String, age: Schema.Number })
    expect(deriveSchemaVersion(Renamed)).not.toBe(deriveSchemaVersion(Base))
  })

  // The win over a names-only hash: a same-named field whose TYPE changed must still reset.
  it("changes when a field's type changes (same name)", () => {
    const Retyped = Schema.Struct({ id: Schema.String, name: Schema.Number, age: Schema.Number })
    expect(deriveSchemaVersion(Retyped)).not.toBe(deriveSchemaVersion(Base))
  })

  // Brands ride along in the AST string, so two different brands on the same base type differ.
  it("changes when a field's brand changes", () => {
    const A = Schema.Struct({ id: Schema.String.pipe(Schema.brand("A")) })
    const B = Schema.Struct({ id: Schema.String.pipe(Schema.brand("B")) })
    expect(deriveSchemaVersion(A)).not.toBe(deriveSchemaVersion(B))
  })

  it("returns a uint32", () => {
    const v = deriveSchemaVersion(Base)
    expect(Number.isInteger(v)).toBe(true)
    expect(v).toBeGreaterThanOrEqual(0)
    expect(v).toBeLessThanOrEqual(0xffffffff)
  })
})
