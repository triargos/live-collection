import type { Schema } from "effect"

/**
 * Derives the TanStack `schemaVersion` (a number) from an Effect Schema, so a model change
 * dump-and-rebuilds the local base automatically — no manual version to bump or forget (DEC-A4).
 *
 * The hash input is `String(schema.ast)` — the schema's full structural type string, e.g.
 * `{ readonly id: nonEmptyString & Brand<"ModelId">; readonly name: string }`. Unlike hosting's
 * field-names-only hash, this folds in **types and brands**, so `name: string → number` (same field
 * name) still changes the version. That matters under DEC-A2: we trust the local base, so a missed
 * type change would silently keep stale-typed rows.
 *
 * Trade-off: if Effect's AST-string format shifts between versions, the hash changes and you get a
 * spurious reset on upgrade — which is a harmless refetch, the right side of the trade vs. a *missed*
 * change (a real bug).
 *
 * FNV-1a 32-bit → `uint32`, the same family TanStack itself uses for table names.
 */
export const deriveSchemaVersion = (schema: Schema.Schema.Any): number => {
  const signature = String(schema.ast)
  let hash = 2166136261
  for (let i = 0; i < signature.length; i++) {
    hash ^= signature.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  return hash >>> 0 // unsigned 32-bit
}
