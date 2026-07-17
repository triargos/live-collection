import { type Schema, SchemaRepresentation } from "effect"

/**
 * Derives the TanStack `schemaVersion` (a number) from an Effect Schema, so a model
 * change dump-and-rebuilds the persisted local table automatically — no manual version
 * to bump or forget. `defineCollection` calls this for you; use it directly only when
 * assembling a persisted collection by hand.
 *
 * The hash input is the JSON representation derived from `schema.ast` — the schema's
 * full structural shape, including fields, checks, and brands. It
 * folds in **types and brands**, not just field names, so changing `name: string` to
 * `name: number` still changes the version. That matters because the library trusts the
 * local base: a missed type change would silently keep stale-typed rows.
 *
 * Trade-off: if Effect's AST-string format shifts between versions, the hash changes and
 * you get a spurious reset on upgrade — a harmless refetch, the right side of the trade
 * vs. a *missed* change (a real bug).
 *
 * FNV-1a 32-bit → `uint32`, the same family TanStack itself uses for table names.
 */
export const deriveSchemaVersion = (schema: Schema.Top): number => {
  const signature = JSON.stringify(SchemaRepresentation.fromAST(schema.ast))
  let hash = 2166136261
  for (let i = 0; i < signature.length; i++) {
    hash ^= signature.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  return hash >>> 0 // unsigned 32-bit
}
