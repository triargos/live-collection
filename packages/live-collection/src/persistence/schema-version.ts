import { Schema, SchemaRepresentation } from "effect"

/**
 * The derived persisted-schema version — a branded FNV-1a hash of the schema's
 * structural shape. It crosses two seams under one identity: TanStack's persistence
 * config (where it widens to a plain `number`) and the event log's base-watermark key
 * (where it namespaces the watermark to the base it describes).
 */
export const SchemaVersion = Schema.Number.pipe(Schema.brand("SchemaVersion"))
export type SchemaVersion = typeof SchemaVersion.Type

/**
 * Derives the TanStack `schemaVersion` from an Effect Schema, so a model
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
 * A version change has **two** effects that must stay in lockstep: TanStack dumps and
 * rebuilds the persisted local table, and — because the version is part of the base
 * watermark's identity in the event log — the old watermark is orphaned, so the next
 * mount decides `Snapshot` and re-lists from the server. Were the watermark not
 * versioned, it would survive the table dump and the mount would `Skip` against an
 * empty base, silently freezing the collection.
 *
 * Trade-off: if Effect's AST-representation format shifts between versions, the hash
 * changes and you get a spurious reset on upgrade — a harmless refetch, the right side
 * of the trade vs. a *missed* change (a real bug).
 *
 * FNV-1a 32-bit → `uint32`, the same family TanStack itself uses for table names.
 */
export const deriveSchemaVersion = (schema: Schema.Top): SchemaVersion => {
  const signature = JSON.stringify(SchemaRepresentation.fromAST(schema.ast))
  let hash = 2166136261
  for (let i = 0; i < signature.length; i++) {
    hash ^= signature.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  return SchemaVersion.make(hash >>> 0) // unsigned 32-bit
}
