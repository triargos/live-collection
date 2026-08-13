---
"@triargos/live-collection-protocol": minor
"@triargos/live-collection-server": minor
"@triargos/live-collection": minor
"@triargos/live-collection-react": minor
---

Upgrade to Effect `4.0.0-rc.108`.

The `effect` peer range moves to `^4.0.0-rc.108`. Upgrade Effect in lockstep; a
workspace cannot mix beta and rc in one type graph.

Renamed APIs, applied across all packages:

- `Schema.TaggedErrorClass` is now `Schema.TaggedError`.
- The `SchemaError` module folded into `Schema`; the decode failure type is
  `Schema.SchemaError`.
- `SchemaRepresentation.fromAST` is now `SchemaRepresentation.toRepresentation`.

Exported error classes keep their tags and fields, so `catchTag` call sites do
not change.

**One-time local rebuild on first load.** `deriveSchemaVersion` hashes Effect's
AST representation, and that representation changed shape. Every collection
derives a new version, so TanStack dumps and rebuilds the persisted local table,
the journal mark is orphaned, and the mount decides `Snapshot` and re-lists from
the server. This costs one full refetch per collection and then settles.
