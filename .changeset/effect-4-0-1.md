---
"@triargos/live-collection-protocol": minor
"@triargos/live-collection-server": minor
"@triargos/live-collection": minor
"@triargos/live-collection-react": minor
---

Upgrade to Effect `4.0.1`, the first stable v4 release.

The `effect` peer range moves to `^4.0.1`. Upgrade Effect in lockstep; a workspace cannot
mix Effect versions in one type graph. No library API changes.

**Brands no longer affect the derived schema version.** `Schema.brand` is TypeScript-only
in Effect `4.0.1` and absent from the schema AST that `deriveSchemaVersion` hashes. A
brand-only change keeps the version; field names, types, and checks still change it.

**One-time local rebuild for branded schemas.** A collection whose schema contains a
brand (for example a branded id field) derives a new version on first load: TanStack
dumps and rebuilds the persisted local table and the collection re-lists from the server
once. Unbranded schemas keep their version.
