# Live Sync System — Task Breakdown

Derived from [`live-sync-system.md`](live-sync-system.md). Tackle one row at a time;
each links back to the spec paragraph it comes from.

**Organizing principle:** the spec is built around a *seam* — generic infrastructure
that never changes when you add an entity, vs. per-entity code written once per model
([§A.2 "the factory is the only seam"](live-sync-system.md#L1138-L1140),
[§14 dispatch registry](live-sync-system.md#L968-L1001)). That seam is the
library/app boundary. Three buckets, plus a shared contract that joins them:

- **Bucket 0** — shared wire contract (interface between frontend lib and backend)
- **Bucket A** — shared frontend library (generic, reusable across projects)
- **Bucket B** — per-app frontend wiring (entity-specific, consumes A)
- **Bucket C** — per-app backend (authoritative server)

Legend: `[generic]` = reusable infra · `[per-entity]` = written once per model.

---

## Cross-bucket sequencing (read first)

1. **Lock Bucket 0** before the client read path (A.6/A.7) or server endpoints
   (C.8/C.9) — [§B names this the prerequisite dependency](live-sync-system.md#L1291-L1294).
2. **A.3 (the spike) is a hard gate** — [§A.1 "Do not roll out before this passes"](live-sync-system.md#L1227-L1234).
3. **C.5 (`clientId` on `DomainEvent`) is the one invasive backend change**; settle the
   `EventClient` metadata API first — [§16.1](live-sync-system.md#L1101-L1101),
   [§clientId on DomainEvent](live-sync-system.md#L322-L345).
4. **Build A.1/A.2 (registry + scoping) early** — [§22: scoping, not the persistence backend, is the lever for large data](live-sync-system.md#L1191-L1211).
5. Backend C.1→C.6 can run in parallel with the client spike A.3; they converge only
   at the contract (Bucket 0) and the read-path endpoints.

**Settle before code** ([§16](live-sync-system.md#L1099-L1110)): `EventClient`
per-publish metadata API · `MembershipChangedEvent` shape · where the permission
resolver lives · Postgres/PgBouncer topology (affects prod bus swap, not the MVP).
Open `TODO`: [pin & record the TanStack DB version](live-sync-system.md#L1185-L1186).

---

## Bucket 0 — Shared contract kit (`@triargos/live-collection-protocol`)

Published, imported by **both** the frontend lib and the backend. Pure — `effect` + Schema +
`@effect/platform` only, zero I/O, zero server framework. Backend implements *against* this. Lock first.

- [ ] **0.1** `SyncEvent` (at rest) + `HydratedSyncEvent<T>` schemas with `encode`/`decode` both directions — [§5 Sync event](live-sync-system.md#L136-L166)
- [ ] **0.2** Sync-group grammar as functions — `deriveGroup`/`parseGroup`/`matches` (wildcards). Also drives the client's glob lifecycle keys — [§5 Sync group](live-sync-system.md#L166-L177)
- [ ] **0.3** Action codes `I/U/D/R` + resync sentinel codecs (`__all`/`__group:<id>`/`__model:<Name>`) — [§9](live-sync-system.md#L556-L580)
- [ ] **0.4** **Squasher** — pure §8 fold (moved here from backend C.6; both ends rely on it). **Property-test hard** (random sequences converge; `syncId` gap-tolerance). — [§8 Squashing](live-sync-system.md#L534-L556), [§12](live-sync-system.md#L599-L638)
- [ ] **0.5** Expected interface **types** (no implementations) — `ModelDescriptor<T,R>`, `SyncContext`, `DispatchArgs`, permission-resolver signature — [§5 Model registry](live-sync-system.md#L177-L210), [§5 SyncContext](live-sync-system.md#L210-L225), [§7](live-sync-system.md#L401-L408)
- [ ] **0.6** `/sync` + `/catchup?from=&group=` `HttpApi` definition → client derives a typed client, backend derives handler stubs — [§8](live-sync-system.md#L416-L556), [§13 group scoping](live-sync-system.md#L750-L772)

---

## Bucket A — Shared frontend library (reusable, frontend only)

### Tier 1 — collection scoping core (build early)

- [ ] **A.1** `[generic]` `CollectionRegistry` — untyped `Map<id, RegisteredCollection>`; `getOrCreate/getById/dispose/disposePattern` + `globToRegex` — [§14 CollectionRegistry](live-sync-system.md#L836-L891)
- [ ] **A.2** `[generic]` Lifecycle helpers — `disposeWorkspace`/`disposeAllWorkspaces`/`disposeEverything` over `disposePattern` — [§14 Lifecycle helpers](live-sync-system.md#L946-L968) · _dep: A.1_

### Tier 2 — persistence factory seam (spike FIRST)

- [ ] **A.3** `[generic]` **Persistence spike (the gate).** Validate `persistedCollectionOptions` (TanStack DB 0.6, SQLite-WASM) three-step flow on one small collection: hydrate-from-storage-on-mount → no full re-list when base exists → catchup deltas land through sync source & persist. Fall back to clean Dexie factory if broken. — [§A.1](live-sync-system.md#L1227-L1234), [§22 three-step flow](live-sync-system.md#L1213-L1223)
- [ ] **A.4** `[generic]` Factory builder `effectCollectionOptions(...)` wrapping `persistedCollectionOptions` + Effect runtime. Keep the shape of existing `create-effect-collection.ts`, swap internals. — [§14 factories](live-sync-system.md#L891-L946), [§A.2](live-sync-system.md#L1235-L1236), [§B](live-sync-system.md#L1268-L1271) · _dep: A.3_

### Tier 3 — sync transport + routing

- [ ] **A.5** `[generic]` Sync dispatch registry — `Map<ModelName, DispatchHandler>` + entity-agnostic resolver (`get(modelName)?.(event)`) — [§14 dispatch registry](live-sync-system.md#L968-L1001) · _dep: 0.1_
- [ ] **A.6** `[generic]` Client SSE service — Effect `Stream` decode, event queue, keep-alive/retry, echo suppression on `clientId`. Model after `client-sync-service.ts`. — [§8 GET /sync](live-sync-system.md#L479-L510), [§10](live-sync-system.md#L580-L589), [§B](live-sync-system.md#L1263-L1267) · _dep: 0.1, A.5_
- [ ] **A.7** `[generic]` `lastSyncId` durable store (self-owned, **not** framework `staleTime`) + catchup service: fetch `/catchup?from=`, feed through the **synced-store write path**, advance `lastSyncId` after applying. Model after `client-sync-catchup-service.ts`. — [§8 catchup](live-sync-system.md#L456-L479), [§22 fixed constraints](live-sync-system.md#L1129-L1141), [§A.5](live-sync-system.md#L1245-L1247) · _dep: 0.1, 0.3, A.4_
- [ ] **A.8** `[generic]` Resync handling — on `__all`/`__group`/`__model` clear matching collections (via `disposePattern`) and trigger rebootstrap — [§9](live-sync-system.md#L556-L580) · _dep: A.1, A.5_

### Tier 4 — orchestration + offline

- [ ] **A.9** `[generic]` Bootstrap orchestrator — cold/warm start sequencing, capture `lastSyncId`, open `/sync`, catchup the gap, workspace-switch flow. Generic shell; app injects which collections to mount. — [§14 Bootstrap flow](live-sync-system.md#L1029-L1066), [§A.6](live-sync-system.md#L1248-L1249) · _dep: A.4, A.6, A.7_
- [ ] **A.10** `[generic]` Offline mutations — integrate `@tanstack/offline-transactions`. **Only after read path solid.** — [§A.7](live-sync-system.md#L1250-L1251) · _dep: A.4, A.9_
- [ ] **A.11** `[generic]` Unmounted-workspace event policy (default ignore; optional persist-only / lazy-mount) as a configurable hook — [§14 events for unmounted workspaces](live-sync-system.md#L1066-L1073) · _dep: A.5_

---

## Bucket B — Per-app frontend wiring (consumes A; one set per entity)

Not library code. Adding an entity = exactly these; the library (A) is untouched.

- [ ] **B.1** `[per-entity]` `create<Entity>Collection(args)` — id format, `schema`, `getKey`, `queryFn` → list endpoint, `onInsert/onUpdate/onDelete` paths. Global = no scope suffix; workspace = `<entity>:<orgId>`. — [§14 factories](live-sync-system.md#L891-L946), [§14 collection identity](live-sync-system.md#L812-L836)
- [ ] **B.2** `[per-entity]` `syncDispatchRegistry.register('<Entity>', handler)` — route hydrated event to scoped collection via `getById` — [§14 dispatch registry](live-sync-system.md#L968-L1001)
- [ ] **B.3** `[per-entity]` App-specific group helpers (e.g. `extractOrgFromGroups`) per the app's group grammar — [§14 dispatch registry](live-sync-system.md#L976-L986)
- [ ] **B.4** `[per-app]` Wire the app's global + active-workspace collections into the bootstrap orchestrator (A.9) — [§14 Two categories](live-sync-system.md#L799-L812), [§14 Bootstrap flow](live-sync-system.md#L1029-L1066)

---

## Bucket C — Per-app backend (authoritative server)

Spec §1–12, build order [§15](live-sync-system.md#L1073-L1099). Has its own
generic/per-entity split, but lives in each app's backend.

### Foundation

- [ ] **C.1** `[generic]` `sync_events` schema + migrations (GIN index on groups, lookup index) — [§4](live-sync-system.md#L105-L134) · _§15.1, 0.5d_
- [ ] **C.2** `[generic]` `SyncEventBus` interface + in-memory Effect `PubSub` (seam for Redis/LISTEN-NOTIFY later) — [§5 SyncEventBus](live-sync-system.md#L225-L241) · _§15.2, 0.5d_
- [ ] **C.3** `[generic]` `SyncEventRepository` — append, query-by-groups, query-by-syncId — [§7](live-sync-system.md#L364-L416) · _§15.3, 1d_
- [ ] **C.4** `[generic]` `SyncEventDispatcher` — thin: append row, best-effort bus publish — [§7](live-sync-system.md#L364-L416) · _§15.4, 0.5d_
- [ ] **C.5** `[generic]` **`clientId` on `DomainEvent` + propagate through `EventClient.publish`** — invasive prerequisite for echo suppression — [§clientId on DomainEvent](live-sync-system.md#L322-L345) · _§15.5, 1d_

### Read path

- [x] **C.6** → **moved to [0.4](#bucket-0--shared-contract-kit-triargoslive-collection-protocol)**. The squasher is contract behavior; it lives in `protocol` and the backend imports it. Backend work here = just wire it into `/catchup` (see C.8).
- [ ] **C.7** `[generic]` `SyncPermissionResolver.groupsFor({userId})` — derives groups from real memberships — [§8 Permission resolver](live-sync-system.md#L418-L456) · _§15.8, 1d_
- [ ] **C.8** `[generic]` `GET /catchup?from=&group=` — auth, resolve groups, retention check → inline `__all`, query, squash, **batched `hydrateMany`**, return `{events, lastSyncId}` — [§8 catchup](live-sync-system.md#L456-L479) · _§15.10, 2d_
- [ ] **C.9** `[generic]` `GET /sync` SSE — bus subscribe, group+echo filter, hydrate, synthetic-delete on ACL loss, `Last-Event-ID` implicit catchup, per-connection `Scope` — [§8 GET /sync](live-sync-system.md#L479-L510) · _§15.11, 2d_
- [ ] **C.10** `[generic]` Live-connection refresh via `MembershipChangedEvent` (settle its shape) — re-runs resolver, swaps cached group set — [§8 Live-connection refresh](live-sync-system.md#L510-L534) · _§15.12, 1d_
- [ ] **C.11** `[generic]` Resync events — all three variants (per-model / per-group / global); membership removal emits per-group resync tagged `user:<id>` — [§9](live-sync-system.md#L556-L580) · _§15.13, 1d_

### Per-entity

- [ ] **C.12** `[per-entity]` Model registry plumbing — `ModelDescriptor<T,R>` type + `ModelRegistry` map (`hydrateMany` from day one) — [§5 Model registry](live-sync-system.md#L177-L210) · _§15.9, 1d_
- [ ] **C.13** `[per-entity]` Projection-layer base + **first** `<entity>-sync-projection.server.ts` (one entity, one event) — verify the chain end-to-end — [§6](live-sync-system.md#L241-L308) · _§15.6, 1d_
- [ ] **C.14** `[per-entity]` Add `owner`/ownership refs to update/delete domain events being onboarded — [§6 Domain events need ownership refs](live-sync-system.md#L308-L322) · _§15.6_
- [ ] **C.15** `[per-entity]` Second & third entity projections + descriptors — [§15.16](live-sync-system.md#L1097-L1098) · _1d ea_
- [ ] **C.16** `[per-entity]` Re-tagging on ownership transfer (emit `D`-old + `I`-new) — only aggregates with transfer semantics — [§6 Re-tagging](live-sync-system.md#L345-L364)

### Ops & hardening

- [ ] **C.17** `[generic]` Retention job — nightly `DELETE … < 7 days`; measure `catchup_from_too_old` — [§11](live-sync-system.md#L589-L599) · _§15.15, 0.5d_
- [ ] **C.18** `[generic]` Property tests — random sequences; any client catching up from any `syncId` ends identical to DB; assert `syncId` gap-tolerance — [§12](live-sync-system.md#L599-L638) · _§15.14, 3d_
- [ ] **C.19** `[generic]` _(Optional, deferred)_ consolidated bootstrap endpoints `/organizations/:orgId/sync/bootstrap`, `/me/sync/bootstrap`. **Do NOT build day one** — only if latency justifies. — [§13 Optional consolidated bootstrap](live-sync-system.md#L712-L750)

---

## Out of scope ([§17](live-sync-system.md#L1110-L1118))

Cross-server scaling (single-node first; `SyncEventBus` is the seam) · WebSocket
transport (SSE suffices) · entity-table schema migrations (use existing tool).
