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
   ✅ **Done** — shipped + tested; design language locked in
   [`packages/protocol/DESIGN.md`](packages/protocol/DESIGN.md) (DEC-1…DEC-12).
2. **A.3 (the spike) is a hard gate** — [§A.1 "Do not roll out before this passes"](live-sync-system.md#L1227-L1234).
3. **Build A.1/A.2 (registry + scoping) early** — [§22: scoping, not the persistence backend, is the lever for large data](live-sync-system.md#L1191-L1211).
4. Backend C.1→C.6 can run in parallel with the client spike A.3; they converge only
   at the contract (Bucket 0) and the read-path endpoints.

**Settle before code** ([§16](live-sync-system.md#L1099-L1110)): `MembershipChangedEvent` shape ·
where the permission resolver lives · Postgres/PgBouncer topology (affects prod bus swap, not the
MVP). ~~Open `TODO`: pin & record the TanStack DB version~~ → **resolved:** pinned exactly at `0.6.7` (CLAUDE.md DEC-2).

---

## Bucket 0 — Shared contract kit (`@triargos/live-collection-protocol`) ✅ shipped

Published, imported by **both** the frontend lib and the backend. Pure — **`effect` only** (Schema
included), zero I/O, zero server framework, **no `@effect/platform`** (the kit ships schemas, not an
HTTP surface — DEC-7). Backend implements *against* this. **Locked + implemented**; the as-built
design language is [`packages/protocol/DESIGN.md`](packages/protocol/DESIGN.md) (decisions
DEC-1…DEC-12). 33 tests green — `typecheck` + `vitest` PASS.

Where these rows read differently than the original task wording, the difference is the locked
design — see the linked decision after each.

- [x] **0.1** Event families with `encode`/`decode` both directions: `PendingSyncEvent` (producer input) · `SyncEvent` (at rest, reference-only) · `HydratedSyncEvent<T>` (wire) + the opaque-`data` envelope. Action is the `_tag` (`Insert`/`Update`/`Delete`/`Resync`), and `data` presence is **structural** — present on `Insert`/`Update`, absent on `Delete` — not `Option<data>` (DEC-6). No `clientId` on any arm (DEC-11). — [§5 Sync event](live-sync-system.md#L136-L166)
- [x] **0.2** Sync-group grammar as functions — `deriveGroup`/`parseGroup`, plus the two relations the old single `matches` conflated: `intersects` (literal set overlap — ACL-critical delivery) and `isUnder` (segment-prefix containment incl. equality — scope + resync matching). **Literal-only on the wire, no wildcards/regex** (DEC-4); subscriber brace-sugar deferred to a client builder (DEC-5). — [§5 Sync group](live-sync-system.md#L166-L177)
- [x] **0.3** Resync as a **structural** tagged union `ResyncTarget` = `All` / `Group(group)` / `Model(model)`. The `__all`/`__group:<id>`/`__model:<Name>` sentinel codecs and single-char `I/U/D/R` action codes are **removed** — resync is structural, action is `_tag`-only (DEC-9, DEC-6). — [§9](live-sync-system.md#L556-L580)
- [x] **0.4** **Squasher** — pure §8 fold (moved here from backend C.6; both ends rely on it). Folds on `(modelName, modelId, _tag, syncGroups, syncId)`, never entity data; resync overrides drop preceding events via `isUnder`; folded runs carry the latest `syncId`; idempotent. **Property-tested hard** (random sequences converge from any `from`; `syncId` gap-tolerance). — [§8 Squashing](live-sync-system.md#L534-L556), [§12](live-sync-system.md#L599-L638)
- [x] **0.5** Expected interface **types** (no implementations, zero runtime cost) — `ModelDescriptor<Name,T,R>` (with optional batch `hydrateMany`), `SyncContext`, `GroupsFor` (permission-resolver signature) — plus the open→closed model-name seam: `defineModelRegistry` / `narrowModelName` / `UnknownModelError`. **No `DispatchArgs`**: producers build a complete `PendingSyncEvent` via the tagged constructors and the dispatcher accepts only that (DEC-8). — [§5 Model registry](live-sync-system.md#L177-L210), [§5 SyncContext](live-sync-system.md#L210-L225), [§7](live-sync-system.md#L401-L408)
- [x] **0.6** `/catchup` **request/response schemas only** — `CatchupRequest { from }` + `CatchupResponse { events, lastSyncId }`. **Not an `HttpApi`**: the backend owns route/method/errors/headers/auth and wires the schemas in (DEC-7). **No `group` param** — the server resolves the caller's groups from permissions server-side (DEC-12). `/sync` SSE is a backend detail, deliberately absent from the contract. — [§8](live-sync-system.md#L416-L556), [§13 group scoping](live-sync-system.md#L750-L772)

---

## Bucket A — Shared frontend library (reusable, frontend only)

### Tier 1 — collection scoping core (build early)

- [x] **A.1** `[generic]` `CollectionRegistry` — untyped `Map<id, RegisteredCollection>`; `getOrCreate/getById/dispose/disposePattern` + `globToRegex` — [§14 CollectionRegistry](live-sync-system.md#L836-L891)
- [x] **A.2** `[generic]` Lifecycle helpers — `disposeWorkspace`/`disposeAllWorkspaces`/`disposeEverything` over `disposePattern` — [§14 Lifecycle helpers](live-sync-system.md#L946-L968) · _dep: A.1_

### Tier 2 — persistence factory seam (spike FIRST)

- [x] **A.3** `[generic]` **Persistence spike (the gate).** Validate `persistedCollectionOptions` (TanStack DB 0.6, SQLite-WASM) three-step flow on one small collection: hydrate-from-storage-on-mount → no full re-list when base exists → catchup deltas land through sync source & persist. Fall back to clean Dexie factory if broken. — [§A.1](live-sync-system.md#L1227-L1234), [§22 three-step flow](live-sync-system.md#L1213-L1223)
- [x] **A.4** `[generic]` Factory builder `effectCollectionOptions(...)` wrapping `persistedCollectionOptions` + Effect runtime. Keep the shape of existing `create-effect-collection.ts`, swap internals. — [§14 factories](live-sync-system.md#L891-L946), [§A.2](live-sync-system.md#L1235-L1236), [§B](live-sync-system.md#L1268-L1271) · _dep: A.3_

### Tier 3 — sync transport + routing

- [x] **A.5** `[generic]` Sync dispatch registry — `Map<ModelName, DispatchHandler>` + entity-agnostic resolver (`get(modelName)?.(event)`) — [§14 dispatch registry](live-sync-system.md#L968-L1001) · _dep: 0.1_
- [x] **A.6** `[generic]` Client SSE service — `SyncTransport.connect` (Effect `Stream` decode, keep-alive timeout, **fails on drop** so the orchestrator reconnects — DEC-T4). `src/client/sync-transport.ts`. — [§8 GET /sync](live-sync-system.md#L479-L510), [§10](live-sync-system.md#L580-L589), [§B](live-sync-system.md#L1263-L1267) · _dep: 0.1, A.5_
- [x] **A.7** `[generic]` `LastSyncIdStore` durable cursor (localStorage, self-owned — **not** `staleTime`, DEC-T2) + `CatchupClient` (`/catchup?from=` → synced-store write path via the dispatcher; cursor from `CatchupResponse.lastSyncId`, DEC-T3). `src/client/{last-sync-id-store,catchup-client}.ts`. — [§8 catchup](live-sync-system.md#L456-L479), [§22 fixed constraints](live-sync-system.md#L1129-L1141), [§A.5](live-sync-system.md#L1245-L1247) · _dep: 0.1, 0.3, A.4_
- [x] **A.8** `[generic]` Resync handling — **blunt, target-ignored** (DEC-T6): a catchup-response resync ⇒ snapshot via `bootstrapFn`; a *live* resync ⇒ `cursor.clear *> onResync` (full reload, Model A). Folded into `SyncClient.start`. — [§9](live-sync-system.md#L556-L580) · _dep: A.1, A.5_

### Tier 4 — orchestration + offline

- [x] **A.9** `[generic]` Bootstrap orchestrator — `SyncClient.start(specs)`: cold/warm unified (`from = cursor ?? "0"`, DEC-T5), catchup the gap, tail SSE forever (reconnect re-runs catchup), snapshot-and-tail via `bootstrapSpec`. `src/client/sync-client.ts`. Workspace-switch incremental mount **deferred** (flagged in DESIGN). — [§14 Bootstrap flow](live-sync-system.md#L1029-L1066), [§A.6](live-sync-system.md#L1248-L1249) · _dep: A.4, A.6, A.7_
- [x] **A.R** `[generic]` **Native-collection redesign + React bindings** (DEC-R1…R9, see `packages/live-collection/DESIGN.md`). Collections are native TanStack collections: `defineCollection({runtime,…})` returns a registry-backed handle → `useLiveQuery(() => coll)`. `makeLiveRuntime` (two-surface: sync registry+persistence value | async loop), `liveCollectionOptions` inner creator, explicit `SyncMap`-driven `syncLoop`, `react/useLiveSync`. **Revises:** A.4 (`effectCollectionOptions`→`liveCollectionOptions`), A.5 (`SyncDispatcher.fromEntries`/`dispatchEntry` retired → internal, map-driven), A.9 (`SyncClient.start(specs)`→`syncLoop(map)`), DEC-6/A2 (`PersistenceBase` tag → app value), DEC-10 (`MountRef`→handle, `scopeOf (entity)=>string`). 38 live-collection tests green; react type-test green. Both `useLiveQuery` forms (direct + `q.from` join) typecheck natively (DEC-R9 resolved via a structural-only `SyncWrite` index signature, no `any`).
- [~] **A.10** `[generic]` **Optimistic write path — ONLINE slice DONE.** `defineCollection<T,R>` gains optional native TanStack `onInsert/onUpdate/onDelete` as **Effect-returning** handlers (with `R`) + a `services: ManagedRuntime<R>` that discharges them (and the now-`R`-carrying `listFn`); apps reconcile via the existing `collection.utils.writeSynced`/`deleteSynced`. **Model B** (handler writes synced *before* resolving — required by TanStack 0.6.7 `state.js:884` tx-cleanup timing, no flicker). Ids are the app's; **client-minted ⇒ idempotent self-echo, validates DEC-8** (no `clientId`/echo-suppression). NOT built on `@tanstack/offline-transactions` (doesn't exist). 4 node write-path tests + 1 OPFS browser e2e (optimistic insert → confirm → idempotent echo → remote insert → persisted across reload) + a full cross-tab demo (shared-log + BroadcastChannel backend, debug inspector) in `examples/playground` (commit `6445465`). **DEFERRED:** offline-durable mutation queue (persisted collections persist the *synced* store, not the optimistic overlay — needs a durable mutation log). — [§A.7](live-sync-system.md#L1250-L1251) · _dep: A.4, A.9_
- [ ] **A.11** `[generic]` Unmounted-workspace event policy (default ignore; optional persist-only / lazy-mount) as a configurable hook. Complements A.12: A.11 governs events for a *not-mounted* scope; A.12 bootstraps a scope *on* mount. — [§14 events for unmounted workspaces](live-sync-system.md#L1066-L1073) · _dep: A.5_
- [ ] **A.12** `[generic]` **Snapshot-on-mount (DEC-A12 — the "loadFn recall").** A scope mounted *after* its events streamed past renders **empty**: `syncLoop` drops events for not-yet-mounted collections (`getById ⇒ None ⇒ ignore`) **yet advances the global cursor past them**, and mount is network-free (OPFS hydrate only) — `listFn` runs only on `Resync`. Fix: on first `getOrCreate`, bootstrap via the existing `snapshotInstance` reconcile (`listFn(scope) → writeSynced + delete-absent`), gated by a freshness check (never-bootstrapped vs. cursor staleness). Reproducible single-tab; **supersedes the A.9 "workspace-switch incremental mount deferred" flag.** Design-first: weigh snapshot-on-mount vs. eager-mount (globals only) vs. local replay queue vs. per-scope cursors. The `examples/playground` `listFn` is the ready-made acceptance test. See `packages/live-collection/DESIGN.md` **DEC-A12** + `HANDOFF.md`. · _dep: A.9, A.10, A.R.opfs_
- [x] **A.R.opfs** `[generic]` Browser OPFS persistence value so the read path is browser-runnable (node only has the test sqlite persistence). No library code: prod uses the official `@tanstack/browser-db-sqlite-persistence@0.1.11` — `await openBrowserWASQLiteOPFSDatabase({ databaseName })` → `createBrowserWASQLitePersistence({ database })` over `@journeyapps/wa-sqlite`. Deliverable is the `examples/playground` wiring + OPFS browser smoke (the A.3 gate node can't prove). _dep: A.R_

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
- [x] **C.5** → **removed (DEC-11).** `clientId` / echo suppression is dropped for now — it conflicts with TanStack DB's optimistic-mutation reconciliation, and the wire shape never carried it. No `DomainEvent` or `EventClient.publish` change. If reintroduced, it belongs as a *client-side reconciliation key*, not a server filter — [DEC-11](CLAUDE.md), [`packages/protocol/DESIGN.md`](packages/protocol/DESIGN.md).

### Read path

- [x] **C.6** → **moved to [0.4](#bucket-0--shared-contract-kit-triargoslive-collection-protocol)**. The squasher is contract behavior; it lives in `protocol` and the backend imports it. Backend work here = just wire it into `/catchup` (see C.8).
- [ ] **C.7** `[generic]` `SyncPermissionResolver.groupsFor({userId})` — derives groups from real memberships — [§8 Permission resolver](live-sync-system.md#L418-L456) · _§15.8, 1d_
- [ ] **C.8** `[generic]` `GET /catchup?from=&group=` — auth, resolve groups, retention check → inline `__all`, query, squash, **batched `hydrateMany`**, return `{events, lastSyncId}` — [§8 catchup](live-sync-system.md#L456-L479) · _§15.10, 2d_
- [ ] **C.9** `[generic]` `GET /sync` SSE — bus subscribe, group filter, hydrate, synthetic-delete on ACL loss, `Last-Event-ID` implicit catchup, per-connection `Scope` — [§8 GET /sync](live-sync-system.md#L479-L510) · _§15.11, 2d_
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
