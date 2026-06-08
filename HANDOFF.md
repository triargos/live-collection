# Handoff — `@triargos/live-collection` transport tier (read path) landed

_Updated 2026-06-08. The transport tier A.6–A.9 (SSE transport, durable cursor, catchup, bootstrap
orchestrator, resync) is designed-first-locked, implemented, and green. Bucket A is now feature-
complete for the **read path**; what remains is React bindings, the browser OPFS layer, and offline
mutations (A.10/A.11)._

## Goal
Build the frontend-only Effect + TanStack DB live-sync library. Prior sessions shipped Bucket A's
registry/scoping (A.1/A.2), the persistence factory + A.3 gate (A.3/A.4), and the dispatcher (A.5).
**This session shipped the transport tier (A.6–A.9)** — the whole client read path over that factory.

## Source of truth — read these first, don't re-derive
- **`packages/live-collection/DESIGN.md`** — TWO locked designs now:
  - "Bucket A — the collection factory & persistence base" (DEC-A1…A14), and
  - "**Transport tier (A.6–A.9)**" appended at the end (DEC-T1…T9, the loop, deferred items).
- **`packages/protocol/DESIGN.md`** — the wire contract (DEC-1…DEC-12). `SyncId` admits `"0"` (the
  cold-start cursor); `CatchupResponse` carries `lastSyncId`; squasher is pure + property-tested.
- Commits (most recent first): `5632dc1` (feat: transport tier A.6–A.9 — this session),
  `6fc97c7` (wire dispatcher↔factory), `ba2dc1c` (factory + gate), `5ea3491` (dispatcher).
- `TASKS.md` (Bucket A **A.1–A.9 now checked**), `CLAUDE.md` decisions, `live-sync-system.md`.

## Progress
**Done & verified this session** (`pnpm -r typecheck` exit 0 all 3 packages; `pnpm -r test` exit 0 —
**77 tests**: protocol 33, live-collection 44; fiber tests non-flaky over 3 repeats):
- `src/client/last-sync-id-store.ts` — `LastSyncIdStore`: global durable cursor, **monotonic `set`**
  by `compareSyncId`. `layer` (localStorage; faults → orDie), `layerMemory` (Ref).
- `src/client/catchup-client.ts` — `CatchupClient.fetch({from}) → CatchupResponse`, decoded against
  the schema at the boundary; `CatchupFailed` is **modeled + recoverable** (orchestrator tails anyway).
  `layer({url})` over `HttpClient`, `layerMemory(canned)`.
- `src/client/sync-transport.ts` — `SyncTransport.connect: Stream<HydratedSyncEventEnvelope,
  SyncConnectionLost>`, one decoded SSE connection that **fails on drop** (DEC-T4). Hides line-framing,
  keep-alive timeout, JSON/envelope decode (bad lines skipped+logged). `layer({url, keepAlive})`,
  `layerMemory(queue)`.
- `src/client/sync-client.ts` — `SyncClient.start(specs)`: the orchestrator. `BootstrapSpec` +
  `bootstrapSpec` erase-helper (mirrors `dispatchEntry`); `reloadWindow` convenience; `layer({onResync})`.
  A.8 resync is folded in. **The loop:** `from = cursor ?? "0"` → catchup → (resync arm ⇒ snapshot
  every spec via `bootstrapFn` + delete-absent reconcile ; else dispatch deltas) → `cursor.set(lastSyncId)`
  → tail SSE forever (retry re-runs catchup); a **live** resync ⇒ `cursor.clear *> onResync` then stop.
- `src/index.ts` — now exports the `client/` modules, the persistence seam (`effectCollectionOptions`,
  `PersistenceBase`, `deriveSchemaVersion`), and the **real** `LiveCollection` hero type (was a stale
  A.4 placeholder). `packages/react/src/index.ts` — `T extends object` to match the real type.
- Tests: `last-sync-id-store` (3), `catchup-client` (3, incl. boundary decode + non-2xx→CatchupFailed
  via a real in-memory `HttpClient`), `sync-transport` (2), `sync-client.integration` (5 — cold/snapshot,
  warm/delta, live insert+delete, live resync→onResync+clear, snapshot delete-absent; real registry +
  factory + dispatcher, memory transport/catchup/cursor).

**Locked design decisions (DEC-T*, see DESIGN.md):** Model Y (one global connection + central
dispatch); localStorage cursor accepting fire-and-forget skew (**revises DEC-A14** — no alpha
durability handle); cursor's single source of truth = the sync stream, `bootstrapFn` returns
**rows-only** (DEC-T3); blunt **full-reload** resync, target ignored, no `groupScope` hook (DEC-T6,
user's call); cold/warm unified by `from = cursor ?? "0"` with a catchup-`Resync` triggering the
snapshot (DEC-T5, preserves snapshot-and-tail for free).

**Not started (the "next thing", roughly in order):**
1. **React bindings** (`packages/react`) — `useLiveCollection` over `@tanstack/react-db` + a runtime
   provider that runs `SyncClient.start` (forked) and provides the registry/dispatcher/client layers.
   Currently a typed skeleton. Do **design-first** for the provider/runtime shape.
2. **Browser OPFS `PersistenceBase.layer`** (`persistence-base-opfs.ts`, its OWN module so node never
   loads wa-sqlite — see DESIGN §3 platform split). **Today only `layerSqliteDriver` (test infra)
   exists, so the read path is NOT browser-runnable yet.** This + the playground OPFS smoke is the one
   thing node can't prove.
3. **A.10 offline mutations** (`@tanstack/offline-transactions`) — only now that the read path is solid.
   **A.11** unmounted-workspace event policy (default ignore).
4. **Workspace-switch incremental bootstrap** — mounting a new scope while the cursor is `Some` won't
   snapshot it (the global-cursor cold/warm signal is all-or-nothing). Deferred; flagged in DESIGN.
   Likely needs per-collection "has a base?" detection or DEC-A12's `lastBootstrapAt` threshold.
5. **Playground / examples** scaffold (not yet created) to exercise OPFS + a reference Bucket-B wiring.

> Bucket B (per-app entity wiring) and Bucket C (the backend, incl. the real `/sync` + `/catchup`
> handlers the client talks to) are out of the library; they incubate per-app / in `examples/server`.

## Gotchas discovered (non-obvious; would burn time again)
- **`Effect.retry({ while, schedule })` does NOT remove the error from the type.** `while` only
  *gates* retries; the residual channel still carries the error. `SyncClient.start` discharges both
  `SyncConnectionLost` (unreachable — infinite `spaced`) and `ResyncStop` via `Effect.catchTags` to get
  a `never` channel. Don't expect `retry` to narrow.
- **Resync has TWO contexts, handled differently** (DEC-T6): a resync **in a catchup response** ⇒
  snapshot (no reload → no infinite loop); a **live** resync in SSE ⇒ reload. They're different code
  locations in `start`, so no ambiguity. This is what breaks the cold-start-resync-loop.
- **`bootstrapFn` returns rows only — never a syncId.** The cursor comes from `CatchupResponse.lastSyncId`
  (and live `event.syncId`). A mature server (older than its retention window) answers `from:"0"` with
  `Resync(All)` + a `lastSyncId`, so cold start naturally snapshots AND gets a cursor. Don't reintroduce
  a `syncId` on the list endpoint.
- **`SyncClient` has ONE impl** — there is no `layerMemory` for it; the variation is in its *deps*
  (store/catchup/transport each have `layerMemory`). Tests share the memory layers between the client
  and the test via `Layer.provideMerge` so cursor assertions see the same store instance.
- **Integration tests assert IN-MEMORY (`coll.has`), not cross-reload** — so the fire-and-forget persist
  isn't a factor here (in-memory upsert via the session is synchronous). The A.3 *gate* tests still use
  `it.live` + `reloadUntil` because they cross a reload. Fork `start` with `forkScoped`, poll a condition
  with a small-sleep helper, then interrupt.
- **`SyncTransport.layerMemory` ends the stream when the queue is shut down**, then concats a
  `SyncConnectionLost` — that's how the "drop" contract is tested. Don't close the queue mid-integration-
  test unless you want a reconnect.

## Constraints / preferences reinforced
- Design-first with hard grilling; the user makes decisive simplifying calls (full-reload resync over a
  `groupScope` hook; "cursor from catchup, ignore too-old" over coupling list endpoints). Bring the
  trade-off + a recommendation, one decision at a time.
- **Reuse over invention**: the hosting repo (`~/IdeaProjects/hosting/apps/dashboard`) is the reference
  — `client-sync-{service,catchup-service,resolver}.ts`, `bootstrap-service.ts` — but it uses
  `Effect.Service`, `catchAllCause`, and `data as T` casts, all **banned here**. Carry the *ideas*
  (stream-decode shape, self-owned cursor), not the mechanics.
- Verify framework APIs from source (did so for `@effect/platform` HttpClient/Response, TanStack
  `Collection.keys()`, Effect `Stream`/`retry`). The user catches overclaims — back every claim with a
  test and report PASS only on an observed exit 0.
- Commit only when asked; the user commits **directly on `main`**. No co-author footer.

## Suggested skills for the next agent
- **`design-first`** before the React bindings / OPFS layer — both are new seams (a runtime provider; a
  platform-split persistence layer). Lock the interface with the user first.
- **`tdd`** for the implementation — red→green; the 77 existing tests guard the read path, registry,
  dispatch, and persistence seams against regressions.
