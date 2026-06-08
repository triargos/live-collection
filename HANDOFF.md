# Handoff — next: make the read path browser-runnable (OPFS), then offline mutations

_Updated 2026-06-08. The native-collection redesign + React bindings are **done, committed, and green**
(`faa8aa2` feat(live-collection), `9957c94` feat(react)). `pnpm -r typecheck` / `test` / `build` all
exit 0 (protocol 33, live-collection 38, react type-test). This handoff is forward-looking — the design
and rationale live elsewhere (see below), don't re-derive them._

## Goal
Frontend-only Effect + TanStack DB live-sync library. Collections are now **native TanStack collections**
(`defineCollection({runtime,…})` → handle → `useLiveQuery` directly). The read path is verified in node
but is **not yet runnable in a browser** — that's the next milestone.

## Read these first (don't re-derive)
- `packages/live-collection/DESIGN.md` → section **"Native-collection redesign + React bindings (DEC-R*)"**
  — the UX, the acyclic DAG, the two-surface mount/loop mechanic, DEC-R1…R9 (R9 resolved).
- Memory: `[[native-collection-redesign]]`, `[[live-collection-test-harness-quirk]]`.
- `TASKS.md` — task **A.R** (done), **A.R.opfs** / **A.10** / **A.11** (open).
- Commits `faa8aa2`, `9957c94` for the full diff.

## Next steps (ordered)
1. **A.R.opfs — browser OPFS persistence (the gating item).** Today only the node test sqlite persistence
   exists (`test/sqlite-persistence.ts`), so nothing has run in a real browser. The app passes a
   `PersistedCollectionPersistence` **value** to `makeLiveRuntime({ persistence })` (DEC-R3); production
   builds it with `createOpfsSQLitePersistence(...)` (or the browser WA-SQLite creator) from
   `@tanstack/db-sqlite-persistence-core`. **Verify the creator's exact name/signature from the installed
   package — do not guess.** The library itself ships no browser persistence builder (the app owns it),
   so this is mostly a **playground** + a smoke test that the three-step A.3 gate holds over OPFS
   (hydrate-from-storage → no full re-list → catchup deltas persist). This is the one thing node can't prove.
2. **Playground scaffold** (`examples/playground`, not yet created) — exercises OPFS + a reference Bucket-B
   wiring against the new `defineCollection`/`makeLiveRuntime`/`useLiveSync`. Smallest real app that mounts
   a global + a scoped collection and renders via `useLiveQuery`.
3. **A.10 — offline mutations** (`@tanstack/offline-transactions`). Only now that the read path is native
   and solid. This is the write path (optimistic mutations → server confirm via the synced-store path).
4. **A.11 — unmounted-workspace event policy** (default ignore; optional persist-only / lazy-mount hook).
5. **Confirm workspace-switch bootstrap.** Per-collection `listFn` now seeds a scope on first mount, which
   partly addresses the old "deferred" gap. Still verify: mounting a NEW scope while the global cursor is
   `Some` (warm) snapshots that scope correctly rather than waiting for a catchup `Resync`. Flagged in
   DESIGN "Deferred". May need a per-collection "has a base?" check.

## Gotchas that will burn time again
- **Sync mount is real but `@effect/vitest` lies about it.** `createCollection` mount is synchronous in
  prod/node (proven via standalone `tsx`), but a plain `it` doing a `runSync` mount throws
  `AsyncFiberException` once a file has >1 test. Unit-test `defineCollection` against a **fake registry**;
  mount inside `it.live` for integration. See `[[live-collection-test-harness-quirk]]`.
- `no such savepoint: s1` / `SQL logic error` on stderr during tests = node:sqlite test-driver teardown
  noise, **not** a failure.
- The mount path needs `persistence` as a **closed-over value**, never a context dep — keeps
  `getOrCreate`'s `make` at `Effect<A, never, Scope>` → `runSync`-safe in render.

## Constraints & preferences (reinforced this session)
- **Design-first with hard grilling, one decision at a time, recommendation attached, and ASK before doing.**
  The user course-corrects fast and makes decisive simplifying calls (drove React bindings from
  "provider+hook" → "native createCollection"; chose the structural `SyncWrite` index over casts).
- Verify framework APIs from source, never guess (esp. the TanStack 0.6 alpha + `@effect/platform`).
- Report PASS only on an observed exit 0. No `any`, no `throw`, typed errors, `Option` over null,
  object-args, `Context.Tag` not `Effect.Service` — see CLAUDE.md.
- User commits **directly on `main`**, only when asked; no co-author footer.

## Suggested skills
- **`design-first`** for the OPFS persistence seam + the playground/Bucket-B wiring shape (lock the
  interface with the user before coding), and again before A.10 (the write path is a real new seam).
- **`tdd`** for implementation — the 71 existing tests guard the read path; keep red→green.
- **`verify`** / **`run`** once the playground exists, to confirm OPFS hydrate/catchup actually works in a
  browser (the node suite can't).
