# Handoff — next: snapshot-on-mount (DEC-A12), so a freshly-mounted collection recalls current state

_Updated 2026-06-09. The A.10 optimistic write path and the browser playground are **done, committed, and
green** (`e51c3ba` feat(live-collection): optimistic write path (A.10); `6445465` feat(playground):
cross-tab live-sync demo). `pnpm -r typecheck` and the live-collection suite (42 tests) exit 0; the
playground builds and its browser tests pass. This handoff is forward-looking — design + rationale live in
`DESIGN.md`, don't re-derive them._

## Goal
A scoped collection mounted **after** its sync events have already streamed past renders **empty**.
Implement the missing read-path step so mounting a collection loads its current server state (the
"loadFn"/`listFn` snapshot), then keeps tailing deltas. This is already designed as **DEC-A12** in
`packages/live-collection/DESIGN.md` ("snapshot-and-tail, not catchup-from-zero") — **recorded, not yet
implemented.**

## The bug (general library gap, not a demo artifact — reproducible in a single tab)
1. App starts → `useLiveSync` forks the loop → catchup from cursor `"0"` returns events for **all** scopes.
2. Workspace B isn't open, so B's collection isn't mounted. In `sync-loop.ts` `dispatch`,
   `registry.getById(key)` → `None` → **event ignored** (`// not mounted ⇒ ignore`).
3. The cursor still advances (`store.set(lastSyncId)` in `applyCatchup`; `store.set(syncId)` in `route`) —
   those events are now **behind the cursor** and will never be redelivered.
4. User opens B → it mounts → `liveCollectionOptions.sync` is **network-free** (installs the `writeSynced`
   session + `markReady()`, hydrates from OPFS only). OPFS for B is empty (never synced here) → **B is empty.**
5. `listFn` is invoked **only** by the loop's `snapshotInstance`/`snapshotAll`, which fire **only on a
   `Resync`** — never on mount.

Net: a never-bootstrapped (or stale) scope stays empty until a full `Resync`. Cross-tab makes it obvious
but it is not cross-tab-specific.

## Read these first (don't re-derive)
- `packages/live-collection/DESIGN.md` → **DEC-A12** (snapshot-and-tail), DEC-A11 (one-shot session),
  DEC-A10 (`gcTime: Infinity`, registry is sole GC), and the "snapshot every spec (bootstrapFn → upsert +
  delete-absent)" flow near the bottom. **Authoritative.**
- `packages/live-collection/src/client/sync-loop.ts` — `dispatch` (the `onNone ⇒ ignore`), `route` +
  `applyCatchup` (cursor advance), **`snapshotInstance`** (the exact `listFn → writeSynced + delete-absent`
  reconcile to reuse), `snapshotAll` (Resync only).
- `packages/live-collection/src/persistence/live-collection-options.ts` — the network-free `sync`
  (candidate hook), `startSync: true`, the `SyncSession`.
- `packages/live-collection/src/registry/define-collection.ts` — `makeFor` (mount seam); `meta.listFn` is
  already **bridged to `R = never`** via `services`, so it's callable from the mount path with no app dep.
- `packages/live-collection/src/registry/collection-registry.ts` — `getOrCreate` (canonical first-mount).
- Memory: `[[native-collection-redesign]]`, `[[optimistic-write-path-a10]]`, `[[opfs-persistence-reality]]`,
  `[[live-collection-test-harness-quirk]]`.

## What "done" looks like
Open workspace B for the first time → B fetches current rows via `listFn(B)` → upsert + delete-absent →
live deltas keep it fresh. The playground is already wired to prove it: `examples/playground/src/live/
shared-backend.ts` `WebhookApi.list(orgId)` folds the shared log into current rows (a correct snapshot
source). Once the library calls `listFn` on mount, switching to `org-2` in a second tab populates it. That
is the end-to-end acceptance check.

## Decide the seam first (design-first) — weigh these, don't just build option 1
1. **Snapshot-on-mount via `listFn` (DEC-A12 `BootstrapSource`) — the spec'd path.** On first
   `getOrCreate`, run the `snapshotInstance` reconcile. Open questions: hook in `liveCollectionOptions.sync`
   vs. `defineCollection.makeFor` vs. registry `getOrCreate`; background (non-blocking, like `startSync`'s
   hydration) vs. blocking (`preload()`-style); **freshness gate** — snapshot vs. trust OPFS, decided by
   never-bootstrapped vs. `lastSyncId`/cursor staleness; the snapshot↔live-delta race (healed by idempotent
   keyed `writeSynced` + catchup overlap — DESIGN: "(rows, cursor) can skew; healed by catchup overlap +
   cold re-snapshot").
2. **Eager-mount the registry ("mount then").** Mount collections before/at loop start so events aren't
   dropped. Rejected-by-design for scopes (DEC-4: scoping is the lever for large data — can't mount all
   workspaces up front); may be valid for **global** collections. A partial answer, not the general one.
3. **Local replay queue of dropped events.** Buffer events whose scope isn't mounted (keyed by scope),
   replay on mount. Trades a fetch for unbounded client memory + still can't reconstruct cold state once the
   server log is retention-pruned (DESIGN DEC-A12 calls the snapshot endpoint a *correctness* requirement
   for exactly this). Likely a complement, not a replacement.
4. **Don't advance the cursor past unmounted-scope events / per-scope cursors.** Keep events redeliverable
   on reconnect. Re-opens the single-global-cursor decision (DEC-5); probably too invasive — name it so it's
   an explicit rejection with a reason.

Framing: **#1 is the answer**, #2 covers globals, #3/#4 are alternatives to rule out (or fold in) with
reasons. Lock the seam + freshness rule before coding.

## Gotchas that will burn time again
- **Sync mount is real but `@effect/vitest` lies about it.** `createCollection` mount is synchronous in
  prod/node, but a plain `it` doing a `runSync` mount throws `AsyncFiberException` once a file has >1 test.
  Unit-test `defineCollection`-shaped code against a **fake registry**; mount inside `it.live` for
  integration. See `[[live-collection-test-harness-quirk]]`.
- `no such savepoint: s1` / `SQL logic error` on stderr during tests = node:sqlite driver teardown noise,
  **not** a failure (the suite still reports all green).
- The mount path needs `persistence` as a **closed-over value**, never a context dep — keeps
  `getOrCreate`'s `make` at `Effect<A, never, Scope>` → `runSync`-safe in render.
- The playground fakes the backend cross-tab via a shared `localStorage` log + `BroadcastChannel`, per-tab
  OPFS db named from `sessionStorage`. A real service worker could host `/sync` + `/catchup` at higher
  fidelity (MSW-style) — noted as an optional future mode, not required.

## Constraints & preferences (from CLAUDE.md + this user)
- **Design-first with hard grilling, one decision at a time, recommendation attached, ASK before doing.**
  The user course-corrects fast and makes decisive simplifying calls.
- Hand-rolled `Context.Tag` + `<Name>Shape` + separate `make` + `Layer` — **never `Effect.Service`**.
- No `throw`/`new Error` across boundaries (`Schema.TaggedError`; infra failures `Effect.orDie`); decode at
  boundaries only, no `as` on IO; `Option` over null/undefined; object-args for >1 param.
- Tests: `@effect/vitest` (`it.effect`, **`assert` never `expect`**); behavior through the public interface;
  no `vi.mock` — drive seams via `layerMemory`; tests in sibling `test/`, not `src/`.
- Verify framework APIs from source, never guess (TanStack 0.6 alpha + `@effect/platform`). Report PASS only
  on an observed exit 0. Run `pnpm -r typecheck` after every change.
- User commits **directly on `main`**, only when asked; **no co-author footer**.

## Next steps (ordered)
1. **`design-first`** — frame the mount/bootstrap seam, draft the tag/Shape (or the hook into existing
   `liveCollectionOptions`/registry), the freshness rule, and the prod+test call graph. Grill options #1–#4
   above; get explicit sign-off.
2. **`tdd`** — failing behavior test first: mount a collection **after** its events have streamed past,
   assert it converges to current state via `listFn` (in-memory catchup/transport + in-memory `listFn`).
   Then a freshness test (warm OPFS base ⇒ no needless re-list).
3. Implement to green, **reusing `snapshotInstance`**'s reconcile. Keep blast radius to the mount path.
4. Verify end-to-end in `examples/playground` (`pnpm playground dev`): open `org-2` in a second tab → rows
   appear (the live repro). `verify` / `run` skills help here — node can't prove the browser path.

## Suggested skills
`design-first` (lock the seam + freshness rule) → `tdd` (red→green) → `verify`/`run` (browser proof).
Optionally `improve-codebase-architecture` if the bootstrap seam tempts a wider read-path refactor.
