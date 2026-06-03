# CLAUDE.md

Guidance for Claude Code working in this repo.

> **What this repo is.** `@triargos/live-collection-*` — a reusable, **frontend-only** Effect + TanStack DB
> live-sync library (hero type `LiveCollection<T>`). The authoritative spec is [`live-sync-system.md`](live-sync-system.md);
> the task breakdown is [`TASKS.md`](TASKS.md). The backend is per-app and lives elsewhere;
> a reference backend lives (unpublished) in `examples/server`. Read the spec before writing code.

## Mandatory before any code change

1. **Design the interface before implementing** (see below). The design is the deliverable first; implementation is downstream. Use the `design-first` skill for that, it provides additional guidance on how to design interfaces.
2. **Reuse before invent.** Before writing a new helper, type, or component, search the codebase for an existing one. Match the shape of what's already there — don't introduce a second way to do the same thing. If it can be improved however, recommend it with a nice comparison (in "interface" language, before and after so the author has a clear picture of the changes / upsides / downsides)
3. **Never guess framework APIs.** Read the source or docs. Confirm a signature before you call it. (Effect, TanStack DB 0.6, `@effect/platform` — verify, don't assume; the TanStack DB persistence layer is an *alpha* and its surface shifts.)
4. **After every file change, run the typecheck** (`pnpm typecheck`). Non-negotiable. Don't continue with type errors outstanding.

## Mandatory: design the interface before implementing

For any new feature, refactor, or redesign, **the design is the deliverable first.** Follow this loop (the `design-first` skill drives it):

1. **Frame the seam.** Name the module and who calls it. Apply the *deletion test* — if deleting it just moves complexity elsewhere instead of concentrating it, it shouldn't exist.
2. **Draft the interface as concise TypeScript pseudocode — no implementation bodies.** Show:
   - the `Context.Tag` interface (a `<Name>Shape` + tag — **never `Effect.Service`**, see decision 6): method signatures with typed errors in the channel,
   - branded domain types (`Schema.brand` for IDs/slugs/urls; `Option` for modeled absence, never `null`/`undefined` internally),
   - one `Schema.TaggedError` per failure mode, listed in each method's error channel,
   - the **call sites** (`const x = yield* Service`),
   - the **call graph** for production *and* test, top to bottom, showing each seam and the adapter behind it,
   - layer composition: `Service.layer` (prod default), `Service.layerMemory` (in-memory test/dev adapter), `Service.layerFromEnv` (env-built) where needed. **No `Live` suffix.**

   Sketch the shape **twice** when it's non-obvious and keep the deeper one — a small interface hiding much behavior beats a wide interface over thin implementation.
3. **Lock it before coding.** Present the spec, grill it one decision at a time (recommend an answer at each branch), fold in line-anchored feedback, and re-output the full revised spec after substantive changes. **Get explicit sign-off.**
4. **Tests against the locked interface**, then **implement to green** in vertical slices. For a service seam this is a strict red→green order — never write `make`/layer ahead of a failing test:
   1. Write the `Context.Tag` + `<Name>Shape` **only** — the interface, no `make`, no `Layer`.
   2. Write behavior tests that consume the tag (`yield* <Name>`). They compile against the interface but are **red** — there's no layer to provide yet.
   3. Implement `make` + `<Name>.layer` until the tests go **green**.

**Do not write implementation before the interface design is locked and tests exist against it.** Concretely: the `make`/layer for a seam is written *after* its tests are red, never alongside them. When a design turns out wrong, rewind and redesign — don't polish a flawed shape.

## Mandatory: no throw, typed errors only

- **Never `throw`.** Model failures as `Schema.TaggedError`; a tagged error *is* an Effect — return it.
- **Never `new Error(...)` / `Effect.fail(new Error(...))`** for anything that crosses a module boundary. Use typed, tagged errors with structured context.
- Infrastructure failures (network, DB driver, etc.) are defects, not domain errors — `Effect.orDie` them; keep the Effect error channel limited to modeled domain failures.
- Recover with `Effect.catchTag` / `Effect.catchTags` / `Effect.mapError`. **Never `Effect.catchAllCause`** — it swallows defects.

## Mandatory: validation at boundaries only

Validation happens **at boundaries** — HTTP handlers, repository mappers, env/config, any seam where data starts as `unknown`. Inside the app, data already carries branded types.

- **Treat schemas as parsers, not post-hoc validators:** decode external input into stronger domain values at the seam and rely on those types downstream.
- **Never re-validate and never cast inside the app.** Calling `BrandedId.make(raw)` or `raw as BrandedId` outside a mapper/input handler means the data flow is wrong — fix the flow.
- **No `as` casts on IO results** (`response.json()`, `JSON.parse()`, SQL rows, KV/cache values, request bodies). Parse/decode unknown data at the seam. **In this repo specifically:** decode the SSE/`/catchup` payloads against the `@triargos/live-collection-protocol` `HydratedSyncEvent` schema at the client boundary — never cast the wire shape.
- **No `any`.** Use `unknown` and narrow through Schema or explicit guards.

## Mandatory: `Option` over null/undefined

- Use `Option` for modeled internal absence. Convert `null`/`undefined` from external APIs at the seam (`Option.fromNullishOr` / `fromNullOr` / `fromUndefinedOr`), pass `Option` through domain and service interfaces, and convert back only when an external contract requires a nullable/optional field. (Note: `HydratedSyncEvent.data` is `T | null` *on the wire* by contract — decode it to `Option<T>` at the boundary.)

## Mandatory: object args when more than one parameter

- **One parameter → positional. More than one → a single options object** (`fn({ key, make })`, not `fn(key, make)`). Object keys are self-documenting at the call site, can't be transposed, and extend without breaking callers — positional pairs of same-typed args (`(entity, scope)`) are the opposite.
- The leading data argument of a `dual`/data-last combinator is exempt (that's the Effect idiom); this rule is about a function's *own* parameters.
- Applies to new code; the shipped `protocol` package is grandfathered until revisited.

## Mandatory: choose the right test framework

| Code | Framework | Import |
|------|-----------|--------|
| Effect, Stream, Layer, TestClock | `@effect/vitest` | `import { assert, describe, it } from "@effect/vitest"` — `it.effect(...)` |
| Pure TS (Array, String, Number) | regular `vitest` | `import { describe, expect, it } from "vitest"` |

- In Effect tests: **always `assert`, never `expect`** — mixing them breaks the runtime.
- **Tests verify behavior through the public interface, not implementation.** A test that breaks on an internal refactor (with behavior unchanged) was testing the wrong thing.
- **Test our business logic, not the library.** Assert behavior of code *we* wrote — folds (the squasher), predicates (`intersects`, `isUnder`), orderings (`compareSyncId`), custom `Schema.filter`/`pattern` rules, `narrowModelName`, type-level constraints (`@ts-expect-error`), and the threading of a generic `T` through a schema. **Do not test that Effect.Schema does its job:** no tests for plain union discrimination, round-trips, required-field/excess-property handling, or stock combinators (`NonEmptyArray`, `NonEmptyString`, `Schema.Unknown` pass-through). The shape of a struct is a declaration, not a behavior — `assert(!("data" in del))` when the input never had `data` proves nothing. The litmus test: *could this fail if only Effect changed, with our code untouched?* If yes, delete it. Schema declarations get exercised transitively by the business-logic tests that decode them, so coverage isn't lost.
- **No `vi.mock` / `vi.stubGlobal` / `vi.spyOn`.** Design for dependency injection; drive the seam under test through its `layerMemory` adapter.
- **The squasher (in `protocol`) is pure — property-test it hard** (spec §8, §12): random event sequences, assert a client catching up from any `syncId` converges to the same state; assert `syncId` gap-tolerance. It's contract behavior, so its tests live with it in `protocol` and both ends rely on them.

**Test file layout (every package).** Tests live in a sibling `test/` directory, **not** in `src/` — `src/` stays publishable (`tsc -b` emits only source into `dist/`, no `*.test.*`). Tests import the unit under test by relative source path (`../src/foo.js`). Each package keeps a `tsconfig.test.json` (`noEmit`, `composite: false`, includes `src` + `test`) and runs `typecheck` as `tsc -b && tsc -p tsconfig.test.json` so tests are type-gated without polluting the build graph. Property tests use `effect`'s `FastCheck` (`import { FastCheck as fc } from "effect"`).

## Codify decisions as you go

This is a young codebase — conventions are still being set, so write them down the moment they're decided.

- When you establish a reusable shape (a primitive, a service pattern, a naming rule), add it to this file so the next change follows it.
- When the user rejects an approach with a load-bearing reason, record it (an ADR or a note here) so it isn't re-litigated.
- Reuse-before-invent applies to your own prior work in this repo, not just libraries.

## Validation reporting discipline

- Never describe a command's result impressionistically.
- A command is **PASS** only if its exit code was directly observed to be `0`; **FAIL** if non-zero; **UNVERIFIED** if the exit code wasn't confirmed.
- `UNVERIFIED` results must never be called passing, fine, or "looking good." If verification is ambiguous, rerun in a way that produces an attributable exit status.

## Architecture

**Stack:**

- **Effect** — core runtime, services, layers, schema. The whole library is Effect-native.
- **TanStack DB `persistedCollectionOptions`** (SQLite-WASM) — client persistence base. Imported from **`@tanstack/db-sqlite-persistence-core`** (the SQLite persistence adapter), **not** `@tanstack/db` core — core only provides `createCollection`. `@tanstack/db` is **pinned exactly at `0.6.7`** (alpha; resolves spec §22's open version `TODO`). Bump deliberately, not via caret.
- **`@effect/platform`** — HTTP client + SSE stream decode for the read path.
- **React** (optional) — bindings via `@tanstack/react-db` in `@triargos/live-collection-react`; core stays framework-neutral.
- **Tooling:** pnpm workspaces (no Turborepo — orchestrate with `pnpm -r` + `tsc -b` project references) · tsup (ESM/CJS) · `@effect/vitest` · Changesets (independent versioning).
- **Reference backend** (`examples/server`, unpublished): Hono + Effect HTTP API + Postgres, per spec §B.

**Layout** (`@triargos/live-collection-*` scope; flat npm scope → product-prefixed names; see spec §14 + §A for the rationale):

**3 published packages.** `core`/`persistence`/`client` are NOT separate packages — they always
travel together (no consumer wants one without the others), so they're directories inside the main
package. Their seams survive as modules + Effect service tags, not npm boundaries. Two boundaries
earn separation: `protocol` (a *different consumer* — the backend — needs it without frontend deps)
and `react` (a *different dep* non-React apps must avoid).

```
packages/
  protocol/         # @triargos/live-collection-protocol   deps: effect   (NOT @effect/platform — DEC-7)
                    #   The shared CONTRACT KIT — backend implements against it (pure, no I/O):
                    #   - SyncEvent + HydratedSyncEvent<T> schemas w/ encode/decode (both directions)
                    #   - sync-group grammar: deriveGroup/parseGroup/matches (wildcards)
                    #   - resync sentinel codecs (__all / __group:<id> / __model:<Name>)
                    #   - the SQUASHER (pure §8 fold; property-tested here; backend imports it)
                    #   - expected interface TYPES: ModelDescriptor<T,R>, SyncContext, DispatchArgs,
                    #     permission-resolver signature (no implementations)
                    #   - the /catchup CatchupRequest/CatchupResponse SCHEMAS (not an HttpApi):
                    #     the backend owns the route, errors, and auth and wires the schemas in.
                    #     Groups are resolved server-side from user perms (no client narrowing).
  live-collection/  # @triargos/live-collection            deps: effect, @effect/platform, @tanstack/db
                    #   src/registry/    CollectionRegistry, globToRegex, lifecycle helpers
                    #   src/dispatch/    dispatch registry + resolver
                    #   src/persistence/ effectCollectionOptions (TanStack 0.6 persisted)
                    #   src/client/      SSE transport, catchup, lastSyncId store
                    #   src/bootstrap/   orchestrator
                    #   + service tags (SyncTransport, PersistedCollectionFactory, LastSyncIdStore)
                    #     and their default layers; public LiveCollection<T> + createCollection.
  react/            # @triargos/live-collection-react      deps: react, @tanstack/react-db, + main
                    #   useLiveCollection hook, runtime provider.

examples/           # NOT published, NOT packages — workspace apps
  playground/       # the A.3 persistence spike home + reference Bucket-B (per-entity) wiring
  server/           # optional reference backend (generic Bucket-C infra), keeps playground runnable
```

Dependency DAG (acyclic): `protocol → live-collection → react`. Inside `live-collection`, `core`
modules declare the service tags; `persistence`/`client` modules provide their default `Layer`s;
the app composes them at the edge.

> **Decided:** the external backend imports `@triargos/live-collection-protocol` from the company
> registry, so it stays a separate package and is scoped as a **contract kit** — schemas, the pure
> squasher, the sync-group grammar, the expected interface types, and the `/catchup` request/response
> schemas (NOT an `HttpApi` — the backend owns routes/errors/auth) — so implementing a feature's
> backend is mostly filling typed blanks. Implementations (repo, dispatcher, SSE handler, permission
> resolver, hydration bodies) stay in the backend, not here.

### Decisions (load-bearing; do not re-litigate without a new reason)

1. **Library is frontend-only; the backend is per-app.** Generic backend infra (bus, repo, dispatcher, squasher, `/sync`, `/catchup`) incubates in `examples/server` and may later graduate to a published `@triargos/live-collection-server` — but it does not gate the frontend library. (User directive + spec framing.)
2. **Persistence = TanStack DB 0.6 `persistedCollectionOptions` (SQLite-WASM), accepting alpha status.** NOT the old custom Dexie bridge (whole-table load + per-tick rescan), NOT a fresh bespoke Dexie engine. (Spec §22.) **`persistedCollectionOptions` lives in `@tanstack/db-sqlite-persistence-core`, not `@tanstack/db` core** — core only exports `createCollection`.
3. **The factory is the only seam.** A wrong persistence choice is contained to the inside of `create<Entity>Collection`; registry, dispatch, resolver, and bootstrap are unchanged. Blast radius = one function per entity. (Spec §22, §A.2.)
4. **Scoping is the lever for large data — not the persistence backend.** A collection's working set is in memory under *either* backend. Per-workspace collections (`<entity>:<orgId>`) + windowed queries are how large data stays small. **Build the registry/scoping early.** (Spec §22.)
5. **Freshness metadata is ours.** A durable, global `lastSyncId` gates catchup — *not* the framework's `staleTime` (which resets on reload). Catchup writes go through the **synced-store write path**, never the optimistic-mutation path. (Spec §22.)
6. **`core` exposes Effect service interfaces (tags); implementations are layers.** This is the Effect-idiomatic expression of the spec's "the X is the seam" properties and keeps the DAG acyclic. **Build seams as a hand-rolled `Context.Tag` + a separate impl + `Layer`, NOT `Effect.Service`** (the latter fuses tag/impl/default-layer and is being removed in Effect v4). Keep interface and implementation separate, in this exact shape:
   - the **interface** is an `interface <Name>Shape` (the contract — `Shape`, never `Impl`, because it's the interface not an implementation);
   - the **tag** is `class <Name> extends Context.Tag("<Name>")<<Name>, <Name>Shape>() {}` — the seam, `yield* <Name>`;
   - the **impl** is a separate `const make: Effect<<Name>Shape, …>` (or a function returning one) — the adapter body;
   - the **layer(s)** hang off the tag: `static readonly layer = Layer.scoped/effect(<Name>, make)` (prod default), plus `layerMemory` / `layerFromEnv` where a second adapter is real. **No `Live` suffix.**
7. **The A.3 persistence spike is a hard gate.** Do not roll persistence out across entities until the three-step flow (hydrate-from-storage → no full re-list → catchup deltas persist via the sync source) is verified against the alpha. (Spec §A.1.)
8. **Echo suppression / `clientId` is removed for now** (protocol DEC-11). The spec's §10 server-side `clientId` filter is in tension with TanStack DB's optimistic-mutation reconciliation (which expects the synced store to confirm your own writes) and withholds the server-transformed value from the originator. If an id is ever reintroduced, it belongs as a **client-side reconciliation key** (Replicache `lastMutationID` style), not a server filter — and only if testing proves it necessary. Until then, no `clientId` on events, `SyncContext`, or the HTTP contract; no `DomainEvent` change required.
9. **Collection identity is a *structured* `CollectionKey {entity, scope: Option<string>}` — NOT the spec §14 string-id + `globToRegex` glob.** The registry never parses an id (no separator, escaping, or `*` semantics); disposal matches on the structured fields (`disposeScope` = scope equality). This is the same structure-over-sentinels choice the protocol made for resync targets (DEC-9). One instance per `(entity, scope)`; variants-within-a-scope are deferred (would be an additive `variant: Option<string>` dimension that `disposeScope` ignores — never folded into `scope`, which would break workspace teardown).
10. **`defineCollection` is the typed skin; `MountRef` is the handle.** `defineCollection` has **two overloads** — global (`() => MountRef`, no `scopeOf`) and scoped (`(args) => MountRef`, `scopeOf` present). The split is load-bearing: a single `scopeOf?` signature infers `Args = unknown` and forces a phantom arg on global calls. `scopeOf: (args) => string` is the **only** place an app's "workspace" notion appears — the library stays scope-generic (the locked answer to "should workspace leak into the generic package?": no). `MountRef extends Effectable.Class` (yieldable: `yield* webhookCollection(orgId)` mounts via the registry), surface is bare — `.key` + `commit()` only; disposal/lookup go through the registry, the sole orchestrator. `R` discharges `Scope` via `Exclude<R, Scope>`, mirroring `getOrCreate`.

### Anti-references — do NOT replicate (spec §B)

- Whole-table `toArray()` + snapshot-diff per liveQuery tick (`services/sync-service.ts` in the old repo).
- `Deferred` + `refreshTrigger` + ack-timeout write handshake (`services/persistence-service.ts`).
- Heavy per-collection bookkeeping propping up the above (`services/collection-state.ts`).

## Commands

> The workspace is not scaffolded yet — these are the target commands and light up after the
> initial pnpm-workspace scaffold lands.

```bash
pnpm -r typecheck                    # tsc -b project references — run after every change
pnpm -r test                         # @effect/vitest across packages
pnpm -r build                        # tsup (ESM/CJS) across packages
pnpm --filter server db:migrate      # reference backend only (examples/server); the library has no DB
pnpm changeset                       # record a version bump before publishing
```
