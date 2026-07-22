# AGENTS.md

Guidance for agents working in this repository.

> `@triargos/live-collection-*` is a reusable, frontend-only Effect + TanStack DB
> live-sync library. Its hero type is `LiveCollection<T>`. Read [`docs/`](docs/)
> (start with `docs/architecture.md` and `docs/protocol.md`) before changing
> architecture or protocol behavior. The unpublished pi-demo contains the reference backend.

## Working rules

- Design and lock interfaces before implementing non-trivial features or refactors.
- Reuse existing seams and adapters before introducing a second pattern.
- Verify framework APIs against the installed source; Effect v4 and TanStack DB are moving targets.
- Keep typed errors in Effect error channels. Infrastructure failures may be defects at explicit boundaries.
- Decode unknown wire/storage input at boundaries; never cast HTTP JSON or persisted rows into domain types.
- Report validation as **PASS** only after directly observing exit code 0, **FAIL** after a non-zero exit, and
  **UNVERIFIED** when no attributable exit code was observed.

## Architecture

### Stack

- **Effect v4** — runtime, services, layers, schemas, streams, and HTTP.
- **TanStack DB `persistedCollectionOptions`** from
  `@tanstack/db-sqlite-persistence-core` — client persistence. `@tanstack/db` is pinned exactly at
  `0.6.7`; bump deliberately because the persistence integration is alpha.
- **`effect/unstable/http`** — HTTP client/response APIs. Keep these unstable imports confined to
  `packages/live-collection/src/client/sync-transport.ts`,
  `packages/live-collection/src/client/catchup-client.ts`, and application-edge wiring.
- **React** is optional and lives in `@triargos/live-collection-react`; core stays framework-neutral.
- **Tooling:** pnpm workspaces, TypeScript project references, Vitest/`@effect/vitest`, and Changesets.

### Packages and dependency DAG

The npm DAG is acyclic: `protocol → live-collection → react`, plus `protocol → server`
(the server kernel never depends on the frontend package).

```text
packages/
  protocol/         @triargos/live-collection-protocol
                    Shared contract kit: wire schemas, sync-group routing keys, resync targets,
                    pure squasher, model-registry types, and catchup schemas. No I/O.
  server/           @triargos/live-collection-server
                    Optional backend kernel: SyncEventStore port, event bus, dispatcher
                    (persist-then-publish), and SyncFeed (catchup + SSE frames) enforcing the
                    backend contract's invariants. effect + protocol only; no HTTP/storage/auth.
  live-collection/  @triargos/live-collection
                    Registry/scoping, persistence factory, catchup/SSE adapters, broker,
                    and runtime. Public hero: LiveCollection<T>.
  react/            @triargos/live-collection-react
                    Optional React lifecycle bindings; reads use TanStack useLiveQuery directly.

examples/
  pi-demo/          Shared HttpApi contract, reference Effect backend (consumes
                    @triargos/live-collection-server), and React web app.
```

`core`/`persistence`/`client` remain modules inside the main package because consumers need them
as one unit. `protocol` is separate because backend consumers need it without frontend dependencies;
`react` is separate because non-React consumers must avoid React dependencies.

## Load-bearing decisions

1. **Frontend library, per-app backend.** Generic backend infrastructure incubates in pi-demo and does
   not gate the frontend package.
2. **Persistence is TanStack DB 0.6 SQLite-WASM.** Do not restore the old Dexie whole-table bridge or
   invent another persistence engine.
3. **The persistence factory is the containment seam.** Registry, drain, broker, and bootstrap must
   not depend on persistence internals.
4. **Scoping controls working-set size.** Use per-scope collections and windowed queries; changing the
   persistence backend does not make in-memory collections unboundedly cheap.
5. **Freshness metadata is ours.** The durable global `lastSyncId`, not framework `staleTime`, gates
   catchup. Catchup writes use the synced-store path, never optimistic mutation hooks.
6. **Services use a separate shape, constructor, and layers.** Keep this exact v4 form:

   ```ts
   export interface ServiceNameShape {
     readonly operation: (input: Input) => Effect.Effect<Output, DomainError>
   }

   const make: Effect.Effect<ServiceNameShape, never, Dependency> = Effect.gen(function* () {
     // adapter implementation
   })

   export class ServiceName extends Context.Service<ServiceName, ServiceNameShape>()("ServiceName") {
     static readonly layer = Layer.effect(ServiceName, make)
     static readonly layerMemory = Layer.succeed(ServiceName, memoryImplementation)
   }
   ```

   Use `<Name>Shape`, not `Impl`. Keep `make` separate. Layers are statics named `layer`,
   `layerMemory`, or `layerFromEnv`. **Do not use a `Live` suffix**—this is an intentional repository
   convention even where broader Effect conventions differ.
7. **The A.3 persistence spike remains a hard gate.** Preserve hydrate-from-storage, no full relist, and
   durable catchup delta behavior when changing persistence code.
8. **No server-side echo suppression/clientId.** Originating clients receive their own transformed
   server value through normal sync. Reintroduce a client reconciliation key only if tests prove it needed.
9. **Collection identity is structured:** `CollectionKey { entity, scope: Option<string> }`. Never parse
   glob-like string IDs; `disposeScope` compares the scope field.
10. **`defineCollection` is the typed skin over native TanStack collections.** It has global and scoped
    overloads, mounts synchronously through the registry, and starts one collection drain in the registry
    child scope. The registry is a lifetime table, not a router.
11. **Client sync is broker-based.** One `SyncBroker` owns catchup, SSE, cursor/log, pruning, and PubSub.
    Collections subscribe and sequentially apply `Snapshot | Upsert | Delete`. Replay and live tail are
    one stream; all models are logged even while unmounted.

## Protocol and boundary rules

- Decode SSE and `/catchup` payloads with schemas exported by
  `@triargos/live-collection-protocol`.
- `narrowModelName` is pure and returns `Result.Result<N, UnknownModelError>`; Effect v4 has no `Either`.
- Wire event data presence is structural: Insert/Update carry `data`; Delete does not.
- ISO dates on the wire use `Schema.DateFromString`, not v4's Date-instance-only `Schema.Date`.
- The `SyncJournal` is the explicit `Option ⇄ null` seam for at-rest rows. Keep `Option` inside service
  and domain APIs.
- Provider/network work stays outside authoritative persistence transactions.

## Tests

- Tests live in sibling `test/` directories, never under publishable `src/`.
- Published packages use `tsconfig.test.json` with `noEmit` and run both build-graph and test typechecks.
- Effect tests import `assert`, `describe`, and `it` from `@effect/vitest`; use `it.effect` or `it.live`.
  Use `assert`, not `expect`, in Effect tests.
- Pure tests may use regular Vitest.
- Property tests import FastCheck from `effect/testing/FastCheck`:

  ```ts
  import * as fc from "effect/testing/FastCheck"
  ```

- Test behavior through public service seams and real `layerMemory` adapters; do not use `vi.mock`,
  `vi.stubGlobal`, or `vi.spyOn` when dependency injection can drive the behavior.
- The protocol squasher is pure and must remain property-tested for convergence from arbitrary cursors
  and gap-tolerant sync IDs.
- Run package scripts. A bare Vitest invocation can pick up the wrong root configuration.

## Effect v4 notes

- Workspace Effect packages are intentionally pinned with `^4.0.0-beta.98`. The caret may float to a
  newer beta or final v4, so treat lockfile updates as deliberate compatibility events and typecheck all
  packages together.
- The workspace cannot mix Effect v3 and v4 in one type graph.
- `@effect/platform` is not a dependency. HTTP client APIs come from `effect/unstable/http`, HttpApi APIs
  from `effect/unstable/httpapi`, and Node integrations from matching `@effect/platform-node` v4 versions.
- Unstable HTTP/HttpApi imports may break between releases; containment at existing adapters and app edges
  is the chosen mitigation, not another wrapper abstraction.

## Anti-references

Do not recreate these old-repository patterns:

- Whole-table `toArray()` plus snapshot-diff on every live-query tick.
- `Deferred` plus refresh-trigger plus ack-timeout write handshakes.
- Heavy per-collection bookkeeping used to support those patterns.

## Commands

```bash
pnpm -r typecheck
pnpm -r test
pnpm -r build
pnpm changeset
```

Run typecheck and the affected package tests after each coherent work package; finish with the root
`pnpm -r typecheck` and `pnpm -r test` gates.
