export const meta = {
  name: 'write-docs',
  description: 'Author docs/*.md + root README for the live-collection library, one agent per domain, then cross-link',
  phases: [
    { title: 'Write docs', detail: 'one agent per doc — verify every signature against src/ before writing' },
    { title: 'Synthesize', detail: 'cross-link the set, build index, flag contradictions/gaps' },
  ],
}

const PREAMBLE = `
You are writing ONE documentation file for the @triargos/live-collection repo
(/Users/tim/IdeaProjects/effect-live-collection), a frontend-only Effect + TanStack DB live-sync library.

THE SINGLE MOST IMPORTANT RULE — verify every API against src/ before you write it:
- packages/live-collection/DESIGN.md is layered by history; LATER sections SUPERSEDE earlier ones.
  Four passes appended over time:
    1. "# Bucket A — the collection factory & persistence base" (§1–§4) — ORIGINAL shapes.
    2. "# Transport tier (A.6–A.9)" (DEC-T*).
    3. "# Native-collection redesign + React bindings (DEC-R*)" — REVISED §2/§3:
       effectCollectionOptions → liveCollectionOptions; PersistenceBase *tag* → plain \`persistence\`
       *value* on the runtime; MountRef → handle. Citing §2/§3 names verbatim documents a DEAD API.
    4. "# EventLog manager — replay-on-mount (A.12, DEC-E*)" — adds replay path + EventLogStore.
- Read DESIGN.md for rationale, but treat src/ as the source of truth. NEVER transcribe an API from prose.
  git grep the symbol; read the actual export; cite as file.ts:line so readers can click through.

CODEBASE CONVENTIONS — any example you show MUST honor these or it misleads:
- No throw / no new Error across boundaries; failures are Schema.TaggedError (a tagged error IS an Effect).
- Option over null/undefined for modeled absence; decode wire \`T | null\` to Option at the boundary.
- Object args when a function has >1 of its own params (fn({a, b}), not fn(a, b)); leading data-last arg exempt.
- Seams are hand-rolled Context.Tag + \`interface <Name>Shape\` + separate \`make\` + \`<Name>.layer\` — NEVER Effect.Service.
- Branded ids minted only at boundaries (mappers/input handlers), never cast inside the app.
- Validation at boundaries only — decode SSE/catchup payloads against protocol schemas, never cast wire shapes.

FRAMING:
- Frontend-only throughout. Every doc EXCEPT docs/backend.md documents the CLIENT library; the backend is
  the reader's responsibility. Cross-link to docs/backend.md and docs/protocol.md for the wire contract.
- Do NOT document deferred/rejected things as present. Mention them as "not built / why" at most.
  Deferred today: A.11 unmounted-workspace policy; offline-durable writes; throttled watermark flush;
  registry eviction backstop; per-target resync.
- Pin versions where they matter: @tanstack/db pinned 0.6.7 (alpha); persistence adapter 0.1.11; the
  persistence surface shifts — tell readers to "verify against installed version."

TONE: precise, example-led, skimmable. Open with a one-paragraph "what this is / when you touch it,"
then the API, then a worked snippet drawn from examples/playground/. Use project terminology
(seam, tag, layer, replay/skip/bootstrap, scope, watermark, squasher). Markdown, GitHub-flavored.

OUTPUT: Write the file yourself with the Write tool at the absolute path given. Then return a SHORT
summary: the file path, its section headings, every src file:line you cited, and any contradiction,
dead-name, or gap you hit that the synthesis pass should know about. Your return text is data for the
synthesizer, not prose for a human.
`

const REPO = '/Users/tim/IdeaProjects/effect-live-collection'

const DOCS = [
  {
    file: `${REPO}/README.md`,
    label: 'README',
    audience: 'New user',
    scope: `What it is (hero LiveCollection<T>), the 3 packages (protocol → live-collection → react), install (pnpm),
a ~30-line quick start: makeLiveRuntime → defineCollection → useLiveSync + useLiveQuery; link out to each docs/*.
Frontend-only; backend is yours (link docs/backend.md). Keep it the front door — skimmable, links into docs/.`,
    sources: `CLAUDE.md, examples/playground/src/live/playground.ts, packages/react/src/index.ts,
DESIGN.md "# Native-collection redesign" (the north-star surface), packages/live-collection/src/index.ts (public exports),
packages/live-collection/src/runtime/live-runtime.ts (makeLiveRuntime).`,
  },
  {
    file: `${REPO}/docs/architecture.md`,
    label: 'architecture',
    audience: 'Contributor/integrator',
    scope: `The acyclic DAG protocol → live-collection → react; the 3-published-package rationale (why core/persistence/client
are DIRECTORIES not packages — they always travel together; protocol & react earn separation); the TWO EXECUTION SURFACES
(sync mount value vs forked async loop, DEC-R8); seams = Context.Tag + Shape + Layer (decision 6, never Effect.Service);
the conventions (typed errors, Option, validation-at-boundaries, object-args).`,
    sources: `CLAUDE.md Architecture + Decisions sections, DESIGN.md "# Native-collection redesign" (DAG, two surfaces),
packages/live-collection/src/runtime/live-runtime.ts, packages/live-collection/src/index.ts.`,
  },
  {
    file: `${REPO}/docs/collections.md`,
    label: 'collections',
    audience: 'User',
    scope: `defineCollection global vs scoped overloads + scopeOf; the registry-backed handle; CollectionKey {entity, scope}
(structured, NO glob — DEC-9); getOrCreate / getById / dispose / disposeScope / disposeAll*; scoping as the lever for large
data (decision 4); the LiveCollection<T> surface. NOTE: dispose is async (runs cleanup()), getOrCreate is sync.`,
    sources: `packages/live-collection/src/registry/define-collection.ts, registry/collection-key.ts,
registry/collection-registry.ts, DESIGN.md §1 + DEC-R2/R9/R10.`,
  },
  {
    file: `${REPO}/docs/persistence.md`,
    label: 'persistence',
    audience: 'User/integrator',
    scope: `liveCollectionOptions({getKey}) inner creator; \`persistence\` is a closed-over VALUE on the runtime (NOT a tag);
browser OPFS via @tanstack/browser-db-sqlite-persistence (openBrowserWASQLiteOPFSDatabase → createBrowserWASQLitePersistence)
vs node test sqlite; deriveSchemaVersion; the A.3 three-step gate (hydrate-from-storage → no full re-list → catchup deltas
persist via the sync source). persistedCollectionOptions is in @tanstack/db-sqlite-persistence-core, NOT @tanstack/db core
(decision 2). Tell readers to verify the alpha surface against the installed version.`,
    sources: `packages/live-collection/src/persistence/*.ts (live-collection-options.ts, live-collection.ts, schema-version.ts,
sync-session.ts), DESIGN.md §2/§3 as REVISED by DEC-R*, decisions 2/3/7, examples/playground/test/opfs-smoke.browser.test.ts,
memory opfs-persistence-reality.`,
  },
  {
    file: `${REPO}/docs/read-path.md`,
    label: 'read-path',
    audience: 'Integrator',
    scope: `The merged-inbox single-fiber syncLoop (catchup → tail SSE forever); SyncTransport (SSE decode, fails-on-drop →
reconnect, DEC-T4); CatchupClient (/catchup?from=, write via dispatcher); LastSyncIdStore durable cursor (ours, not staleTime,
decision 5; monotonic by compareSyncId); dispatch = applyWrite/applyDelete keyed by scope; resync handling (blunt/global, DEC-T6/E9).`,
    sources: `packages/live-collection/src/client/sync-loop.ts, client/sync-transport.ts, client/catchup-client.ts,
client/last-sync-id-store.ts, DESIGN.md "# Transport tier" (DEC-T*).`,
  },
  {
    file: `${REPO}/docs/replay-on-mount.md`,
    label: 'replay-on-mount',
    audience: 'Integrator',
    scope: `The problem (a scope mounted AFTER events streamed past renders empty); decideOnMount → skip / replay / bootstrap
from syncId positions only; EventLogStore (layer = durable IndexedDB, layerMemory = Ref); prunePlan (per-model + global caps);
floor = prune boundary; base watermarks; the 3 invariants (idempotency / floor-guard / cursor-completeness); DEC-E13a (index on
modelName alone, in-memory sort — IDB orders keys lexicographically but syncIds by magnitude; compound [modelName,scope] drops
scope-less Deletes).`,
    sources: `packages/live-collection/src/client/event-log-store.ts, client/mount-decision.ts, client/prune-plan.ts,
DESIGN.md "# EventLog manager" (DEC-E*), examples/playground/test/event-log-store.browser.test.ts,
examples/playground/test/replay-on-mount.browser.test.ts.`,
  },
  {
    file: `${REPO}/docs/optimistic-writes.md`,
    label: 'optimistic-writes',
    audience: 'User',
    scope: `The native write path (A.10): onInsert/onUpdate/onDelete return EFFECTS with app R; services: ManagedRuntime
discharges R; Model B — the handler calls collection.utils.writeSynced(confirmed) BEFORE resolving (no flicker; the SSE echo is
an idempotent re-write); client-minted ids (DEC-8); failure ⇒ TanStack rollback; SyncWrite<T>. Offline-durable writes are deferred.`,
    sources: `packages/live-collection/src/registry/define-collection.ts (MutationHandlers/ServicesOf),
packages/live-collection/src/dispatch/sync-write.ts, examples/playground/src/live/playground.ts handlers,
memory optimistic-write-path-a10.`,
  },
  {
    file: `${REPO}/docs/react.md`,
    label: 'react',
    audience: 'User',
    scope: `useLiveSync(runtime, syncMap) forks the loop for the app lifetime (mount once near root; interrupts loop on unmount
but does NOT dispose collections — registry lifetime is the app's); useLiveQuery(() => coll); the runtime/provider pattern.`,
    sources: `packages/react/src/index.ts (the entire binding, small), examples/playground/src/routes/App.tsx,
examples/playground/src/live/context.tsx.`,
  },
  {
    file: `${REPO}/docs/protocol.md`,
    label: 'protocol',
    audience: 'Backend + frontend authors',
    scope: `The contract kit: SyncEvent / HydratedSyncEvent<T> (wire data: T | null, decode to Option at boundary); ids
(SyncId canonical-decimal + compareSyncId bigint order; ModelName / ModelId); sync-group grammar (deriveGroup / parseGroup /
matches incl. wildcards); resync sentinels (__all / __group:<id> / __model:<Name>); the SQUASHER (pure §8 fold, property-tested,
both ends rely on it); ModelDescriptor / SyncContext types; the /catchup request/response SCHEMAS (not an HttpApi — backend owns
routes/errors/auth).`,
    sources: `packages/protocol/src/*.ts (ids.ts, sync-event.ts, sync-group.ts, resync.ts, squash.ts, catchup.ts,
model-registry.ts, index.ts), CLAUDE.md protocol block, live-sync-system.md §4–§9.`,
  },
  {
    file: `${REPO}/docs/backend.md`,
    label: 'backend',
    audience: 'Backend implementer',
    scope: `A SPEC OF OBLIGATIONS (NOT code that lives in this repo — this library is frontend-only): sync_events store (append +
query-by-groups + by-syncId); SyncEventBus (pub/sub seam); dispatcher (append row + best-effort publish); permission resolver
groupsFor({userId}); GET /catchup?from=&group= (auth, resolve groups SERVER-side from perms, retention check, query, SQUASH,
hydrateMany, return {events, lastSyncId}); GET /sync SSE (bus subscribe, group filter, hydrate, synthetic-delete on ACL loss,
per-connection scope); resync emission (all 3 variants; membership-removal → per-group resync); retention job (wall-clock;
surfaces to client only as a Resync); the two retention axes (client event-count vs server wall-clock). Use the protocol schemas
as the typed blanks to fill. Reference examples/server if present and shared-backend.ts as a miniature.
Flag MembershipChangedEvent shape as unsettled (live-sync-system.md notes it).`,
    sources: `live-sync-system.md §4–§13, packages/protocol/src/*.ts, TASKS.md Bucket C, CLAUDE.md "Decided" backend framing,
examples/playground/src/live/shared-backend.ts (miniature reference).`,
  },
]

phase('Write docs')
log(`Fanning out ${DOCS.length} doc agents — one per file.`)

const SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['file', 'headings', 'citations', 'notes'],
  properties: {
    file: { type: 'string', description: 'absolute path written' },
    headings: { type: 'array', items: { type: 'string' }, description: 'section headings in the doc' },
    citations: { type: 'array', items: { type: 'string' }, description: 'every src file:line cited' },
    notes: { type: 'string', description: 'contradictions, dead names, gaps for the synthesizer; empty string if none' },
  },
}

const results = (await parallel(DOCS.map((d) => () =>
  agent(
    `${PREAMBLE}

=== YOUR DOC ===
Write: ${d.file}
Audience: ${d.audience}
Scope / must cover:
${d.scope}
Primary sources (READ THESE FIRST, verify every signature against the src/ ones):
${d.sources}

Read your src/ files, then the mapped DESIGN.md section (remember SUPERSEDES), then write the file.
${d.label === 'README' || d.label === 'protocol' || d.label === 'backend'
        ? 'Cross-link the other docs/*.md by relative path where relevant.'
        : 'Cross-link sibling docs (./architecture.md, ./protocol.md, ./backend.md etc.) by relative path where relevant.'}`,
    { label: `doc:${d.label}`, phase: 'Write docs', schema: SCHEMA },
  )
))).filter(Boolean)

phase('Synthesize')
log('Cross-linking the doc set and checking for contradictions.')

const manifest = results
  .map((r) => `### ${r.file}\nHeadings: ${r.headings.join(' · ')}\nCitations: ${r.citations.join(', ')}\nNotes: ${r.notes || '(none)'}`)
  .join('\n\n')

const synthesis = await agent(
  `${PREAMBLE}

=== SYNTHESIS PASS ===
${DOCS.length} doc agents just wrote the files below. Your job:
1. Read each written file under ${REPO}/docs/ and ${REPO}/README.md.
2. Ensure README links to every docs/*.md and reads as a coherent front door.
3. Add ${REPO}/docs/README.md — a one-screen index linking each doc with a one-line description.
4. Cross-link related docs where an agent missed it (read-path ↔ replay-on-mount ↔ protocol ↔ backend;
   collections ↔ persistence ↔ optimistic-writes; react ↔ architecture). Fix broken relative links.
5. Reconcile any contradiction or duplicated/dead API the agents flagged in their notes below. If two docs
   describe the same API differently, verify against src/ and fix the wrong one.
6. Do NOT rewrite whole docs — make surgical edits. Use Edit/Write only where needed.

Agent manifest (their notes are the leads to chase):
${manifest}

Return: the final list of files in docs/ + README.md, the cross-links you added, any contradiction you
resolved, and any UNRESOLVED question or gap (e.g. A.11 final shape, MembershipChangedEvent shape) for the
human. This is a report for a person — be concise and concrete.`,
  { label: 'synthesize', phase: 'Synthesize' },
)

return { docs: results.map((r) => r.file), synthesis }
