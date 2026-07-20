/**
 * `@triargos/live-collection` — local-first live collections for Effect + TanStack DB.
 *
 * Collections are **native** TanStack collections, persisted locally (SQLite) and kept
 * in sync with your backend over a live event stream plus catchup. Getting started:
 *
 * 1. Build the app-wide runtime once at startup: `makeLiveRuntime`.
 * 2. Declare one `defineCollection` per synced model — it returns the collection handle.
 * 3. Start broker ingest near your app root with `useLiveSync(runtime)`
 *    (from `@triargos/live-collection-react`), or `runtime.forkSync()` outside React.
 * 4. Read with `useLiveQuery`, write optimistically with `collection.insert/update/delete`.
 *
 * The hero type is `LiveCollection<T>` — what a collection handle returns. The wire
 * contract shared with your backend lives in `@triargos/live-collection-protocol`.
 */
// core/ — shared identity primitives: structured collection keys and the derived schema version.
export * from "./core/collection-key.js"
export * from "./core/schema-version.js"

// persistence/ — LiveCollection<T> (the hero type), the synced-store write path contract, and
// the building blocks for assembling a persisted collection by hand (defineCollection uses them).
export type { LiveCollection } from "./persistence/live-collection.js"
export * from "./persistence/live-collection-options.js"
export * from "./persistence/sync-write.js"

// client/ — SSE transport, catchup, durable cursor/journal, and the subscription broker.
// (ingest, subscribe, mount-plan, prune-plan are internal machines/policies.)
export * from "./client/sync-cursor.js"
export * from "./client/catchup-client.js"
export * from "./client/sync-transport.js"
export * from "./client/sync-journal.js"
export * from "./client/sync-broker.js"

// registry/ — the collection lifetime table.
export * from "./registry/collection-registry.js"

// runtime/ — the live runtime that owns infra and forks broker ingest.
export * from "./runtime/live-runtime.js"

// Top-level assembly — defineCollection wires persistence + drain + registry + runtime
// into one collection handle.
export * from "./define-collection.js"
