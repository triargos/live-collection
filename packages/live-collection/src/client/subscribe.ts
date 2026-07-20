import { Effect, Option, PubSub, Stream } from "effect"
import type { ModelName } from "@triargos/live-collection-protocol"
import type { SchemaVersion } from "../persistence/schema-version.js"
import { type CollectionKey, globalKey, scopedKey } from "../registry/collection-key.js"
import type { SyncJournalShape } from "./sync-journal.js"
import type { LastSyncIdStoreShape } from "./last-sync-id-store.js"
import type { PublishedItem } from "./ingest.js"
import type { LastAppliedTracker } from "./last-applied-tracker.js"
import { MountDecision, SyncSignal, concernsModel, dropStale, maxSyncId, planMount, signalFromRow } from "./mount-plan.js"

/** Scoped collections key their persisted rows (and last-applied marks) per scope value. */
export const keyFor = (modelName: ModelName, scope: Option.Option<string>): CollectionKey<unknown> =>
  Option.match(scope, {
    onNone: () => globalKey(modelName),
    onSome: (value) => scopedKey({ entity: modelName, scope: value }),
  })

/**
 * The SERVE machine — assembles one mount stream per subscriber: the replay slice the
 * collection is missing, then the live tail, as one seamless stream.
 *
 * Ordering invariant owned here: the PubSub subscription is established BEFORE any
 * journal/meta read, so no event can fall between "read the journal" and "start tailing" —
 * the overlap is deduped by the tail guard instead.
 */
export const makeSubscribe =
  (deps: {
    readonly journal: SyncJournalShape
    readonly cursorStore: LastSyncIdStoreShape
    readonly published: PubSub.PubSub<PublishedItem>
    readonly current: LastAppliedTracker["current"]
  }) =>
  (args: {
    readonly modelName: ModelName
    readonly scope: Option.Option<string>
    readonly schemaVersion: SchemaVersion
  }): Stream.Stream<SyncSignal> =>
    Stream.unwrap(
      Effect.gen(function* () {
        const { modelName, scope, schemaVersion } = args
        const queue = yield* PubSub.subscribe(deps.published)
        const key = keyFor(modelName, scope)
        const plan = planMount({
          collectionLastApplied: yield* deps.current({ key, schemaVersion }),
          cursor: yield* deps.cursorStore.get,
          maxDeletedSyncId: yield* deps.journal.floor(modelName),
          lastResyncAt: yield* deps.journal.getLastResync,
        })
        const rows = yield* deps.journal.read({ modelName, since: plan.since })
        const tailGuard = rows.reduce((guard, row) => maxSyncId(guard, row.syncId), plan.tailGuardSeed)
        const replay = [
          ...MountDecision.$match(plan.decision, {
            Skip: () => [],
            Replay: () => [],
            Snapshot: ({ at }) => [SyncSignal.Snapshot({ at })],
          }),
          ...rows.map(signalFromRow),
        ]
        const tail = Stream.fromSubscription(queue).pipe(
          Stream.filter(concernsModel(modelName)),
          Stream.mapAccum(() => tailGuard, dropStale),
        )
        return Stream.concat(Stream.fromIterable(replay), tail)
      }),
    )
