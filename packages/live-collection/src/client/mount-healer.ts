import { Effect, Option } from "effect"
import { compareSyncId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import type { CollectionRegistryShape } from "../registry/collection-registry.js"
import type { CollectionKey } from "../registry/collection-key.js"
import type { ModelMeta } from "../registry/define-collection.js"
import type { SyncWrite } from "../dispatch/sync-write.js"
import type { EventLogStoreShape, LoggedEvent } from "./event-log-store.js"
import type { LastSyncIdStoreShape } from "./last-sync-id-store.js"
import { decideOnMount, MountDecision } from "./mount-decision.js"

/** Minimal view of a mounted collection the loop and the healer write through. */
export type Writable = { readonly utils: SyncWrite<unknown> }

/**
 * What the healer needs from the loop: the wiring (`map`), the seams it reads freshness from
 * (`registry`/`store`/`log`), and the two *application* arms it dispatches to — replay and snapshot
 * stay the loop's source-agnostic dispatch; the healer owns only the policy of when to use which.
 */
export interface MountHealerDeps {
  /** The loop's routing index: wire model name (=== the entity name) → its meta. */
  readonly models: ReadonlyMap<string, ModelMeta<any>>
  readonly registry: CollectionRegistryShape
  readonly store: LastSyncIdStoreShape
  readonly log: EventLogStoreShape
  /** Apply one logged row through the loop's dispatcher (undecodable rows are skipped there). */
  readonly replayRow: (meta: ModelMeta<any>, row: LoggedEvent) => Effect.Effect<void>
  /** Replace one mounted instance with current server truth (`listFn → replaceSynced`). */
  readonly snapshotInstance: (
    meta: ModelMeta<any>,
    scope: Option.Option<string>,
    collection: Writable,
  ) => Effect.Effect<void>
}

/**
 * The mount-healer: everything that decides and records how complete a collection's base is.
 * Loop-internal — its only caller is `syncLoop`; it is a module, not a tag.
 *
 * It owns the **watermark policy** end to end: the per-mount decision (`heal` →
 * skip/replay/bootstrap), the idempotent every-cycle pass (`healAllMounted`), and the
 * post-catchup completeness stamps (`onCatchupApplied`). The loop keeps application
 * (apply/ingest/snapshot) and routing; nothing else writes watermarks.
 */
export interface MountHealer {
  /** Heal one collection from its freshness metadata: Skip / Replay / Bootstrap. Idempotent. */
  readonly heal: (key: CollectionKey<unknown>) => Effect.Effect<void>
  /**
   * Heal every currently-mounted instance (idempotent — complete instances Skip). The registry also
   * queues a Mount signal per first mount, but a signal consumed by a cycle that then died with the
   * connection is gone for good — this pass makes healing a property of every cycle, not of queue
   * delivery, so a collection mounted during a disconnect still converges on the next catchup.
   */
  readonly healAllMounted: Effect.Effect<void>
  /**
   * Record completeness after a catchup was applied. `resync: true` (the catchup carried a `Resync`
   * arm, every mounted instance was just snapshotted) stamps **all** mounted instances complete to
   * `at`. A delta catchup stamps only the instances that **rode it from a complete base**: watermark
   * already `>= from`, or no watermark when `from = "0"` (cursor-completeness ⇒ the full visible
   * state was delivered). An instance mounted mid-flight with a gap below `from` keeps its watermark
   * and heals in its own `heal` — stamping it here would silently skip that heal: a scope deep-linked
   * on a warm-cursor start would render only the delta window, durably.
   */
  readonly onCatchupApplied: (args: {
    readonly from: SyncId
    readonly at: SyncId
    readonly resync: boolean
  }) => Effect.Effect<void>
}

export const makeMountHealer = (deps: MountHealerDeps): MountHealer => {
  const { models, registry, store, log, replayRow, snapshotInstance } = deps

  const forEachMounted = (
    body: (key: CollectionKey<unknown>) => Effect.Effect<void>,
  ): Effect.Effect<void> =>
    Effect.forEach(
      [...models.values()],
      (meta) =>
        registry
          .getByEntity(meta.entity)
          .pipe(Effect.flatMap((mounted) => Effect.forEach(mounted, ({ key }) => body(key), { discard: true }))),
      { discard: true },
    )

  const heal = (key: CollectionKey<unknown>): Effect.Effect<void> =>
    Effect.gen(function* () {
      const meta = models.get(key.entity)
      if (meta === undefined) return
      const modelName = ModelName.make(key.entity) // the wire model name IS the entity name

      const baseWatermark = yield* log.getBaseWatermark(key)
      const cursor = yield* store.get
      const modelFloor = yield* log.floor(modelName)
      const lastResyncAt = yield* log.getLastResync
      const at = Option.getOrElse(cursor, () => SyncId.make("0"))

      switch (decideOnMount({ baseWatermark, cursor, modelFloor, lastResyncAt })) {
        case MountDecision.Skip:
          return
        case MountDecision.Replay: {
          const since = Option.getOrElse(baseWatermark, () => SyncId.make("0"))
          const rows = yield* log.read({ modelName, scope: key.scope, since })
          yield* Effect.forEach(rows, (row) => replayRow(meta, row), { discard: true })
          yield* log.setBaseWatermark({ key, at })
          return
        }
        case MountDecision.Bootstrap: {
          const found = yield* registry.getById(key as CollectionKey<Writable>)
          yield* Option.match(found, {
            onNone: () => Effect.void,
            onSome: (collection) => snapshotInstance(meta, key.scope, collection),
          })
          yield* log.setBaseWatermark({ key, at })
          return
        }
      }
    })

  const onCatchupApplied = (args: {
    readonly from: SyncId
    readonly at: SyncId
    readonly resync: boolean
  }): Effect.Effect<void> =>
    args.resync
      ? forEachMounted((key) => log.setBaseWatermark({ key, at: args.at }))
      : forEachMounted((key) =>
          log.getBaseWatermark(key).pipe(
            Effect.flatMap((watermark) => {
              const rode = Option.match(watermark, {
                onNone: () => compareSyncId(args.from, SyncId.make("0")) === 0,
                onSome: (base) => compareSyncId(base, args.from) >= 0,
              })
              return rode ? log.setBaseWatermark({ key, at: args.at }) : Effect.void
            }),
          ),
        )

  return { heal, healAllMounted: forEachMounted(heal), onCatchupApplied }
}
