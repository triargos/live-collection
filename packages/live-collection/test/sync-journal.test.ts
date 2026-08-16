import { Effect, Option } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { Epoch, ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import { SchemaVersion } from "../src/core/schema-version.js"
import { scopedKey, subsetKey } from "../src/core/collection-key.js"
import { JournalWrite, makeMemoryStore } from "../src/client/journal-store.js"
import { makeSyncJournal, SyncJournal, type JournalEvent } from "../src/client/sync-journal.js"

const sid = (s: string) => SyncId.make(s)
const insert = (syncId: string, scope: string, id: string): JournalEvent => ({
  syncId: sid(syncId),
  modelName: ModelName.make("Webhook"),
  tag: "Insert",
  modelId: ModelId.make(id),
  data: Option.some({ id, scope }),
})
const insertFor = (model: string, syncId: string, id: string): JournalEvent => ({
  syncId: sid(syncId),
  modelName: ModelName.make(model),
  tag: "Insert",
  modelId: ModelId.make(id),
  data: Option.some({ id }),
})
const del = (syncId: string, id: string): JournalEvent => ({
  syncId: sid(syncId),
  modelName: ModelName.make("Webhook"),
  tag: "Delete",
  modelId: ModelId.make(id),
  data: Option.none(),
})

describe("SyncJournal (memory)", () => {
  it.effect("append dedupes by syncId — a re-delivered event upserts instead of duplicating", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      yield* journal.append([insert("1", "org-1", "w1")])
      yield* journal.append([insert("1", "org-1", "w1-v2")]) // same syncId, re-delivered on reconnect overlap
      const rows = yield* journal.read({ modelName: ModelName.make("Webhook"), since: sid("0") })
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0]!.modelId, ModelId.make("w1-v2")) // the later append wins
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("read returns every event for the model, syncId-ordered, after `since`", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      yield* journal.append([insert("2", "org-1", "a"), insert("3", "org-2", "b"), del("4", "a"), insert("1", "org-1", "z")])
      const rows = yield* journal.read({ modelName: ModelName.make("Webhook"), since: sid("1") })
      assert.deepStrictEqual(
        rows.map((r) => r.syncId),
        [sid("2"), sid("3"), sid("4")],
      )
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("epoch round-trips: None until set, then the stored value", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      assert.deepStrictEqual(yield* journal.getEpoch, Option.none())
      yield* journal.setEpoch(Epoch.make("a1c9"))
      assert.deepStrictEqual(yield* journal.getEpoch, Option.some(Epoch.make("a1c9")))
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("last-ingested syncId starts None, then returns what was set", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      assert.isTrue(Option.isNone(yield* journal.getLastIngestedSyncId))
      yield* journal.setLastIngestedSyncId(sid("5"))
      assert.deepStrictEqual(yield* journal.getLastIngestedSyncId, Option.some(sid("5")))
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  // The mark must never regress: a late-arriving older event can't pull it back. And the compare is
  // numeric, not lexical — "12" beats "3" even though it sorts before it as a string.
  it.effect("setLastIngestedSyncId is monotonic by numeric magnitude — a smaller id does not regress the mark", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      yield* journal.setLastIngestedSyncId(sid("10"))
      yield* journal.setLastIngestedSyncId(sid("3"))
      assert.deepStrictEqual(yield* journal.getLastIngestedSyncId, Option.some(sid("10")))
      yield* journal.setLastIngestedSyncId(sid("12"))
      assert.deepStrictEqual(yield* journal.getLastIngestedSyncId, Option.some(sid("12")))
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("resetToEpoch wipes the entire journal and installs the new epoch + last-ingested syncId atomically", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      const version = SchemaVersion.make(1)
      yield* journal.append(["1", "2", "3"].map((s, i) => insert(s, "org-1", `w${i}`)))
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("0") })
      yield* journal.prune({ maxEventsPerModel: 2, maxEventsTotal: 100 }) // establishes a prune boundary at 1
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("3") })
      yield* journal.setLastResync(sid("2"))
      yield* journal.setEpoch(Epoch.make("old"))
      yield* journal.setLastIngestedSyncId(sid("500"))

      yield* journal.resetToEpoch({ epoch: Epoch.make("new"), at: sid("4") })

      assert.deepStrictEqual(yield* journal.read({ modelName: ModelName.make("Webhook"), since: sid("0") }), [])
      assert.deepStrictEqual(yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }), Option.none())
      assert.deepStrictEqual(yield* journal.highestPrunedSyncId(ModelName.make("Webhook")), Option.none())
      assert.deepStrictEqual(yield* journal.getLastResync, Option.none())
      assert.deepStrictEqual(yield* journal.getEpoch, Option.some(Epoch.make("new")))
      // Not a monotonic set — the new-epoch position is *smaller* than the wiped one and must win.
      assert.deepStrictEqual(yield* journal.getLastIngestedSyncId, Option.some(sid("4")))
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("a last-applied write under a new schema version supersedes the old record", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      const oldVersion = SchemaVersion.make(1)
      const newVersion = SchemaVersion.make(2)
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: oldVersion, at: sid("10") })
      // The schema changed: the saved table was dumped, the new-version write replaces outright —
      // even with a numerically smaller syncId (monotonicity holds only within a version).
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: newVersion, at: sid("4") })
      assert.deepStrictEqual(
        yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: newVersion }),
        Option.some(sid("4")),
      )
      // One record per collection key: the old version's mark is gone, not merely shadowed.
      assert.deepStrictEqual(
        yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: oldVersion }),
        Option.none(),
      )
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("last-applied stays monotonic within a schema version", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      const version = SchemaVersion.make(1)
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("5") })
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("3") }) // older ⇒ ignored
      assert.deepStrictEqual(
        yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: version }),
        Option.some(sid("5")),
      )
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("prune deletes rows every collection has applied — without moving the prune boundary", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      const version = SchemaVersion.make(1)
      // Distinct entities so stage 1 (squash) keeps them all — this isolates stage 2.
      yield* journal.append(["1", "2", "3"].map((s) => insert(s, "org-1", `w${s}`)))
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("2") })
      yield* journal.prune({ maxEventsPerModel: 100, maxEventsTotal: 100 })
      const rows = yield* journal.read({ modelName: ModelName.make("Webhook"), since: sid("0") })
      assert.deepStrictEqual(rows.map((r) => r.syncId), [sid("3")]) // 1 and 2 are ≤ min ⇒ dead weight
      assert.deepStrictEqual(yield* journal.highestPrunedSyncId(ModelName.make("Webhook")), Option.none()) // floor-neutral
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("prune drops all rows of a model with no last-applied record, prune boundary untouched", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const key = scopedKey<unknown>({ entity: "Settings", scope: "org-1" })
      const version = SchemaVersion.make(1)
      yield* journal.append([insert("1", "org-1", "w1"), insert("2", "org-1", "w2"), insertFor("Settings", "3", "s1")])
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("0") }) // Settings only
      yield* journal.prune({ maxEventsPerModel: 100, maxEventsTotal: 100 })
      // Webhook has no record ⇒ any mount decides Snapshot regardless ⇒ its rows are dead weight.
      assert.deepStrictEqual(yield* journal.read({ modelName: ModelName.make("Webhook"), since: sid("0") }), [])
      const settings = yield* journal.read({ modelName: ModelName.make("Settings"), since: sid("0") })
      assert.deepStrictEqual(settings.map((r) => r.syncId), [sid("3")])
      assert.deepStrictEqual(yield* journal.highestPrunedSyncId(ModelName.make("Webhook")), Option.none())
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("subset marks round-trip and filter by entity, scope, and schema version", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const version = SchemaVersion.make(1)
      const t1 = subsetKey<unknown>({
        entity: "Value",
        scope: Option.none(),
        subset: { indexKey: "templateId", keyValue: "t1" },
      })
      const t2 = subsetKey<unknown>({
        entity: "Value",
        scope: Option.none(),
        subset: { indexKey: "templateId", keyValue: "t2" },
      })
      yield* journal.setCollectionLastAppliedSyncId({ key: t1, schemaVersion: version, at: sid("5") })
      yield* journal.setCollectionLastAppliedSyncId({ key: t2, schemaVersion: version, at: sid("7") })
      // Non-subset mark for the same model — must NOT show up as a subset.
      yield* journal.setCollectionLastAppliedSyncId({
        key: scopedKey({ entity: "Value", scope: "org-1" }),
        schemaVersion: version,
        at: sid("9"),
      })
      // Same subset under another scope — excluded by the scope filter.
      yield* journal.setCollectionLastAppliedSyncId({
        key: subsetKey({ entity: "Value", scope: Option.some("org-1"), subset: { indexKey: "templateId", keyValue: "t1" } }),
        schemaVersion: version,
        at: sid("11"),
      })

      const marks = yield* journal.subsetMarks({ entity: "Value", scope: Option.none(), schemaVersion: version })
      assert.deepStrictEqual(
        [...marks].sort((a, b) => a.subset.keyValue.localeCompare(b.subset.keyValue)),
        [
          { subset: { indexKey: "templateId", keyValue: "t1" }, at: sid("5") },
          { subset: { indexKey: "templateId", keyValue: "t2" }, at: sid("7") },
        ],
      )

      // A schema bump dumps the saved table — old-version marks describe dead rows.
      const bumped = yield* journal.subsetMarks({ entity: "Value", scope: Option.none(), schemaVersion: SchemaVersion.make(2) })
      assert.deepStrictEqual(bumped, [])
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("prune's per-model minimum includes subset marks — a trailing subset keeps its replay tail", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const version = SchemaVersion.make(1)
      yield* journal.append(["1", "2", "3"].map((s) => insert(s, "org-1", `w${s}`)))
      // The collection itself is far ahead…
      yield* journal.setCollectionLastAppliedSyncId({
        key: scopedKey({ entity: "Webhook", scope: "org-1" }),
        schemaVersion: version,
        at: sid("3"),
      })
      // …but one subset's mark trails at 1: rows 2 and 3 are still its replay tail.
      yield* journal.setCollectionLastAppliedSyncId({
        key: subsetKey({ entity: "Webhook", scope: Option.some("org-1"), subset: { indexKey: "channel", keyValue: "c1" } }),
        schemaVersion: version,
        at: sid("1"),
      })
      yield* journal.prune({ maxEventsPerModel: 100, maxEventsTotal: 100 })
      const rows = yield* journal.read({ modelName: ModelName.make("Webhook"), since: sid("0") })
      assert.deepStrictEqual(rows.map((r) => r.syncId), [sid("2"), sid("3")])
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("resetToEpoch wipes subset marks with everything else", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const version = SchemaVersion.make(1)
      yield* journal.setCollectionLastAppliedSyncId({
        key: subsetKey({ entity: "Value", scope: Option.none(), subset: { indexKey: "templateId", keyValue: "t1" } }),
        schemaVersion: version,
        at: sid("5"),
      })
      yield* journal.resetToEpoch({ epoch: Epoch.make("new"), at: sid("1") })
      assert.deepStrictEqual(
        yield* journal.subsetMarks({ entity: "Value", scope: Option.none(), schemaVersion: version }),
        [],
      )
    }).pipe(Effect.provide(SyncJournal.layerMemory)))

  it.effect("pre-partial-index at-rest records still decode — scope and subset default to None", () =>
    Effect.gen(function* () {
      // A journal written by an old client: the last-applied record has no scope/subset
      // fields, stored under the frozen key spelling. The upgraded policy layer must
      // read it as a plain collection mark, never die, and never mistake it for a subset.
      const store = yield* makeMemoryStore
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      yield* store.commit(
        JournalWrite.Patch({
          putRecords: [['wm:["Webhook","org-1"]', { entity: "Webhook", schemaVersion: 1, at: "7" }]],
        }),
      )
      const journal = makeSyncJournal(store)
      assert.deepStrictEqual(
        yield* journal.getCollectionLastAppliedSyncId({ key, schemaVersion: SchemaVersion.make(1) }),
        Option.some(sid("7")),
      )
      assert.deepStrictEqual(
        yield* journal.subsetMarks({ entity: "Webhook", scope: Option.some("org-1"), schemaVersion: SchemaVersion.make(1) }),
        [],
      )
      // Prune folds the old record without dying.
      yield* journal.prune({ maxEventsPerModel: 100, maxEventsTotal: 100 })
    }))

  it.effect("prune squashes an entity's history to its newest event without moving the prune boundary", () =>
    Effect.gen(function* () {
      const journal = yield* SyncJournal
      const key = scopedKey<unknown>({ entity: "Webhook", scope: "org-1" })
      const version = SchemaVersion.make(1)
      // Same entity three times: squash keeps only #5.
      yield* journal.append(["1", "3", "5"].map((s) => insert(s, "org-1", "w1")))
      yield* journal.setCollectionLastAppliedSyncId({ key, schemaVersion: version, at: sid("0") })
      yield* journal.prune({ maxEventsPerModel: 100, maxEventsTotal: 100 })
      const rows = yield* journal.read({ modelName: ModelName.make("Webhook"), since: sid("0") })
      assert.deepStrictEqual(rows.map((r) => r.syncId), [sid("5")])
      assert.deepStrictEqual(yield* journal.highestPrunedSyncId(ModelName.make("Webhook")), Option.none())
    }).pipe(Effect.provide(SyncJournal.layerMemory)))
})
