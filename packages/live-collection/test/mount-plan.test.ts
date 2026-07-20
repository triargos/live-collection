import { describe, expect, it } from "vitest"
import { Option } from "effect"
import { FastCheck as fc } from "effect/testing"
import { ModelId, ModelName, SyncId, compareSyncId } from "@triargos/live-collection-protocol"
import type { JournalEvent } from "../src/client/sync-journal.js"
import { PublishedItem } from "../src/client/ingest.js"
import { MountDecision, concernsModel, dropStale, planMount } from "../src/client/mount-plan.js"

const id = (n: number) => SyncId.make(String(n))
const some = (n: number) => Option.some(id(n))

const plan = (input: {
  lastApplied?: number | undefined
  cursor?: number | undefined
  maxDeleted?: number | undefined
  lastResync?: number | undefined
}) =>
  planMount({
    collectionLastApplied: input.lastApplied === undefined ? Option.none() : some(input.lastApplied),
    cursor: input.cursor === undefined ? Option.none() : some(input.cursor),
    maxDeletedSyncId: input.maxDeleted === undefined ? Option.none() : some(input.maxDeleted),
    lastResyncAt: input.lastResync === undefined ? Option.none() : some(input.lastResync),
  })

describe("planMount", () => {
  it("decides Snapshot at the cursor when no last-applied record exists (fresh install / schema bump)", () => {
    const result = plan({ cursor: 7 })
    expect(result.decision).toEqual(MountDecision.Snapshot({ at: id(7) }))
    expect(result.since).toBe(id(7))
    expect(result.tailGuardSeed).toBe(id(7))
  })

  it("snapshots at zero when there is no cursor either (true cold start)", () => {
    expect(plan({}).decision).toEqual(MountDecision.Snapshot({ at: id(0) }))
  })

  it("decides Snapshot when a resync crossed the last-applied point, at max(cursor, resync)", () => {
    const result = plan({ lastApplied: 5, cursor: 9, lastResync: 6 })
    expect(result.decision).toEqual(MountDecision.Snapshot({ at: id(9) }))
    const past = plan({ lastApplied: 5, cursor: 9, lastResync: 12 })
    expect(past.decision).toEqual(MountDecision.Snapshot({ at: id(12) }))
  })

  it("decides Skip when the collection is already at or past the cursor — even if pruning ate history", () => {
    const result = plan({ lastApplied: 9, cursor: 9, maxDeleted: 20 })
    expect(result.decision).toEqual(MountDecision.Skip())
    expect(result.since).toBe(id(9))
    expect(result.tailGuardSeed).toBe(id(9))
  })

  it("decides Snapshot when pruning deleted events above the last-applied (the gap is gone)", () => {
    const result = plan({ lastApplied: 5, cursor: 9, maxDeleted: 6 })
    expect(result.decision).toEqual(MountDecision.Snapshot({ at: id(9) }))
  })

  it("decides Replay from the last-applied when the journal still covers the gap", () => {
    const result = plan({ lastApplied: 5, cursor: 9, maxDeleted: 3 })
    expect(result.decision).toEqual(MountDecision.Replay())
    expect(result.since).toBe(id(5))
    expect(result.tailGuardSeed).toBe(id(9))
  })

  it("a resync at or below the last-applied does not force a snapshot", () => {
    expect(plan({ lastApplied: 5, cursor: 9, lastResync: 5 }).decision).toEqual(MountDecision.Replay())
  })

  it("property: since and tailGuardSeed are always consistent with the decision", () => {
    const maybe = fc.option(fc.integer({ min: 0, max: 50 }), { nil: undefined })
    fc.assert(
      fc.property(maybe, maybe, maybe, maybe, (lastApplied, cursor, maxDeleted, lastResync) => {
        const result = plan({ lastApplied, cursor, maxDeleted, lastResync })
        // The tail guard never trails the replay start: nothing read is later re-emitted by the tail.
        expect(compareSyncId(result.tailGuardSeed, result.since)).toBeGreaterThanOrEqual(0)
        if (result.decision._tag === "Snapshot") {
          expect(result.since).toBe(result.decision.at)
          expect(result.tailGuardSeed).toBe(result.decision.at)
        } else {
          expect(result.since).toBe(id(lastApplied ?? 0))
        }
      }),
    )
  })
})

const row = (syncId: number, model = "Webhook"): JournalEvent => ({
  syncId: id(syncId),
  modelName: ModelName.make(model),
  tag: "Insert",
  modelId: ModelId.make(`m-${syncId}`),
  data: Option.some({ n: syncId }),
})

describe("concernsModel", () => {
  const concerns = concernsModel(ModelName.make("Webhook"))

  it("keeps own-model events, drops foreign ones", () => {
    expect(concerns(PublishedItem.Event({ row: row(4) }))).toBe(true)
    expect(concerns(PublishedItem.Event({ row: row(9, "Settings") }))).toBe(false)
  })

  it("Resync and EpochReset concern every subscriber", () => {
    expect(concerns(PublishedItem.Resync({ at: id(1) }))).toBe(true)
    expect(concerns(PublishedItem.EpochReset({ at: id(1) }))).toBe(true)
  })
})

describe("dropStale", () => {
  const step = dropStale

  it("emits an event above the guard and advances the guard to it", () => {
    const [guard, signals] = step(id(3), PublishedItem.Event({ row: row(4) }))
    expect(guard).toBe(id(4))
    expect(signals).toHaveLength(1)
  })

  it("drops events at or below the guard", () => {
    expect(step(id(4), PublishedItem.Event({ row: row(4) }))[1]).toHaveLength(0)
  })

  it("re-snapshots on a Resync only if it is newer than the guard", () => {
    expect(step(id(5), PublishedItem.Resync({ at: id(5) }))[1]).toHaveLength(0)
    const [guard, signals] = step(id(5), PublishedItem.Resync({ at: id(6) }))
    expect(guard).toBe(id(6))
    expect(signals[0]?._tag).toBe("Snapshot")
  })

  it("EpochReset bypasses staleness: snapshots even at a smaller syncId", () => {
    const [guard, signals] = step(id(9999), PublishedItem.EpochReset({ at: id(1) }))
    expect(guard).toBe(id(1))
    expect(signals[0]?._tag).toBe("Snapshot")
  })

  it("property: the guard never decreases except through EpochReset", () => {
    const arbItem = fc.oneof(
      fc.integer({ min: 0, max: 99 }).map((n) => PublishedItem.Event({ row: row(n) })),
      fc.integer({ min: 0, max: 99 }).map((n) => PublishedItem.Resync({ at: id(n) })),
    )
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 99 }), fc.array(arbItem, { maxLength: 30 }), (start, items) => {
        let guard = id(start)
        for (const item of items) {
          const [next, signals] = step(guard, item)
          expect(compareSyncId(next, guard)).toBeGreaterThanOrEqual(0)
          // anything emitted is strictly above the previous guard
          for (const signal of signals) {
            if (signal._tag !== "Snapshot") expect(compareSyncId(signal.syncId, guard)).toBeGreaterThan(0)
          }
          guard = next
        }
      }),
    )
  })
})
