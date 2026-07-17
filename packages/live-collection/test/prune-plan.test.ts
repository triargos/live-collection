import { describe, expect, it } from "vitest"
import { Option } from "effect"
import { ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import type { LoggedEvent } from "../src/client/event-log-store.js"
import { prunePlan } from "../src/client/prune-plan.js"

const row = (model: string, syncId: string): LoggedEvent => ({
  syncId: SyncId.make(syncId),
  modelName: ModelName.make(model),
  tag: "Insert",
  modelId: ModelId.make(`${model}-${syncId}`),
  data: Option.some({}),
})

const ids = (rows: ReadonlyArray<LoggedEvent>) => rows.map((r) => r.syncId).sort()

describe("prunePlan", () => {
  it("keeps the newest perModel events of a model and reports the highest deleted as the floor", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "1"), row("Webhook", "2"), row("Webhook", "3")],
      perModel: 2,
      total: 100,
    })
    expect(ids(plan.keep)).toEqual([SyncId.make("2"), SyncId.make("3")])
    expect(plan.deletedHighWater.get("Webhook")).toBe(SyncId.make("1"))
  })

  it("isolates models: a chatty model is trimmed while a quiet one is left whole", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "1"), row("Webhook", "2"), row("Webhook", "3"), row("Settings", "1")],
      perModel: 2,
      total: 100,
    })
    // Settings (1 event) survives intact; only Webhook loses its oldest.
    expect(plan.deletedHighWater.has("Settings")).toBe(false)
    expect(plan.deletedHighWater.get("Webhook")).toBe(SyncId.make("1"))
    expect(ids(plan.keep)).toEqual([SyncId.make("1"), SyncId.make("2"), SyncId.make("3")]) // S1, W2, W3
  })

  it("applies the global cap, trimming the oldest across models past `total`", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "10"), row("Settings", "20"), row("Settings", "30")],
      perModel: 100, // per-model keeps everything…
      total: 2, // …but the global cap keeps only the 2 newest overall
    })
    expect(ids(plan.keep)).toEqual([SyncId.make("20"), SyncId.make("30")])
    expect(plan.deletedHighWater.get("Webhook")).toBe(SyncId.make("10")) // the oldest, trimmed globally
  })

  it("prunes nothing when within both caps", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "1"), row("Settings", "2")],
      perModel: 10,
      total: 10,
    })
    expect(plan.keep).toHaveLength(2)
    expect(plan.deletedHighWater.size).toBe(0)
  })
})
