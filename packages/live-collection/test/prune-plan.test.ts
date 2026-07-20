import { Option } from "effect"
import * as fc from "effect/testing/FastCheck"
import { assert, describe, it } from "@effect/vitest"
import { ModelId, ModelName, SyncId } from "@triargos/live-collection-protocol"
import type { JournalEvent } from "../src/client/sync-journal.js"
import { prunePlan } from "../src/client/prune-plan.js"

const sid = SyncId.make

/** A row with a unique entity per syncId — squash-neutral, for exercising stages 2–3. */
const row = (model: string, syncId: string): JournalEvent => ({
  syncId: sid(syncId),
  modelName: ModelName.make(model),
  tag: "Insert",
  modelId: ModelId.make(`${model}-${syncId}`),
  data: Option.some({}),
})

/** A row addressing a specific entity — for exercising stage-1 squash. */
const evt = (
  model: string,
  id: string,
  syncId: string,
  tag: "Insert" | "Update" | "Delete" = "Insert",
): JournalEvent => ({
  syncId: sid(syncId),
  modelName: ModelName.make(model),
  tag,
  modelId: ModelId.make(id),
  data: tag === "Delete" ? Option.none() : Option.some({ at: syncId }),
})

const minOf = (entries: Record<string, string>) =>
  new Map(Object.entries(entries).map(([model, s]) => [model, sid(s)]))

const ids = (rows: ReadonlyArray<JournalEvent>) =>
  rows.map((r) => r.syncId).sort((a, b) => Number(a) - Number(b))

/** Neutralize stage 2 for a set of models: every model has a record at "0" (below all rows). */
const zeroMin = (...models: ReadonlyArray<string>) => minOf(Object.fromEntries(models.map((m) => [m, "0"])))

describe("prunePlan — stage 3 (count caps, floor-moving)", () => {
  it("keeps the newest maxEventsPerModel events of a model and reports the highest deleted as the floor", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "1"), row("Webhook", "2"), row("Webhook", "3")],
      minLastApplied: zeroMin("Webhook"),
      maxEventsPerModel: 2,
      maxEventsTotal: 100,
    })
    assert.deepStrictEqual(ids(plan.keep), [sid("2"), sid("3")])
    assert.strictEqual(plan.maxDeletedSyncId.get("Webhook"), sid("1"))
  })

  it("isolates models: a chatty model is trimmed while a quiet one is left whole", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "1"), row("Webhook", "2"), row("Webhook", "3"), row("Settings", "1")],
      minLastApplied: zeroMin("Webhook", "Settings"),
      maxEventsPerModel: 2,
      maxEventsTotal: 100,
    })
    // Settings (1 event) survives intact; only Webhook loses its oldest.
    assert.isFalse(plan.maxDeletedSyncId.has("Settings"))
    assert.strictEqual(plan.maxDeletedSyncId.get("Webhook"), sid("1"))
    assert.deepStrictEqual(ids(plan.keep), [sid("1"), sid("2"), sid("3")]) // S1, W2, W3
  })

  it("applies the global cap, trimming the oldest across models past maxEventsTotal", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "10"), row("Settings", "20"), row("Settings", "30")],
      minLastApplied: zeroMin("Webhook", "Settings"),
      maxEventsPerModel: 100, // per-model keeps everything…
      maxEventsTotal: 2, // …but the global cap keeps only the 2 newest overall
    })
    assert.deepStrictEqual(ids(plan.keep), [sid("20"), sid("30")])
    assert.strictEqual(plan.maxDeletedSyncId.get("Webhook"), sid("10")) // the oldest, trimmed globally
  })

  it("prunes nothing when within both caps", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "1"), row("Settings", "2")],
      minLastApplied: zeroMin("Webhook", "Settings"),
      maxEventsPerModel: 10,
      maxEventsTotal: 10,
    })
    assert.strictEqual(plan.keep.length, 2)
    assert.strictEqual(plan.maxDeletedSyncId.size, 0)
  })
})

describe("prunePlan — stage 1 (squash, floor-neutral)", () => {
  it("keeps only the newest event per entity and never moves the floor", () => {
    const plan = prunePlan({
      rows: [evt("Webhook", "w1", "1"), evt("Webhook", "w1", "3", "Update"), evt("Webhook", "w1", "5", "Update")],
      minLastApplied: zeroMin("Webhook"),
      maxEventsPerModel: 100,
      maxEventsTotal: 100,
    })
    assert.deepStrictEqual(ids(plan.keep), [sid("5")])
    assert.strictEqual(plan.maxDeletedSyncId.size, 0) // squash deletions are floor-neutral
  })

  it("keeps a Delete tombstone (never the protocol squasher's Drop)", () => {
    // A collection whose last-applied sits between 1 and 4 saw the Insert; dropping the
    // pair would leave it a zombie row. The terminal Delete must survive.
    const plan = prunePlan({
      rows: [evt("Webhook", "w1", "1"), evt("Webhook", "w1", "4", "Delete")],
      minLastApplied: zeroMin("Webhook"),
      maxEventsPerModel: 100,
      maxEventsTotal: 100,
    })
    assert.strictEqual(plan.keep.length, 1)
    assert.strictEqual(plan.keep[0]!.tag, "Delete")
    assert.strictEqual(plan.keep[0]!.syncId, sid("4"))
    assert.strictEqual(plan.maxDeletedSyncId.size, 0)
  })

  it("squashes per entity, not per model — distinct entities each keep their newest", () => {
    const plan = prunePlan({
      rows: [evt("Webhook", "a", "1"), evt("Webhook", "b", "2"), evt("Webhook", "a", "3", "Update")],
      minLastApplied: zeroMin("Webhook"),
      maxEventsPerModel: 100,
      maxEventsTotal: 100,
    })
    assert.deepStrictEqual(ids(plan.keep), [sid("2"), sid("3")])
  })
})

describe("prunePlan — stage 2 (dead weight, floor-neutral)", () => {
  it("drops rows at or below the model's minimum last-applied syncId, inclusive", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "1"), row("Webhook", "2"), row("Webhook", "3")],
      minLastApplied: minOf({ Webhook: "2" }),
      maxEventsPerModel: 100,
      maxEventsTotal: 100,
    })
    assert.deepStrictEqual(ids(plan.keep), [sid("3")]) // 1 and 2 are ≤ min ⇒ dead weight
    assert.strictEqual(plan.maxDeletedSyncId.size, 0) // dead-weight deletions are floor-neutral
  })

  it("drops ALL rows of a model with no last-applied record (any mount snapshots regardless)", () => {
    const plan = prunePlan({
      rows: [row("Webhook", "1"), row("Webhook", "2"), row("Settings", "3")],
      minLastApplied: minOf({ Settings: "0" }), // Webhook has no record
      maxEventsPerModel: 100,
      maxEventsTotal: 100,
    })
    assert.deepStrictEqual(ids(plan.keep), [sid("3")]) // only Settings survives
    assert.strictEqual(plan.maxDeletedSyncId.size, 0)
  })
})

// ── Convergence property: pruning must be invisible to every possible replayer ──
//
// For any event stream, any per-model minimum `min`, and any collection last-applied
// position L ≥ min: (state at L from full history) + (kept rows > L) must equal the
// terminal state of the full history — under the drain's binary fold (Upsert = set,
// Delete = remove). Caps are infinite so only the floor-neutral stages 1–2 act.

const MODELS = ["A", "B"] as const
const IDS = ["1", "2", "3"] as const

type Choice = { readonly mi: number; readonly idi: number; readonly del: boolean }
const choiceArb: fc.Arbitrary<Choice> = fc.record({
  mi: fc.integer({ min: 0, max: MODELS.length - 1 }),
  idi: fc.integer({ min: 0, max: IDS.length - 1 }),
  del: fc.boolean(),
})

const buildStream = (choices: ReadonlyArray<Choice>, steps: ReadonlyArray<number>): Array<JournalEvent> => {
  const present = new Set<string>()
  const events: Array<JournalEvent> = []
  let n = 0
  choices.forEach((choice, i) => {
    n += 1 + (steps[i] ?? 0) // strictly increasing, gap-tolerant
    const model = MODELS[choice.mi]!
    const id = IDS[choice.idi]!
    const k = `${model}\u0000${id}`
    const tag = !present.has(k) ? "Insert" : choice.del ? "Delete" : "Update"
    if (tag === "Delete") present.delete(k)
    else present.add(k)
    events.push(evt(model, id, String(n), tag))
  })
  return events
}

const streamArb = fc
  .tuple(
    fc.array(choiceArb, { minLength: 0, maxLength: 30 }),
    fc.array(fc.integer({ min: 0, max: 4 }), { maxLength: 30 }),
  )
  .map(([choices, steps]) => buildStream(choices, steps))

/** The drain's binary fold: Upsert replaces, Delete removes. Rows must be syncId-ordered. */
const applyInOrder = (state: Map<string, unknown>, rows: ReadonlyArray<JournalEvent>): Map<string, unknown> => {
  const sorted = [...rows].sort((a, b) => Number(a.syncId) - Number(b.syncId))
  for (const r of sorted) {
    const k = `${r.modelName}\u0000${r.modelId}`
    if (r.tag === "Delete") state.delete(k)
    else state.set(k, Option.getOrNull(r.data))
  }
  return state
}

const stateThrough = (events: ReadonlyArray<JournalEvent>, through: number): Map<string, unknown> =>
  applyInOrder(new Map(), events.filter((e) => Number(e.syncId) <= through))

describe("prunePlan — convergence contract (stages 1–2)", () => {
  const boundsArb = fc.tuple(fc.integer({ min: 0, max: 40 }), fc.integer({ min: 0, max: 40 }))

  it("state at any L ≥ min, plus replay of kept rows > L, equals the terminal state", () => {
    fc.assert(
      fc.property(streamArb, boundsArb, (events, [a, b]) => {
        const min = Math.min(a, b)
        const lastApplied = Math.max(a, b)
        const plan = prunePlan({
          rows: events,
          minLastApplied: minOf(Object.fromEntries(MODELS.map((m) => [m, String(min)]))),
          maxEventsPerModel: Number.MAX_SAFE_INTEGER,
          maxEventsTotal: Number.MAX_SAFE_INTEGER,
        })
        const replayed = applyInOrder(
          stateThrough(events, lastApplied),
          plan.keep.filter((r) => Number(r.syncId) > lastApplied),
        )
        const terminal = stateThrough(events, Number.MAX_SAFE_INTEGER)
        assert.deepStrictEqual(
          [...replayed.entries()].sort(),
          [...terminal.entries()].sort(),
        )
      }),
    )
  })

  it("stages 1–2 never move the floor", () => {
    fc.assert(
      fc.property(streamArb, fc.integer({ min: 0, max: 40 }), (events, min) => {
        const plan = prunePlan({
          rows: events,
          minLastApplied: minOf(Object.fromEntries(MODELS.map((m) => [m, String(min)]))),
          maxEventsPerModel: Number.MAX_SAFE_INTEGER,
          maxEventsTotal: Number.MAX_SAFE_INTEGER,
        })
        assert.strictEqual(plan.maxDeletedSyncId.size, 0)
      }),
    )
  })

  it("is idempotent: pruning the kept rows again keeps them all", () => {
    fc.assert(
      fc.property(streamArb, fc.integer({ min: 0, max: 40 }), (events, min) => {
        const args = {
          minLastApplied: minOf(Object.fromEntries(MODELS.map((m) => [m, String(min)]))),
          maxEventsPerModel: Number.MAX_SAFE_INTEGER,
          maxEventsTotal: Number.MAX_SAFE_INTEGER,
        }
        const once = prunePlan({ rows: events, ...args })
        const twice = prunePlan({ rows: once.keep, ...args })
        assert.deepStrictEqual(ids(twice.keep), ids(once.keep))
      }),
    )
  })
})
