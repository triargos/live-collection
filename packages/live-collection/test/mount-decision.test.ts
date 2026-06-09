import { describe, expect, it } from "vitest"
import { Option } from "effect"
import { SyncId } from "@triargos/live-collection-protocol"
import { decideOnMount, MountDecision } from "../src/client/mount-decision.js"

const sid = (s: string) => SyncId.make(s)

describe("decideOnMount", () => {
  it("bootstraps when the collection has no base watermark", () => {
    expect(
      decideOnMount({
        baseWatermark: Option.none(),
        cursor: Option.some(sid("5")),
        modelFloor: Option.some(sid("1")),
        lastResyncAt: Option.none(),
      }),
    ).toBe(MountDecision.Bootstrap)
  })

  it("skips when the base is already at or past the cursor", () => {
    expect(
      decideOnMount({
        baseWatermark: Option.some(sid("9")),
        cursor: Option.some(sid("9")),
        modelFloor: Option.some(sid("1")),
        lastResyncAt: Option.none(),
      }),
    ).toBe(MountDecision.Skip)
  })

  it("replays when the base is behind the cursor and the log covers the gap", () => {
    expect(
      decideOnMount({
        baseWatermark: Option.some(sid("5")),
        cursor: Option.some(sid("9")),
        modelFloor: Option.some(sid("2")), // floor <= base ⇒ (base, cursor] retained
        lastResyncAt: Option.none(),
      }),
    ).toBe(MountDecision.Replay)
  })

  it("bootstraps when the log was pruned past the base (floor above base)", () => {
    expect(
      decideOnMount({
        baseWatermark: Option.some(sid("5")),
        cursor: Option.some(sid("9")),
        modelFloor: Option.some(sid("7")), // floor > base ⇒ events in (base, floor) gone
        lastResyncAt: Option.none(),
      }),
    ).toBe(MountDecision.Bootstrap)
  })

  it("replays when nothing has been pruned (no floor ⇒ complete from the start)", () => {
    expect(
      decideOnMount({
        baseWatermark: Option.some(sid("5")),
        cursor: Option.some(sid("9")),
        modelFloor: Option.none(), // never pruned ⇒ log complete since the beginning
        lastResyncAt: Option.none(),
      }),
    ).toBe(MountDecision.Replay)
  })

  it("bootstraps when a resync passed since the base, even with no cursor (cleared by the resync)", () => {
    expect(
      decideOnMount({
        baseWatermark: Option.some(sid("50")),
        cursor: Option.none(), // cleared by a live resync; the next catchup hasn't landed (or failed)
        modelFloor: Option.none(),
        lastResyncAt: Option.some(sid("60")), // a resync after the base ⇒ the base is invalidated
      }),
    ).toBe(MountDecision.Bootstrap) // a coerced cursor of "0" must not short-circuit into Skip
  })

  it("bootstraps when a resync passed since the base (invalidated)", () => {
    expect(
      decideOnMount({
        baseWatermark: Option.some(sid("5")),
        cursor: Option.some(sid("9")),
        modelFloor: Option.some(sid("2")), // log would cover, but a resync intervened
        lastResyncAt: Option.some(sid("6")),
      }),
    ).toBe(MountDecision.Bootstrap)
  })
})
