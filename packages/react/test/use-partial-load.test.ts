import { describe, expect, it } from "vitest"
import { ModelName } from "@triargos/live-collection-protocol"
import { HydrateFailed, SubsetForbidden } from "@triargos/live-collection"
import { statusFromRejection, SubsetStatus } from "../src/index.js"

const forbidden = new SubsetForbidden({
  modelName: ModelName.make("Value"),
  indexKey: "templateId",
  keyValue: "t1",
})

describe("statusFromRejection", () => {
  it("SubsetForbidden ⇒ Forbidden carrying the typed error", () => {
    const status = statusFromRejection(forbidden, () => {})
    expect(status._tag).toBe("Forbidden")
    if (status._tag === "Forbidden") expect(status.error).toBe(forbidden)
  })

  it("HydrateFailed ⇒ Failed carrying the retry handle", () => {
    const error = new HydrateFailed({ reason: "boom" })
    let retried = 0
    const status = statusFromRejection(error, () => {
      retried += 1
    })
    expect(status._tag).toBe("Failed")
    if (status._tag === "Failed") {
      expect(status.error).toBe(error)
      status.retry()
      expect(retried).toBe(1)
    }
  })

  it("an unrecognized rejection still becomes Failed — never an eternal Loading, never a throw", () => {
    const status = statusFromRejection(new Error("defect"), () => {})
    expect(status._tag).toBe("Failed")
    if (status._tag === "Failed") expect(status.error.reason).toContain("defect")
  })

  it("SubsetStatus matches exhaustively", () => {
    const label = SubsetStatus.$match(statusFromRejection(forbidden, () => {}), {
      Loading: () => "loading",
      Ready: () => "ready",
      Forbidden: () => "forbidden",
      Failed: () => "failed",
    })
    expect(label).toBe("forbidden")
  })
})
