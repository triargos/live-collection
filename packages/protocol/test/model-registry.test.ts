import { Effect, Option, Result, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelName } from "../src/ids.js"
import { defineModelRegistry, narrowModelName } from "../src/model-registry.js"

const Person = Schema.Struct({ id: Schema.String, name: Schema.String })
const name = (s: string): ModelName => ModelName.make(s)

describe("narrowModelName", () => {
  const known = ["Webhook", "Channel"] as const

  it("returns Success(name) for a registered name", () => {
    const result = narrowModelName(known, name("Webhook"))
    assert.isTrue(Result.isSuccess(result))
    if (Result.isSuccess(result)) assert.strictEqual(result.success, "Webhook")
  })

  it("returns Failure(UnknownModelError) carrying context for an unknown name", () => {
    const result = narrowModelName(known, name("Ghost"))
    assert.isTrue(Result.isFailure(result))
    if (Result.isFailure(result)) {
      assert.strictEqual(result.failure._tag, "UnknownModelError")
      assert.strictEqual(result.failure.modelName, "Ghost")
      assert.deepStrictEqual([...result.failure.known], ["Webhook", "Channel"])
    }
  })

  it("never throws on an unknown name — failure is data, not an exception", () => {
    assert.doesNotThrow(() => narrowModelName(known, name("Nope")))
  })
})

describe("defineModelRegistry", () => {
  it("rejects a descriptor whose modelName does not equal its key (type-level)", () => {
    const mismatched = {
      modelName: "WRONG" as const,
      schema: Person,
      hydrate: () => Effect.succeed(Option.none())
    }
    // @ts-expect-error modelName literal "WRONG" must equal its key "Webhook"
    defineModelRegistry({ Webhook: mismatched })
  })
})
