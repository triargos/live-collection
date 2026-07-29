import { Effect, Option, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelName } from "../src/ids.js"
import { defineModelRegistry, narrowModelName } from "../src/model-registry.js"

const Person = Schema.Struct({ id: Schema.String, name: Schema.String })
const name = (s: string): ModelName => ModelName.make(s)

describe("narrowModelName", () => {
  const known = ["Webhook", "Channel"] as const

  it("returns the narrowed name for a registered name", () => {
    const result = narrowModelName(known, name("Webhook"))
    assert.deepStrictEqual(result, Option.some("Webhook"))
  })

  it("returns None for an unknown name", () => {
    assert.deepStrictEqual(narrowModelName(known, name("Ghost")), Option.none())
  })

  it("never throws on an unknown name — absence is data, not an exception", () => {
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
