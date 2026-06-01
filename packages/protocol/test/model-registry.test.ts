import { Effect, Either, Option, Schema } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { ModelName } from "../src/ids.js"
import { defineModelRegistry, narrowModelName } from "../src/model-registry.js"

const Person = Schema.Struct({ id: Schema.String, name: Schema.String })
const name = (s: string): ModelName => ModelName.make(s)

describe("narrowModelName", () => {
  const known = ["Webhook", "Channel"] as const

  it("returns Right(name) for a registered name", () => {
    const result = narrowModelName(known, name("Webhook"))
    assert.isTrue(Either.isRight(result))
    if (Either.isRight(result)) assert.strictEqual(result.right, "Webhook")
  })

  it("returns Left(UnknownModelError) carrying context for an unknown name", () => {
    const result = narrowModelName(known, name("Ghost"))
    assert.isTrue(Either.isLeft(result))
    if (Either.isLeft(result)) {
      assert.strictEqual(result.left._tag, "UnknownModelError")
      assert.strictEqual(result.left.modelName, "Ghost")
      assert.deepStrictEqual([...result.left.known], ["Webhook", "Channel"])
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
