import { Effect, Option } from "effect"
import { assert, describe, it } from "@effect/vitest"
import { SyncId } from "@triargos/live-collection-protocol"
import { LastSyncIdStore } from "../src/client/last-sync-id-store.js"

const sid = (s: string) => SyncId.make(s)

describe("LastSyncIdStore", () => {
  it.effect("starts empty, then returns what was set", () =>
    Effect.gen(function* () {
      const store = yield* LastSyncIdStore
      assert.isTrue(Option.isNone(yield* store.get))
      yield* store.set(sid("5"))
      assert.deepStrictEqual(yield* store.get, Option.some(sid("5")))
    }).pipe(Effect.provide(LastSyncIdStore.layerMemory)))

  // The cursor must never regress: a late-arriving older event can't pull it back. And the compare is
  // numeric, not lexical — "12" beats "3" even though it sorts before it as a string.
  it.effect("set is monotonic by numeric magnitude — a smaller id does not regress the cursor", () =>
    Effect.gen(function* () {
      const store = yield* LastSyncIdStore
      yield* store.set(sid("10"))
      yield* store.set(sid("3"))
      assert.deepStrictEqual(yield* store.get, Option.some(sid("10")))
      yield* store.set(sid("12"))
      assert.deepStrictEqual(yield* store.get, Option.some(sid("12")))
    }).pipe(Effect.provide(LastSyncIdStore.layerMemory)))

  it.effect("clear resets to empty (the live-resync reload path)", () =>
    Effect.gen(function* () {
      const store = yield* LastSyncIdStore
      yield* store.set(sid("7"))
      yield* store.clear
      assert.isTrue(Option.isNone(yield* store.get))
    }).pipe(Effect.provide(LastSyncIdStore.layerMemory)))
})
