import { NodeRuntime } from "@effect/platform-node"
import { Config, Effect, Layer } from "effect"
import { makeServerLayer } from "./http/server.js"

const ServerLive = Layer.unwrapEffect(
  Config.integer("PORT").pipe(
    Config.withDefault(3050),
    Effect.map((port) => makeServerLayer({ port })),
  ),
)

Layer.launch(ServerLive).pipe(NodeRuntime.runMain)
