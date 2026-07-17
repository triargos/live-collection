import { NodeRuntime } from "@effect/platform-node"
import { Config, Effect, Layer } from "effect"
import { makeServerLayer } from "./http/server.js"

const ServerLive = Layer.unwrap(
  Config.schema(Config.Port, "PORT").pipe(
    Config.withDefault(3050),
    Effect.map((port) => makeServerLayer({ port })),
  ),
)

Layer.launch(ServerLive).pipe(NodeRuntime.runMain)
