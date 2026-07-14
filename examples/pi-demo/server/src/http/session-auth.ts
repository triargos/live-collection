import { HttpServerRequest } from "@effect/platform"
import {
  CurrentSession,
  SessionAuth,
  SessionCode,
  UnauthorizedError,
} from "@pi-demo/shared"
import { Effect, Layer, Schema } from "effect"

const decodeSessionCode = (raw: string | undefined) =>
  raw === undefined
    ? new UnauthorizedError({ reason: "missing x-session-code header" })
    : Schema.decodeUnknown(SessionCode)(raw.trim().toUpperCase()).pipe(
        Effect.mapError(() => new UnauthorizedError({ reason: "invalid x-session-code header" })),
      )

const authenticate = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest
  return yield* decodeSessionCode(request.headers["x-session-code"])
})

export const SessionAuthLive = Layer.succeed(SessionAuth, authenticate)

export const sessionCodeFromRequest = (request: HttpServerRequest.HttpServerRequest) =>
  decodeSessionCode(request.headers["x-session-code"])
