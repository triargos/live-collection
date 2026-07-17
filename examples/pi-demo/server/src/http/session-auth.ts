import { HttpServerRequest } from "effect/unstable/http"
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
    : Schema.decodeUnknownEffect(SessionCode)(raw.trim().toUpperCase()).pipe(
        Effect.mapError(() => new UnauthorizedError({ reason: "invalid x-session-code header" })),
      )

const authenticate = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest
  return yield* decodeSessionCode(request.headers["x-session-code"])
})

export const SessionAuthLive: Layer.Layer<SessionAuth> = Layer.succeed(SessionAuth, (httpEffect) =>
  authenticate.pipe(
    Effect.flatMap((session) => httpEffect.pipe(Effect.provideService(CurrentSession, session))),
  ),
)

export const sessionCodeFromRequest = (request: HttpServerRequest.HttpServerRequest) =>
  decodeSessionCode(request.headers["x-session-code"])
