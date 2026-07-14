import { Option, Schema } from "effect"
import { SessionCode } from "@pi-demo/shared"

const SESSION_KEY = "pi-demo:session"
const CREATED_KEY = "pi-demo:session-created"
let createdFlagConsumed = false

export const normalizeSessionCode = (input: string): string => input.trim().toUpperCase()

export const decodeSessionCode = (input: string): Option.Option<SessionCode> =>
  Schema.decodeUnknownOption(SessionCode)(normalizeSessionCode(input))

export const getSession = (): Option.Option<SessionCode> => {
  const stored = localStorage.getItem(SESSION_KEY)
  return stored === null ? Option.none() : decodeSessionCode(stored)
}

export const setSession = (code: SessionCode): void => {
  localStorage.setItem(SESSION_KEY, code)
  location.assign("/")
}

export const createSession = (code: SessionCode): void => {
  sessionStorage.setItem(CREATED_KEY, code)
  setSession(code)
}

export const clearSession = (): void => {
  localStorage.removeItem(SESSION_KEY)
  sessionStorage.removeItem(CREATED_KEY)
  location.assign("/")
}

export const consumeCreatedSession = (code: SessionCode): boolean => {
  if (createdFlagConsumed) return false
  createdFlagConsumed = true
  const created = sessionStorage.getItem(CREATED_KEY) === code
  sessionStorage.removeItem(CREATED_KEY)
  return created
}
