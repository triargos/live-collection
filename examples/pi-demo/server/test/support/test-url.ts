import { Context } from "effect"
import { HttpServer } from "effect/unstable/http"

/**
 * Reads the ephemeral TCP port the test server bound (via `port: 0`) and
 * returns its base URL. Tests must never hardcode ports: parallel or leaked
 * runs on CI collide with EADDRINUSE.
 */
export const testServerUrl = (services: Context.Context<HttpServer.HttpServer>): string => {
  const address = Context.get(services, HttpServer.HttpServer).address
  if (address._tag !== "TcpAddress") {
    throw new Error("test server must listen on a TCP address")
  }
  return `http://127.0.0.1:${address.port}`
}
