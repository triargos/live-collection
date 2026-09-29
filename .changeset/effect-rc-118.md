---
"@triargos/live-collection-protocol": minor
"@triargos/live-collection-server": minor
"@triargos/live-collection": minor
"@triargos/live-collection-react": minor
---

Upgrade to Effect `4.0.0-rc.118`.

The `effect` peer range moves to `^4.0.0-rc.118`. Upgrade Effect in lockstep; a
workspace cannot mix Effect release candidates in one type graph.

Renamed APIs, applied across all packages and the reference app:

- Every `effect/unstable/*` module moved to `effect/*`; the old paths are removed.
  `effect/unstable/http` is now `effect/http`.
- `effect/unstable/httpapi` is now `effect/http-api`.
- `NetAddress` lives in `effect/net`; `HttpServer.address` is a `NetAddress.SocketAddress`
  (`InetAddressV4 | InetAddressV6 | UnixPathAddress`) instead of `TcpAddress`.
- Built-in `Config` constructors are PascalCase: `Config.string` is `Config.String`, and
  `Config.Port("PORT")` replaces `Config.schema(Config.Port, "PORT")`.

The library's exported services, layers, and error classes keep their names, tags, and
fields. Apps only update their own Effect imports, for example `FetchHttpClient` from
`effect/http`.

**One-time local rebuild on first load.** `deriveSchemaVersion` hashes Effect's
AST representation, and that representation changed shape. Every collection
derives a new version, so TanStack dumps and rebuilds the persisted local table,
the journal mark is orphaned, and the mount decides `Snapshot` and re-lists from
the server. This costs one full refetch per collection and then settles.
