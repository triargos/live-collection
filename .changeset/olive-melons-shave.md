---
"@triargos/live-collection-server": minor
---

feat!: the event bus exposes a stream instead of a subscription handle

`SyncEventBusShape.subscribe: Effect<PubSub.Subscription<SyncEvent>, never, Scope>`
becomes `SyncEventBusShape.events: Stream.Stream<SyncEvent>`. The port no longer
leaks PubSub into the interface an app implements, and the subscription lifetime
moves inside the stream — so a Redis or Postgres-`LISTEN` adapter needs no scope
story of its own. The sole consumer already unwrapped the handle into a stream on
the next line.

`SyncFeed.streamEvents` consequently returns `Stream.Stream<string>` with no
`Scope` requirement, so SSE routes can drop `Stream.provideContext(requestScope)`:

```diff
-const requestScope = yield* Effect.context<Scope.Scope>()
 return HttpServerResponse.stream(
-  feed.streamEvents({ syncGroups }).pipe(Stream.provideContext(requestScope), Stream.encodeText),
+  feed.streamEvents({ syncGroups }).pipe(Stream.encodeText),
   { contentType: "text/event-stream" },
 )
```

Custom bus adapters must return a stream that subscribes per run and releases on
termination, not a shared already-subscribed stream.
