---
"@triargos/live-collection-protocol": minor
"@triargos/live-collection-server": minor
---

feat!: `narrowModelName` returns `Option`, and `UnknownModelError` is removed

An unregistered model name is the routine outcome of talking to a newer backend,
not a failure — every caller already discarded the error and dropped the event.
Modelling it as `Option.Option<N>` says that directly, and drops a tagged error
whose `known` payload nothing ever read.

```diff
-Result.match(narrowModelName(knownNames, event.modelName), {
-  onFailure: () => Effect.logDebug(`skipping ${event.modelName}`),
-  onSuccess: (name) => dispatch(registry[name], event),
-})
+Option.match(narrowModelName(knownNames, event.modelName), {
+  onNone: () => Effect.logDebug(`skipping ${event.modelName}`),
+  onSome: (name) => dispatch(registry[name], event),
+})
```
