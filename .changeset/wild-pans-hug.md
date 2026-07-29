---
"@triargos/live-collection-protocol": minor
"@triargos/live-collection": minor
"@triargos/live-collection-server": minor
"@triargos/live-collection-react": minor
---

Move shared runtime deps to peerDependencies.

`effect`, `@tanstack/db`, `@tanstack/db-sqlite-persistence-core`,
`@triargos/live-collection-protocol`, `@triargos/live-collection`, and `react` all appear
in the public type surface, so duplicate installs broke `Context` tag identity and
collection identity. They are now peers with caret ranges; install them alongside the
library. `idb` stays an internal dependency, and `@tanstack/react-db` is no longer a
runtime dependency of the React package (it was only used by a type test).
