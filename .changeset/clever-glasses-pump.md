---
"@triargos/live-collection": patch
"@triargos/live-collection-server": patch
---

fixed serialization of non-encodable types like dates and maps. use a schema codec to properly encode / decode them at the wire edges instead of letting the http client encode them
