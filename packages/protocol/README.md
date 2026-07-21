# `@triargos/live-collection-protocol`

The wire contract shared by the [`@triargos/live-collection`](https://www.npmjs.com/package/@triargos/live-collection) client and its backend.

Pure and I/O-free (depends only on [`effect`](https://effect.website) v4): the sync event schemas, the sync-group routing keys, the squasher fold, resync targets, branded ids, the model registry and its interface types, and the `/catchup` request/response schemas.

It defines the *shapes* that cross the wire, not the transport: the backend owns the HTTP surface (routes, errors, auth) and wires these schemas into it.

```bash
npm install @triargos/live-collection-protocol effect
```

## When you need it

- You are building a backend for a live-collection client — decode/encode the wire payloads with these schemas instead of hand-rolled types. An optional Effect backend kernel that enforces the contract's invariants for you is [`@triargos/live-collection-server`](https://www.npmjs.com/package/@triargos/live-collection-server).
- You need the branded `ModelId`, the sync-group grammar, or the wire schemas in app code.

## Documentation

- [Protocol reference](https://github.com/triargos/live-collection/blob/main/docs/protocol.md) — every schema, the sync-group grammar, and the squasher.
- [Backend contract](https://github.com/triargos/live-collection/blob/main/docs/backend.md) — the two endpoints a backend must provide and the invariants the client relies on.
- [Repository](https://github.com/triargos/live-collection) — full docs and the frontend engine.

## License

MIT
