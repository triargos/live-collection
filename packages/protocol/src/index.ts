/**
 * `@triargos/live-collection-protocol` — the wire contract shared by the
 * live-collection client and its backend.
 *
 * Pure and I/O-free (depends only on `effect`): the sync event schemas, the
 * sync-group routing grammar, the squasher, resync targets, branded ids, the model
 * registry and its interface types, and the catchup request/response schemas.
 *
 * It defines the *shapes* that cross the wire, not the transport: the backend owns
 * the HTTP surface (routes, errors, auth) and wires these schemas into it.
 */
export * from "./ids.js"
export * from "./sync-group.js"
export * from "./resync.js"
export * from "./sync-event.js"
export * from "./squash.js"
export * from "./model-registry.js"
export * from "./catchup.js"
