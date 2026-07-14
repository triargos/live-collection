# pi-demo — live todos

An end-to-end demo of `@triargos/live-collection`: a todo app whose collections are
persisted locally (OPFS SQLite), written optimistically, and kept live over SSE + catchup
against a real Effect backend.

A six-character session code is a shareable capability. Open or create a session on one
device and enter the same code on another; only that session's rows and sync events are
visible on either device.

```
shared/   @pi-demo/shared   domain schemas + the HttpApi contract (server and web both import it)
server/   @pi-demo/server   Effect HttpApi + NodeHttpServer; in-memory sync event log + PubSub bus; SSE at /api/sync
web/      @pi-demo/web      Vite + React Router SPA; scoped collections; joined live queries (todos ⋈ projects)
```

## Run locally

```bash
pnpm --filter @pi-demo/server dev   # backend on :3050
pnpm --filter @pi-demo/web dev      # SPA on :5183 (proxies /api → :3050)
```

Open http://localhost:5183, create a session, then join it with the displayed code in a
second browser or device. Writes appear live on both. Reloading hydrates from OPFS and
the sync loop catches up from the stored cursor instead of re-fetching everything.

## Deploy with Docker

Build from the repository root, then run the single-container app:

```bash
docker build -f examples/pi-demo/Dockerfile -t pi-demo .
docker run --rm -p 3050:3050 pi-demo
```

Open http://localhost:3050, create a session, and join with its code on another device.
For a remote deployment, expose port 3050 through your platform's HTTPS endpoint.

The backend stores rows and the sync event log in memory, so all server data is lost when
the container restarts. No secrets, database, or volume are required. `PORT` controls the
listen port (default `3050`); `STATIC_DIR` points to the built SPA directory (the image
sets it to `/app/examples/pi-demo/web/dist`).

## What it exercises

- **Write path:** optimistic `collection.insert/update/delete` → REST upsert → confirmed
  row reconciled into the synced baseline → SSE echo is an idempotent re-write.
- **Read path:** cold start `listFn` snapshot → durable `lastSyncId` cursor → catchup
  (squashed server-side) → live SSE tail.
- **Scoped sessions:** each collection instance and event route is keyed by session code;
  the server derives its permission group from the `x-session-code` capability header.
- **Joins:** the todo list is a `useLiveQuery` join across two synced collections
  (`Todo.projectId ⋈ Project.id`); the sidebar aggregates todo counts per project.
- **Fan-out:** deleting a project cascades on the server — one HTTP call, many sync
  events (the project's `Delete` plus one per todo), all healed live on every client.
