# Live Sync System

> **Self-contained spec + resolved client-persistence decision.**
> Sections 1–22 are the backend specification and build plan (the original
> spec). The original "Persistence decision (hand off to Claude Code)" section
> has been **resolved** — see [§22 Persistence decision (RESOLVED)](#22-persistence-decision-resolved).
> The client-side build plan and this-repo context appendix follow in
> [§A](#a-client-persistence--build-plan-this-repo) and [§B](#b-relevant-context-from-this-repo).
>
> The backend is authoritative. The client lives in this repo
> (`~/IdeaProjects/hosting`).

A Linear-style sync engine layered on top of the existing aggregate pattern.
The server is authoritative. Clients receive a live, hydrated stream of entity
changes filtered by access control, and catch up on missed changes after
disconnection.

Read the whole thing before writing code. The architecture has a few
load-bearing properties that are easy to break.

---

## 1. Relationship to the aggregate pattern

The sync system does not introduce a new write path. Domain models already
follow the aggregate pattern: transitions return `{ model, event }`, services
persist then publish via `EventClient`, repos never publish. **The sync system
is a downstream consumer of those events.**

Concretely: for every aggregate whose changes need to sync, a small
`<entity>-sync-projection.server.ts` file in the aggregate's feature folder
declares handlers that turn domain events into sync events. The aggregate
itself is unchanged.

This means:
- No new discipline at write sites. If a service publishes via `EventClient`
  (which it must, per the aggregate pattern), and the aggregate has a
  projection file, sync just works.
- Aggregates without projections silently opt out of sync. Read-only
  projections and value objects never participate.
- The sync system's footprint inside the domain is zero. Everything lives
  in the sync feature folder plus one projection file per synced aggregate.

## 2. What we're building

```
Domain (existing)                           Sync (new)
─────────────────                           ──────────

Service                                     <entity>-sync-projection.server.ts
  yield* Model.update(...)                    events.handle(WebhookUpdatedEvent, e =>
  yield* repo.save({ model })                   dispatcher.dispatch({
  yield* events.publish(event) ─── via ──►        modelName: 'Webhook',
                                EventClient        modelId:   e.webhookId,
                                                   action:    'U',
                                                   syncGroups: [...],
                                                   clientId:  e.clientId,
                                                 }))


SyncEventDispatcher                         Read path
  - appends to sync_events                  ─────────
  - publishes on SyncEventBus                 GET /sync     (SSE)
                                              GET /catchup  (one-shot)
                                              ↓
                                              hydrate via ModelRegistry
                                              filter by user's sync groups
                                              push to client
```

## 3. Core properties

**Events are reference-only at rest.** `sync_events` stores
`(syncId, modelName, modelId, action, syncGroups, clientId, createdAt)`.
No entity data. Two consequences:
- The log is tiny. Every row ~100 bytes regardless of entity size.
- Squashing is trivial. `I → U → U → D` for the same entity becomes a noop.
  No field-level merging.

**Hydration happens server-side, at read time.** The `/sync` and `/catchup`
endpoints decode each event's referenced entity through its schema and push
the full payload to the client. Clients never resolve references. They
receive ready-to-store entities, decoded against the same response schemas
they already use for REST.

**ACL is correct by construction.** Hydration runs in the recipient's context.
If the user lost access to an entity between when the event was written and
when they receive it, hydration returns `null` and the resolver emits a
synthetic delete. The event log itself never leaks data the user can't see.

**Sync groups are the routing primitive.** Every event is tagged with one or
more groups (`organization:abc`, `organization:abc:channel:xyz`,
`user:550e8400-...`). Every connected client has a current set of groups
based on their memberships. The intersection determines delivery.

**One SSE connection per user, all events multiplexed.** No per-workspace
streams. The client dispatches by `modelName` to local collections.

**Subscriptions are resolved dynamically, not stored.** There is no
`user_subscriptions` table. Permission lookups happen at `/sync` connection
open and at every `/catchup` call. Long-lived connections refresh their
cached sync-group set when they receive a `MembershipChangedEvent` for their
user — see [Live-connection refresh](#live-connection-refresh) below.

## 4. Data model

### `sync_events`

```sql
CREATE TABLE sync_events (
  sync_id      BIGSERIAL PRIMARY KEY,
  model_name   TEXT      NOT NULL,
  model_id     UUID      NOT NULL,
  action       CHAR(1)   NOT NULL CHECK (action IN ('I','U','D','R')),
  sync_groups  TEXT[]    NOT NULL,
  client_id    UUID,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()

  -- R (resync) events:
  --   modelName = "__group" with modelId = <syncGroupId>, OR
  --   modelName = "__all"   with modelId = NULL
  -- See "Resync events" below.
);

CREATE INDEX idx_sync_events_groups ON sync_events USING GIN (sync_groups);
CREATE INDEX idx_sync_events_lookup ON sync_events (model_name, model_id, sync_id);
```

No `data` column. Indexes cover the two access patterns: bulk fetch by group
membership (catchup), and per-entity history (debugging).

That's the only schema this system adds.

## 5. Concepts

### Sync event (at rest)

```typescript
interface SyncEvent {
  syncId:      bigint
  modelName:   string
  modelId:     string
  action:      'I' | 'U' | 'D' | 'R'
  syncGroups:  ReadonlyArray<string>
  clientId:    string | null
  createdAt:   Date
}
```

### Hydrated sync event (on the wire)

```typescript
interface HydratedSyncEvent<T> {
  syncId:      bigint
  modelName:   string                    // client dispatches by this
  modelId:     string
  action:      'I' | 'U' | 'D' | 'R'
  syncGroups:  ReadonlyArray<string>     // for client devtools / debugging
  createdAt:   Date
  data:        T | null                  // entity, decoded through response schema. null for D.
}
```

`T` is the schema-typed response shape (e.g. `OrganizationResponse`).

### Sync group

A string identifying an access scope:
- `user:<uuid>` — only that user.
- `organization:<uuid>` — anyone in the org.
- `organization:<uuid>:channel:<uuid>` — only that channel's members.

Subscriptions can use wildcards (`organization:<uuid>:*`) but events always
carry literal group ids. Wildcards are interpreted at subscription-match
time, not at write time.

### Model registry (for hydration)

Server-side, typed. Maps `modelName` to the response schema and a hydration
function. Used by `/sync` and `/catchup` to turn reference-only events into
hydrated ones.

```typescript
interface ModelDescriptor<T, R> {
  readonly modelName: string
  readonly schema:    Schema.Schema<T, R, never>
  readonly hydrate:   (id: string, ctx: SyncContext) =>
    Effect.Effect<T | null, never, R>
  // hydrate returns null if the entity no longer exists OR
  // the user (carried in ctx) cannot see it.
  readonly hydrateMany?: (ids: ReadonlyArray<string>, ctx: SyncContext) =>
    Effect.Effect<ReadonlyMap<string, T>, never, R>
  // Optional but recommended. Used by /catchup for N+1 elimination.
}

const ModelRegistry = {
  Organization: OrganizationDescriptor,
  User:         UserDescriptor,
  Webhook:      WebhookDescriptor,
  // ...
} as const

type SyncedModelName = keyof typeof ModelRegistry
```

The registry is the seam between the sync system and the rest of the domain.
Each `<entity>-sync-projection.server.ts` references it via its `modelName`
literal; mistyping the name is a compile error.

### `SyncContext`

Carries the recipient's identity into hydration:

```typescript
interface SyncContext {
  readonly userId:     UserId
  readonly clientId:   string                // for echo suppression
  readonly syncGroups: ReadonlyArray<string>  // the user's current groups
}
```

Hydration functions use it to apply ACL — typically by calling the relevant
repo with the user's identity, the same way HTTP handlers do.

### `SyncEventBus`

The fanout primitive. In-memory Effect `PubSub` for the initial
implementation; swap for Redis pub/sub or Postgres `LISTEN`/`NOTIFY` later
without changing call sites.

```typescript
interface SyncEventBus {
  readonly publish:   (event: SyncEvent) => Effect.Effect<void>
  readonly subscribe: () => Effect.Effect<Queue.Dequeue<SyncEvent>, never, Scope.Scope>
}
```

Single broadcast channel. Subscribers receive every event. Per-connection
handlers filter by sync groups before forwarding to their client.

## 6. The projection layer

This is the only thing every synced aggregate adds. One file per aggregate,
in its feature folder.

```typescript
// apps/dashboard/app/features/webhook/sync/webhook-sync-projection.server.ts
import { Effect } from 'effect'
import { EventClient } from '~/services/event-client'
import { SyncEventDispatcher } from '~/features/sync/service/sync-event-dispatcher.server'
import {
  WebhookRegisteredEvent,
  WebhookUpdatedEvent,
  WebhookDeletedEvent,
} from '@app/domain/webhook'

export const WebhookSyncProjection = Effect.gen(function* () {
  const events     = yield* EventClient
  const dispatcher = yield* SyncEventDispatcher

  yield* events.handle(WebhookRegisteredEvent, (event) =>
    dispatcher.dispatch({
      modelName:  'Webhook',
      modelId:    event.webhookId,
      action:     'I',
      syncGroups: [`organization:${event.owner.id}`],
      clientId:   event.clientId,
    }))

  yield* events.handle(WebhookUpdatedEvent, (event) =>
    dispatcher.dispatch({
      modelName:  'Webhook',
      modelId:    event.webhookId,
      action:     'U',
      syncGroups: [`organization:${event.owner.id}`],
      clientId:   event.clientId,
    }))

  yield* events.handle(WebhookDeletedEvent, (event) =>
    dispatcher.dispatch({
      modelName:  'Webhook',
      modelId:    event.webhookId,
      action:     'D',
      syncGroups: [`organization:${event.owner.id}`],
      clientId:   event.clientId,
    }))
})
```

Projection files compose into the app's wiring at boot. They are the only
place where domain-event → sync-event mapping lives.

### Rules for projections

- **One file per aggregate.** Lives at `features/<entity>/sync/<entity>-sync-projection.server.ts`.
- **One handler per domain event.** Don't combine `Registered` and `Updated`
  into a single handler — they have different `syncGroups` derivation in
  many cases (e.g. ownership transfers).
- **`syncGroups` derived from event fields only.** Don't call repos or
  services from inside a projection handler. If the domain event lacks the
  info needed, the right fix is to add a field to the event, not to fetch
  in the projection.
- **No business logic.** Projections are mechanical translations. Validation,
  authorization, side effects — none of it belongs here.
- **No `clientId` plumbing inside the projection.** It comes off the event.
  Services populate it when publishing.

### Domain events need ownership refs

A common case: `WebhookUpdatedEvent` originally carried only the fields that
changed. The projection needs `owner` to compute sync groups. Two options:

- **Add `owner` to update/delete events.** A small redundancy (it's a UUID,
  it doesn't change) in exchange for a self-contained event. Recommended.
- **Look it up in the projection.** Couples the projection to a repository
  and adds latency. Avoid.

The aggregate's `Model.update(...)` and `model.toDeletedEvent()` should
include the owner in the returned event. This is a small change to existing
aggregates being onboarded to sync.

### `clientId` on DomainEvent

For echo suppression to work, every domain event must carry the originating
client's id. Add it to the `DomainEvent` base class:

```typescript
// packages/domain/src/domain-event.ts
export abstract class DomainEvent ... {
  // ...
  readonly clientId: string | null
}
```

Service code populates it from request context when publishing:

```typescript
yield* events.publish(WebhookRegisteredEvent, event, { clientId: ctx.clientId })
```

The exact API depends on the existing `EventClient` surface. If `EventClient`
doesn't currently support per-publish metadata, this is the only invasive
change the sync system requires.

### Re-tagging on ownership transfer

If an entity moves between sync groups (a webhook reassigned to a different
org, a channel moved between workspaces), the aggregate's transition method
should emit **two** events in sequence:

1. `<Entity>DeletedFromGroupEvent` tagged with the old group's members.
2. `<Entity>RegisteredEvent` (or equivalent) tagged with the new group's
   members.

The projection handlers translate these to sync `D` and `I` actions
respectively. The reason: a single "moved" event with both old and new groups
would force the projection to write two sync events from one domain event,
which breaks the "one transition → one event" invariant of the aggregate
pattern.

Most aggregates don't have ownership-transfer semantics, so this case rarely
comes up. Document it for the ones that do.

## 7. `SyncEventDispatcher`

The shared piece called from every projection. Writes the sync event, then
publishes on the bus.

```typescript
export class SyncEventDispatcher extends Effect.Service<SyncEventDispatcher>()(
  'SyncEventDispatcher',
  {
    dependencies: [SyncEventRepository.Default, SyncEventBus.Default],
    effect: Effect.gen(function* () {
      const repo = yield* SyncEventRepository
      const bus  = yield* SyncEventBus

      const dispatch = Effect.fn('SyncEventDispatcher.dispatch')(
        function* (args: DispatchArgs) {
          yield* Effect.annotateCurrentSpan({
            'sync.model_name':   args.modelName,
            'sync.model_id':     args.modelId,
            'sync.action':       args.action,
            'sync.group_count':  args.syncGroups.length,
          })
          const event = yield* repo.append(args)
          yield* bus.publish(event).pipe(
            // Bus publish is best-effort. The DB row is authoritative;
            // missed live deliveries heal via catchup.
            Effect.catchAll((err) =>
              Effect.logWarning('Sync bus publish failed', err)),
          )
          return event
        })

      return { dispatch } as const
    }),
  },
) {}

interface DispatchArgs {
  readonly modelName:  string
  readonly modelId:    string
  readonly action:     'I' | 'U' | 'D' | 'R'
  readonly syncGroups: ReadonlyArray<string>
  readonly clientId:   string | null
}
```

`SyncEventRepository.append` writes the row and returns the populated
`SyncEvent` (with `syncId` and `createdAt` filled by the DB).

The dispatcher is intentionally thin. Everything interesting happens at the
read path.

## 8. The read path

### Permission resolver

A single service that, given a user, returns the set of sync groups they can
currently see. Resolved dynamically against the source of truth — org
memberships, channel rosters, team memberships, whatever drives access.

```typescript
export class SyncPermissionResolver extends Effect.Service<SyncPermissionResolver>()(
  'SyncPermissionResolver',
  {
    effect: Effect.gen(function* () {
      // depends on: OrganizationRepository, ChannelRepository, etc.

      const groupsFor = Effect.fn('SyncPermissionResolver.groupsFor')(
        function* ({ userId }: { userId: UserId }) {
          yield* Effect.annotateCurrentSpan({ 'user.id': userId })
          // Compose all sources of access into a flat array of group strings.
          // Implementation depends on your access model.
          const orgIds      = yield* /* fetch */
          const channelIds  = yield* /* fetch */
          // ...
          return [
            `user:${userId}`,
            ...orgIds.map(id => `organization:${id}`),
            ...channelIds.map(c => `organization:${c.orgId}:channel:${c.channelId}`),
          ] as const
        })

      return { groupsFor } as const
    }),
  },
) {}
```

The resolver is called from `/sync` (at connection open and on refresh
signals) and from `/catchup` (per call). It is the only place that knows how
to derive sync groups from the user's actual permissions.

### `GET /catchup?from={syncId}`

One-shot. Authenticated. Returns squashed, hydrated events since `from`.

```
1. Authenticate. Extract userId, clientId.
2. groups = SyncPermissionResolver.groupsFor({ userId })
3. If from < currentSyncId - retentionWindow:
     return { events: [{ action: 'R', modelName: '__all' }], lastSyncId }
4. raw = SELECT * FROM sync_events
         WHERE sync_id > :from
           AND sync_groups && :groups
           AND (client_id IS NULL OR client_id != :clientId)
         ORDER BY sync_id
5. squashed = squash(raw)
6. hydrated = hydrate(squashed, ctx)
7. return { events: hydrated, lastSyncId }
```

Hydration in step 6 uses batched `hydrateMany` from the model registry — one
call per `modelName`, not one per event. Without batching this is the
N+1 problem in disguise.

### `GET /sync` (SSE)

Long-lived. Authenticated. Pushes hydrated events as they happen.

```
On connect:
  1. Authenticate. Extract userId, clientId.
  2. groups = SyncPermissionResolver.groupsFor({ userId })  // initial set
  3. If Last-Event-ID present, run an implicit catchup from that syncId
     and stream those events first.
  4. Subscribe to SyncEventBus.
  5. Also subscribe to MembershipChangedEvent on EventClient,
     filtered by userId === connection.userId. (See live refresh below.)

Per event from SyncEventBus:
  - Skip if event.clientId === connection.clientId.        # echo suppression
  - Skip if none of event.syncGroups matches connection.groups (with wildcards).
  - hydrated = hydrate(event, ctx)
  - If hydrated.data is null:                              # access lost
      emit synthetic { ...event, action: 'D', data: null }
    Else:
      emit hydrated
  - SSE push.

Per MembershipChangedEvent for this user:
  - connection.groups = SyncPermissionResolver.groupsFor({ userId })
```

Each connection holds an Effect `Scope`. Disposing the scope cleans up both
subscriptions.

### Live-connection refresh

Membership changes don't write to a `user_subscriptions` table. They emit a
`MembershipChangedEvent` (a domain event from the membership aggregate),
which the per-connection handler subscribes to:

```typescript
yield* events.handle(MembershipChangedEvent, (event) =>
  Effect.when(
    refreshGroups,
    () => event.userId === connection.userId,
  ))
```

`refreshGroups` re-runs the permission resolver and replaces the connection's
cached set. No DB writes, no denormalization, no propagation delay beyond
the time to publish on `EventClient`.

For ACL revocations to take effect promptly, the *projection* for the
membership aggregate should also emit a `RESYNC_GROUP` sync event for the
removed group, tagged with `user:<userId>`. This tells the client to clear
local data for that group. (Without it, the client would just stop receiving
new events for the group but keep stale data forever.)

### Squashing

For events ordered by `syncId`, group by `(modelName, modelId)` and fold:

| previous | next | result |
|----------|------|--------|
| (none)   | `I`  | `I`    |
| (none)   | `U`  | `U`    |
| (none)   | `D`  | `D`    |
| `I`      | `U`  | `I`    |
| `I`      | `D`  | (noop) |
| `U`      | `U`  | `U`    |
| `U`      | `D`  | `D`    |
| `D`      | `I`  | `U`    |
| any      | `R`  | `R`, drop preceding for this entity |

Coarser-grained resyncs override finer ones:
- `RESYNC_GROUP` for group X drops preceding events tagged with X.
- `RESYNC_ALL` drops everything.

The squasher is pure. Property-test it hard.

## 9. Resync events

Three variants, all written through `SyncEventDispatcher` like any other
event:

- **Per-model** (`modelName: '__model:Webhook'`, `modelId: <ignored>`): rare,
  for targeted recovery. Client clears its `Webhook` collection within the
  affected groups and rebootstraps from `/webhooks`.
- **Per-group** (`modelName: '__group'`, `modelId: <syncGroupId>`): primary
  use case is membership removal. Client clears all collections for entities
  in that group.
- **Global** (`modelName: '__all'`): client drops everything, rebootstraps.
  Returned by `/catchup` when `from` is older than retention; rarely written
  to the event log.

Resync events are emitted by:

- **Membership-change projection.** On removal, emit per-group resync tagged
  with `user:<removedUserId>` so it lands on that user's session.
- **Catchup endpoint.** Returns inline `__all` event when `from` is too old.
  Does not write to the log.
- **Admin tooling.** Manual data-correction flows can emit per-model or
  global resyncs to force clients to refresh.

### The epoch invariant (when resync events can't reach the client)

Every resync variant above arrives *as an event with a syncId* — which
assumes the invariant that syncIds are **durable and monotonic within one
epoch** (the identity of the event log's timeline). If the log's history is
destroyed or replaced — memory-store restart, table truncation, backup
restore, database migration — the sequence restarts and a resync event
minted at the new (small) syncId lands *below* a client's durable cursor,
so the client's monotonic guards silently drop it along with everything
else: the client freezes. The escape hatch is the optional
`CatchupResponse.epoch` (an opaque, server-minted timeline identity, not a
software version): backends that can't guarantee the invariant send it;
the client stores it durably and on mismatch self-heals by wiping its
local sync state (event log, watermarks, floors, cursor) and
re-bootstrapping every collection via `Snapshot`. Backends with a
genuinely durable log omit the field and nothing changes.

## 10. Echo suppression

Every client generates a `clientId` on first load. Every request
(including `/sync`) carries it as `X-Client-Id`. Every sync event records
the originator's `clientId`. The per-connection filter skips events where
`event.clientId === connection.clientId`.

This requires `clientId` on `DomainEvent` — see [clientId on DomainEvent](#clientid-on-domainevent).

## 11. Retention

```sql
DELETE FROM sync_events
WHERE created_at < now() - INTERVAL '7 days';
```

Run nightly. 7 days is a reasonable default. Measure
`catchup_from_too_old` rates and tune.

## 12. Failure modes

**Domain event published, projection handler fails.** The aggregate already
succeeded; the sync event is missing. Live clients miss it. Next catchup
heals it — *if* the projection eventually succeeds on retry. If the handler
crashes deterministically, the sync event is lost permanently and only a
manual resync recovers. Mitigation: projection handlers should be
trivially simple (no I/O beyond the dispatcher), use the dispatcher's own
retry semantics, and be heavily tested. This is the most important
correctness boundary in the system.

**Dispatcher writes the row, bus publish fails.** Row is in the DB. Live
clients miss the event. Catchup heals it. Acceptable — log a warning.

**Hydration fails for one event.** Log it, skip the event, continue. Next
event for that entity (or a future catchup) will heal it. Don't kill the
stream.

**Catchup hydration explosion.** A client offline for a week catching up
thousands of squashed events: use `hydrateMany`. If still too large, return
partial results with a continuation cursor, or refuse and force `RESYNC_ALL`.
Pick a threshold (e.g. > 10K squashed events) and document it.

**Membership change races a connection's event stream.** User receives an
event for a group they just lost access to, or misses one for a group they
just got. The `MembershipChangedEvent` refresh handles forward; the user
might briefly receive one stale event before refresh completes. The next
reconnect is fully consistent. Acceptable.

**Two domain events publish for the same entity in the same logical
transaction.** Each becomes a sync event. The squasher folds them on
catchup; live streams see both. Both are correct outcomes. If you need
strict "one sync event per transaction," fold at the projection layer (rare,
not recommended).

**`syncId` sequence gaps.** `BIGSERIAL` produces gaps on rollback. Clients
order by `syncId`, they don't require contiguity. Document and assert in
tests.

## 13. Workspace-scoped data

The backend serves three categories of data:

- **Global per-user.** Examples: the user's profile (`User`), the list of
  organizations they belong to (`OrganizationMembership`), user settings.
  One logical collection per type. The client mounts these once at login.
- **Workspace-scoped.** Examples: `Webhook`, `ImportRule`, `Member`,
  `Channel`. Owned by an organization. The client mounts these per workspace.
- **Workspace-scoped, background.** Same as above, but for workspaces the
  user belongs to without actively viewing.

This affects three places in the system: list endpoints, sync event
tagging, and what the client does with both.

### List endpoints are per-workspace

```
GET /organizations/:orgId/webhooks
GET /organizations/:orgId/import-rules
GET /organizations/:orgId/members
```

These are normal REST list endpoints. They authorize against the user's
membership in `:orgId` and return only that workspace's entities. The
sync system does not own them — they exist for the broader API already.

The client's per-workspace collection instance points its `queryFn` at the
relevant endpoint. Bootstrapping a collection = calling the endpoint =
populating the collection. Nothing sync-specific.

Global endpoints are unscoped:

```
GET /me
GET /me/organizations
GET /me/settings
```

### Sync event tagging encodes the workspace

Workspace-scoped projections tag events with the workspace's sync group:

```typescript
yield* events.handle(WebhookUpdatedEvent, (event) =>
  dispatcher.dispatch({
    modelName:  'Webhook',
    modelId:    event.webhookId,
    action:     'U',
    syncGroups: [`organization:${event.owner.id}`],   // workspace-scoped
    clientId:   event.clientId,
  }))
```

Global projections (user profile, org memberships) tag with the user's
personal group:

```typescript
yield* events.handle(UserUpdatedEvent, (event) =>
  dispatcher.dispatch({
    modelName:  'User',
    modelId:    event.userId,
    action:     'U',
    syncGroups: [`user:${event.userId}`],             // user-scoped
    clientId:   event.clientId,
  }))
```

Sync groups are the only thing in the event that ties it to a workspace.
There is no `workspaceId` column on `sync_events`; the workspace identity
is part of the sync-group string. The permission resolver and the
filter logic don't special-case workspaces — they're just one of several
group-shape patterns.

### Optional consolidated bootstrap endpoint

For workspaces with many entity types, one round trip per type is wasteful.
A single endpoint per workspace can return the full snapshot:

```
GET /organizations/:orgId/sync/bootstrap
→ {
    webhooks:    [...],
    importRules: [...],
    members:     [...],
    channels:    [...],
    syncId:      12345,
  }
```

The returned `syncId` is the `lastSyncId` at the moment of the snapshot,
captured in the same transaction as the entity reads. The client uses it
as the starting point for catchup.

**Do not build this on day one.** Use the individual list endpoints,
parallelize them on the client, and capture `lastSyncId` from a separate
cheap endpoint (or from a response header on the global bootstrap).
Add the consolidated endpoint only if bootstrap latency measurements
justify it.

The global tier has an analogous endpoint:

```
GET /me/sync/bootstrap
→ {
    user:          {...},
    organizations: [...],
    settings:      {...},
    syncId:        12345,
  }
```

### Catchup and `/sync` can be scoped by group

Both endpoints accept an optional `group` query parameter to narrow
delivery:

```
GET /catchup?from={syncId}                              # everything visible
GET /catchup?from={syncId}&group=organization:abc       # one workspace
GET /catchup?from={syncId}&group=organization:abc&group=organization:xyz
GET /sync?group=organization:abc                        # filter live stream
```

The server filters by the intersection of (the user's actual permissions)
and (the requested groups). Anything outside the user's permissions returns
nothing — the permission filter still wins.

The client uses this when it wants to scope work to specific workspaces
without disconnecting and reconnecting the SSE for global events. The
default (no `group` parameter) is "everything the user can see," which is
fine for users in a small number of workspaces. For power users in many
workspaces, scoping is a meaningful bandwidth win.

### `syncId` is still global, not per-workspace

Even though events are scoped, the `syncId` counter is global. The client
tracks one `lastSyncId` for the whole session. On workspace switch, the
client calls catchup with a group filter to fetch any missed events for
the newly active workspace; it doesn't need a per-workspace cursor.

This works because catchup is filtered server-side. If a client has been
ignoring org B's events for an hour and now switches to it, the
`/catchup?from={lastSyncId}&group=organization:B` call returns only the
events for B since `lastSyncId`. Other workspaces' events were already
seen on the live stream (and potentially ignored if they weren't mounted).

### Server-side: nothing changes

The projection layer doesn't care about workspaces. It tags events with
workspace-scoped sync groups, which the existing filter logic handles.
The catchup and stream endpoints gain the optional `group` filter but
otherwise behave identically.

The only addition is the optional consolidated bootstrap endpoint — and
even that's just an aggregation over existing list logic.

## 14. Workspace-scoped data on the client

> Informational for the backend team; implemented here in this repo.

### Two categories of collections

The client distinguishes:

- **Global collections.** One instance per type. Mounted once at login.
  Id is the plain entity name: `user`, `organizations`, `userSettings`.

- **Workspace-scoped collections.** One instance *per workspace*. Id is
  `<entity>:<orgId>`: `webhook:abc-org-id`, `member:abc-org-id`.

There is no separate "background mode" category — that's a per-collection
runtime property (active / warm / lazy), not a distinct kind of collection.

### Collection identity is the lifecycle key

Every TanStack DB collection has a string `id`. We use that id as a
**structured key** that encodes the collection's lifecycle scope:

```
webhook:abc-org-id      ← workspace-scoped, owned by org abc
member:abc-org-id       ← workspace-scoped, owned by org abc
channel:abc-org-id      ← workspace-scoped, owned by org abc

user                    ← global, no scope suffix
organizations           ← global
userSettings            ← global

webhook:xyz-org-id      ← different workspace, fully independent
```

Convention: `<entityName>:<scopeKey>` for scoped collections, plain
`<entityName>` for global ones. The registry never parses these — the
*factories* own the format. Because the scope is embedded in the id,
there is never a second field to keep consistent. A collection at
`webhook:abc-org-id` cannot accidentally point at a different org; the id
*is* the scope.

### The CollectionRegistry

A long-lived, **generic, untyped** cache. It does not know what a webhook
is. It stores collections by id and disposes them by id or by pattern.

```typescript
class CollectionRegistry {
  private collections = new Map<string, RegisteredCollection>()

  getOrCreate<T>(id: string, factory: () => T): T {
    const existing = this.collections.get(id)
    if (existing) return existing.collection as T
    const collection = factory()
    this.collections.set(id, { collection, dispose: () => /* teardown */ })
    return collection
  }

  getById(id: string): unknown | null {
    return this.collections.get(id)?.collection ?? null
  }

  dispose(id: string): void {
    this.collections.get(id)?.dispose()
    this.collections.delete(id)
  }

  // Dispose every collection whose id matches a glob. `*` matches any
  // run of non-`:` characters, so "*:abc-org-id" matches all entities
  // for org abc but not "user" and not "x:y:abc-org-id".
  disposePattern(pattern: string): void {
    const regex = globToRegex(pattern)
    for (const [id, entry] of this.collections) {
      if (regex.test(id)) {
        entry.dispose()
        this.collections.delete(id)
      }
    }
  }
}

const globToRegex = (pattern: string): RegExp => {
  const escaped = pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&')
  const withWildcards = escaped.replace(/\*/g, '[^:]*')
  return new RegExp(`^${withWildcards}$`)
}
```

The registry exists because TanStack DB collections are stateful objects
with identity. Components, the sync resolver, and mutation handlers all
need the *same* instance for a given id, never ad-hoc copies. The registry
hands out canonical instances and owns their teardown.

Crucially, the registry has no type-level knowledge of entities. Adding a
new workspace-scoped entity does not touch the registry at all.

### Per-entity collection factories

Each entity owns a factory function, co-located with the aggregate it
serves (`features/<entity>/sync/<entity>-collection.ts`). The factory is
the **typed entry point** — it knows the id format and the schema, and it
delegates instance management to the registry.

```typescript
// features/webhook/sync/webhook-collection.ts
import { collectionRegistry } from '~/features/sync/collection-registry'

export const createWebhookCollection = (args: { orgId: OrgId }) =>
  collectionRegistry.getOrCreate(
    `webhook:${args.orgId}`,
    () => createCollection(
      effectCollectionOptions({
        id:       `webhook:${args.orgId}`,
        schema:   WebhookResponse,
        runtime:  ClientRuntime,
        getKey:   w => w.id,
        syncMode: 'eager',
        queryFn:  () => Effect.gen(function* () {
          const client = yield* ClientApiV1
          return yield* client.organizations.webhooks.list({
            path: { orgId: args.orgId },
          })
        }),
        // onInsert / onUpdate / onDelete also use args.orgId in their paths
      }),
    ),
  )

// Global entity — no scope suffix:
export const createUserCollection = () =>
  collectionRegistry.getOrCreate(
    `user`,
    () => createCollection(effectCollectionOptions({ id: 'user', /* ... */ })),
  )
```

Callers never touch the registry or construct ids by hand:

```typescript
const webhooks = createWebhookCollection({ orgId })
// typed as Collection<WebhookResponse, WebhookId>, because that's what
// the factory's return type says. Full typing where it matters.
```

The registry stores `unknown` internally; the factory's signature restores
the type at the boundary. No caller ever sees the untyped layer.

Adding a workspace-scoped entity = write a `create<Entity>Collection`
factory + a persistence table + a dispatch handler (below). The registry,
the lifecycle helpers, and the resolver are untouched.

### Lifecycle helpers

Disposal is expressed as named helpers built on `disposePattern`, so UI
code never writes globs directly:

```typescript
// features/sync/collection-registry/lifecycle.ts
export const disposeWorkspace = (orgId: OrgId) =>
  collectionRegistry.disposePattern(`*:${orgId}`)

export const disposeAllWorkspaces = () =>
  collectionRegistry.disposePattern(`*:*`)   // scoped only; leaves globals

export const disposeEverything = () =>
  collectionRegistry.disposePattern(`*`)      // logout
```

- **Workspace switch** (with full teardown): `disposeWorkspace(oldOrgId)`.
- **Logout**: `disposeEverything()`.
- **Keeping a workspace warm**: just don't dispose it. The collection stays
  in the registry, in memory, receiving sync events.

### The sync dispatch registry

The resolver needs to route an incoming event to the right collection. It
does not import factories or know id conventions. Instead, each entity
registers a dispatch handler, co-located with its factory:

```typescript
// features/webhook/sync/webhook-collection.ts (continued)
import { syncDispatchRegistry } from '~/features/sync/dispatch-registry'

syncDispatchRegistry.register('Webhook', (event) => {
  const orgId = extractOrgFromGroups(event.syncGroups)
  const collection = collectionRegistry.getById(`webhook:${orgId}`)
  if (!collection) return Effect.void          // not mounted → ignore (policy)
  return event.action === 'D'
    ? collection.utils.deleteLocally(event.modelId)
    : collection.utils.updateLocally(event.data)
})
```

The dispatch registry is `Map<ModelName, DispatchHandler>`. The resolver
is then trivial and entity-agnostic:

```typescript
const dispatch = (event: HydratedSyncEvent) =>
  syncDispatchRegistry.get(event.modelName)?.(event) ?? Effect.void
```

Each `<entity>-collection.ts` becomes self-contained: factory + dispatch
handler + (optionally) its persistence hydration. The resolver, the
collection registry, and the dispatch registry are all generic
infrastructure that never changes when you add an entity.

### Mount lifecycle

There is no per-workspace "mount" object — collections are mounted
individually via their factories, on first request. "Mounting a workspace"
just means: some component or bootstrap step called the factories for that
workspace's collections, which registered them. "Unmounting a workspace"
means `disposeWorkspace(orgId)`, which disposes every `*:${orgId}`
collection in one pattern sweep.

A collection can be in one of three runtime modes, but the mode is a
property of the *collection*, not a workspace-wide setting:

**Active.** Hydrated from storage *and* refreshed via `queryFn`. Receiving
sync-dispatched updates. The default when a factory is called for a
collection the user is actively viewing.

**Warm.** In the registry, in memory, still receiving sync-dispatched
updates, but `queryFn` is skipped on (re)mount while data is fresh
(`staleTime`). The collection simply isn't disposed when the user
navigates away.

**Lazy.** In the registry, hydrated from storage only — no network refresh,
and the dispatch handler's "not mounted → ignore" policy is replaced with
"persist to storage." Rarely needed; start without it.

Start with two states — present (in the registry) and disposed — and add
nuance only when switch latency is measured to matter.

### Bootstrap flow

Cold start:

1. App loads, session established, local store opens.
2. Global collections hydrate from storage. UI can render the workspace
   switcher immediately from stale data.
3. Global `queryFn`s fire in parallel (`/me`, `/me/organizations`,
   `/me/settings`). Stale data is updated.
4. Active workspace determined (last used, or default).
5. The active workspace's screens call their collection factories. Each
   hydrates from storage filtered by `orgId`, then fires `queryFn` against
   `/organizations/:orgId/<entity>`. Capture the highest `lastSyncId` from
   the responses. (Or call the consolidated bootstrap endpoint once.)
6. UI for the active workspace renders.
7. `/sync` connection opens. Catchup from `lastSyncId` if there's a gap.
   Live events begin flowing.
8. *(Optional)* Other workspaces lazy-mounted in the background.

Warm start (return visit with populated storage):

1. App loads. Local store opens.
2. As above, but `queryFn`s are skipped if data is within `staleTime`.
   UI renders fresh from storage almost immediately.
3. `/sync` opens, catchup runs for events since the persisted `lastSyncId`.
   Usually a small delta.

Workspace switch (within an active session):

1. User picks new workspace in the switcher.
2. The new workspace's screens call their collection factories. If already
   in the registry (warm), `getOrCreate` returns them instantly; otherwise
   they're created and bootstrapped.
3. The previously active workspace is either left in the registry (warm) or
   torn down with `disposeWorkspace(oldOrgId)`.
4. No SSE reconnect required.

### Events for unmounted workspaces

When the sync resolver receives an event for a workspace not in the
registry, three policy options: **Ignore** (cheapest; next mount
bootstraps fresh), **Persist to storage only** (write through without a
collection; one write per event), **Auto-mount lazy**. Default: ignore.

## 15. Build order (backend)

Roughly two weeks. Don't reorder without thinking.

1. **Schema + migrations.** `sync_events`, indexes. `0.5d`
2. **`SyncEventBus` interface + in-memory Effect PubSub.** `0.5d`
3. **`SyncEventRepository`.** Append, query by groups, query by syncId. `1d`
4. **`SyncEventDispatcher`.** Thin service combining repo + bus. `0.5d`
5. **`clientId` on `DomainEvent` + propagation through `EventClient.publish`.**
   The invasive prerequisite. `1d`
6. **First projection: one entity, one domain event.** Verify the chain
   end-to-end. `1d`
7. **Squasher.** Pure function, property tests. `1d`
8. **`SyncPermissionResolver`.** `1d`
9. **Model registry + hydration for one entity** (`hydrateMany` from day
   one). `1d`
10. **`GET /catchup`.** `2d`
11. **`GET /sync` SSE.** Bus subscription, filtering, hydration, echo
    suppression. `2d`
12. **Live-connection refresh via `MembershipChangedEvent`.** `1d`
13. **Resync events.** All three variants. `1d`
14. **Property tests.** Random sequences; verify any client catching up
    from any `syncId` ends identical to the DB. `3d`
15. **Retention job.** `0.5d`
16. **Second and third entities.** `1d each`

## 16. What to settle before writing code

1. **`EventClient` API for per-publish metadata** (the `clientId` plumbing).
2. **The shape of `MembershipChangedEvent`** (or whatever signals permission
   changes). The live-refresh path subscribes to it.
3. **Where the permission resolver lives.** Probably its own module under
   `features/sync/`.
4. **Postgres deployment topology.** If behind PgBouncer in transaction-
   pooling mode, `LISTEN`/`NOTIFY` doesn't work — affects the production bus
   swap, not the in-memory MVP.

## 17. What's out of scope

- Cross-server scaling. Single-node first. `SyncEventBus` is the seam.
- WebSocket transport. SSE is sufficient.
- Schema migrations of entity tables. Use the existing migration tool.

---

## 22. Persistence decision (RESOLVED)

> Resolves the original spec's *"Persistence decision (hand off to Claude
> Code)"* hand-off. **Decided 2026-06-01** against the real codebase and the
> current upstream state of TanStack DB.

**Decision:** the new (greenfield) client builds persistence on TanStack DB
**0.6 `persistedCollectionOptions`** (SQLite‑WASM), **accepting its alpha
status**. We do **not** carry over the existing custom Dexie bridge, and we do
**not** build a fresh bespoke Dexie engine.

### Fixed constraints (unchanged from the spec)

- **Freshness metadata is ours.** Global `lastSyncId` gates catchup;
  per-collection `lastBootstrapAt` is ours to own if/when we need a time gate.
  Not the framework's `staleTime` clock (it resets on reload).
- **Catchup writes use the synced-store path**, never the optimistic-mutation
  path.
- **The factory is the only seam.** Whichever backend wins changes only the
  inside of `create<Entity>Collection`. Registry, dispatch, resolver, and
  bootstrap flow are unchanged. Blast radius of being wrong = one function per
  entity.

### Why this decision (and why it is version-dependent)

This was a close call. The reasoning is recorded because the right answer
changes as TanStack DB matures.

- **The installed `@tanstack/db@0.5.15` has no `persistedCollectionOptions`.**
  It ships only `localOnlyCollectionOptions` and `localStorageCollectionOptions`
  (verified in `node_modules`). So on the pinned version, the official path did
  not exist — which originally argued for a custom Dexie layer.
- **TanStack DB 0.6 shipped `persistedCollectionOptions` (first *alpha*).** It
  covers **synced** collections (wraps e.g. `queryCollectionOptions`), keeps the
  server authoritative, and implements the **persisted base + pending local
  delta** composition that issue #865 was scoping. Storage is **SQLite only**
  (SQLite‑WASM in the browser); the IndexedDB‑direct design was explicitly
  rejected upstream. Offline mutations come via `@tanstack/offline-transactions`.
- **Greenfield removes the only argument for keeping the old code** ("it already
  works, cleanup is mostly deletion"). With nothing to preserve and no users to
  risk, accepting an alpha is defensible — and the "factory is the only seam"
  constraint caps the downside at one function per entity.

### Why NOT the existing custom Dexie layer

The current impl (`apps/dashboard/app/lib/effect-collection/`) is
IndexedDB‑as‑source‑of‑truth. Its core mechanism is the liability:

- **Loads the whole table into memory and re‑scans it per change.**
  `performInitialSync` (`services/sync-service.ts:71`) loops until the table is
  fully in the in‑memory TanStack store; the reactive handler does
  `table.toArray()` on **every** liveQuery tick (`services/sync-service.ts:196`)
  and diffs a full snapshot — O(table size) per write.
- **Correctness rests on a Deferred + `refreshTrigger` + 2s ack‑timeout
  handshake** (`services/persistence-service.ts`) papering over liveQuery being
  coarse and non‑deterministic.
- **Heavy per‑collection bookkeeping** (`services/collection-state.ts`) exists
  only to support the above.

### Why NOT a fresh bespoke Dexie engine

Viable and dependency‑light, but means owning a sync/persistence engine
forever. Since 0.6 now provides the synced + persisted base+delta composition
officially, hand‑rolling it is redundant surface area.

### Caveats we are explicitly accepting

1. **Alpha churn/bugs** in the foundational layer. Pin the version; budget for
   upgrade friction. **Pinned version: `@tanstack/db@0.6.7`** (exact, no caret;
   set 2026-06-01 during workspace scaffold).
2. **SQLite‑WASM bundle/startup cost** (hundreds of KB to ~1MB+). Measure
   against the "render from local data fast" goal.
3. **Larger‑than‑memory datasets are NOT solved by 0.6.** See next section.

### Critical: the persistence backend does NOT give you "large collections"

A TanStack DB collection is an **in‑memory reactive store**. Its query / join /
live‑query engine runs over in‑memory data. Persistence — Dexie *or*
SQLite‑WASM — is a **durable base** that hydrates the in‑memory collection on
startup and is written through. The working set a collection serves to
`useLiveQuery` lives in memory in **both** designs.

Therefore:
- **Dexie vs SQLite is not the axis that decides large‑collection support.**
- The old client's "live subscription to Dexie" did **not** keep data off‑heap;
  it held the whole table in memory *and* full‑scanned it per change.
- Raw SQLite‑WASM can query off disk, but a `persistedCollectionOptions`
  collection still hydrates an in‑memory view. Larger‑than‑memory paging
  (row‑ vs page‑level) is explicitly **unsolved** in #865 as of the 0.6 alpha.

The lever for large data is **scoping the collection**, not the backend:
per‑workspace collections (`webhook:<orgId>`, §14) and windowed queries
(`.where(...).limit(...)`, re‑subscribed when the window moves). Build the
registry/scoping early — it is orthogonal to, and more important than, the
backend choice.

### Three-step load flow mapped onto 0.6

| Step | 0.6 `persistedCollectionOptions` |
|------|----------------------------------|
| **1. Load‑from‑storage on mount** | ✅ Native — its core feature. Hydrates from the SQLite base before any network. |
| **2. Skip‑`loadFn`‑if‑fresh (self‑owned `lastBootstrapAt`)** | ⚠️ Not a time‑based knob. 0.6 has `schemaVersion` (bump → clear + re‑sync), i.e. *structural* invalidation, not a staleness clock. Intent is served structurally: cold start = hydrate from SQLite + catchup from `lastSyncId`, so the full re‑list is already avoided. Keep a thin self‑owned `lastBootstrapAt` gate only if measurement shows `loadFn` runs too often. Treat `schemaVersion` as the migration/wipe trigger only. |
| **3. Catchup deltas via synced‑store write path** | ✅ If catchup events flow through the collection's **sync source**, they persist to the base automatically — no full refetch, never the optimistic path. |

---

## A. Client persistence — build plan (this repo)

Order matters; do not skip the spike.

1. **Spike on one collection (the gate).** Pick a small global entity (e.g.
   `user`). Implement with `persistedCollectionOptions` wrapping its synced
   config. Validate the three‑step flow end‑to‑end against the alpha:
   hydrate from SQLite on mount (no network) → UI renders; no full re‑list
   when a persisted base exists; catchup deltas land through the sync source
   and persist. If the alpha breaks any of these, fall back to a clean custom
   Dexie factory for that one collection and re‑evaluate. **Do not roll out
   before this passes.**
2. **The factory seam.** Define `create<Entity>Collection(...)` as the single
   typed entry point and the *only* place persistence lives.
3. **CollectionRegistry + scoping** (§14). A long-lived lifetime table keyed
   by structured `(entity, scope)`: `getOrCreate` plus selective disposal. It
   owns instance deduplication and child scopes, but exposes no routing or
   lookup surface. **Build early — scoping is how large collections stay small.**
4. **SyncBroker.** One model-blind broker owns the multiplexed SSE connection,
   global catchup, durable `lastSyncId`, event log, pruning, and resync. Each
   mounted collection subscribes itself and drains `Snapshot | Upsert | Delete`
   into its own synced store.
5. **Replay + live as one stream.** Subscribe to in-memory fan-out before
   reading durable history, then emit optional snapshot, replay rows, and the
   filtered live tail. Subscriber-owned, batched watermarks record completion.
6. **Decode and scope at the collection.** The broker logs opaque rows for all
   models. Collection drains decode with their model schema, filter upserts by
   `scopeOf`, and fan scope-less deletes to every scope of that model.
7. **Offline mutations.** Add `@tanstack/offline-transactions` once the read
   path is solid.
8. **Roll out per entity.** Each new entity is one `defineCollection` call;
   broker ingest and the lifetime table remain untouched.

## B. Relevant context from this repo

**Copy / model after (good):**
- `app/lib/effect-collection/services/bootstrap-service.ts` — self‑owned
  `lastBootstrapAt` freshness gate in a `_metadata` table. The spec's
  "freshness is ours" pattern. Reuse the idea if you keep step‑2 gating.
- `app/lib/sync/react/client-sync-catchup-service.ts` — global `lastSyncId` in
  localStorage + catchup fetch. Carry over largely as‑is.
- `app/lib/sync/react/client-sync-service.ts` & `client-sync-resolver.ts` —
  SSE stream decode (Effect `Stream`), event queue, `modelName → handler`
  dispatch, keep‑alive/retry. Keep the dispatch‑registry shape; point handlers
  at scoped collections via the registry.
- `app/lib/effect-collection/create-effect-collection.ts` — the
  factory‑as‑only‑seam shape and Effect/runtime integration. Keep the *shape*,
  replace the *internals* with `persistedCollectionOptions`.
- `app/features/*/collection/*-collection.ts` — existing per‑entity collection
  definitions (deployments, domains, templates, …): reference for schemas,
  `getKey`, bootstrap `list()` calls, `onInsert/onUpdate/onDelete` wiring.
- `app/features/api/v1/client/` — the typed API client used by `queryFn`s /
  sync sources.

**Anti‑reference (do NOT replicate):**
- `services/sync-service.ts` — whole‑table `toArray()` + snapshot diff per tick.
- `services/persistence-service.ts` — Deferred / `refreshTrigger` / ack‑timeout
  write handshake.
- `services/collection-state.ts` — heavy per‑collection bookkeeping propping up
  the above.

**Conventions to follow:**
- `CLAUDE.md` — Effect‑TS rules, typecheck‑after‑every‑change, testing
  framework selection (`@effect/vitest` for Effect code).
- `EFFECT.md` + `vendor/effect/` — Effect patterns; never guess APIs.
- `patterns/tanstack-collections-pattern.md`, `patterns/sync-events-pattern.md`
  — existing documented patterns; update once the new factory lands.
- Domain models only — never import Prisma types in feature code.

**Backend dependency to confirm before building the read path:**
- The catchup/stream API shape (`/sync`, `/catchup?from=…`, optional `group`
  filter) and the hydrated event payload (`HydratedSyncEvent`). Align the
  client sync source's decode against the server's response schemas.

## C. Upstream references

- TanStack DB 0.6 — persistence, offline support, includes:
  https://tanstack.com/blog/tanstack-db-0.6-app-ready-with-persistence-and-includes
- Issue #865 — Persistence of synced data (design thread; open):
  https://github.com/TanStack/db/issues/865
- LocalStorage collection docs:
  https://tanstack.com/db/latest/docs/collections/local-storage-collection
- RxDB collection docs:
  https://tanstack.com/db/latest/docs/collections/rxdb-collection
