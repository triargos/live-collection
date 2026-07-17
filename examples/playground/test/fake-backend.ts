import { Context, Duration, Effect, Layer, ManagedRuntime, Queue } from "effect"
import { CatchupClient, EventLogStore, LastSyncIdStore, SyncTransport } from "@triargos/live-collection"
import {
  type CatchupResponse,
  type HydratedSyncEventEnvelope,
  ModelId,
  ModelName,
  SyncGroup,
  SyncId,
} from "@triargos/live-collection-protocol"
import { WebhookApi } from "../src/live/shared-backend.js"
import type { Webhook } from "../src/live/schema.js"

/**
 * An **in-memory** adapter for the real {@link WebhookApi} tag — the test/dev seam behind the demo's
 * cross-tab `makeSharedBackend`. It keeps state in this process (a `Map` + an event log), so the browser
 * write-path test can prove OPFS persistence across a reload *purely from disk*: a fresh backend here has
 * an empty log and replays nothing, unlike the cross-tab backend whose shared localStorage log would mask
 * whether OPFS actually persisted. Same four seams a real backend serves (mutations + catchup/SSE/list).
 */
export interface FakeBackend {
  readonly services: ManagedRuntime.ManagedRuntime<WebhookApi, never>
  readonly sync: Layer.Layer<SyncTransport | CatchupClient | LastSyncIdStore | EventLogStore>
}

const GROUP = SyncGroup.make("playground")

export const makeFakeBackend = (config?: {
  readonly delays?: { readonly tail?: Duration.Input; readonly list?: Duration.Input }
  readonly seed?: ReadonlyArray<Webhook>
}): FakeBackend => {
  const tail = Duration.fromInputUnsafe(config?.delays?.tail ?? Duration.millis(30))
  const list = Duration.fromInputUnsafe(config?.delays?.list ?? Duration.millis(50))

  const rows = new Map<string, Webhook>((config?.seed ?? []).map((w) => [w.id, w]))
  const log: Array<HydratedSyncEventEnvelope> = []
  const queue = Effect.runSync(Queue.unbounded<HydratedSyncEventEnvelope>())
  let seq = 0
  const nextId = (): SyncId => SyncId.make(String(++seq))

  const broadcast = (env: HydratedSyncEventEnvelope): void => {
    log.push(env)
    Effect.runSync(Queue.offer(queue, env))
  }

  const api: Context.Service.Shape<typeof WebhookApi> = {
    create: (w) =>
      Effect.sleep(tail).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            rows.set(w.id, w)
            broadcast({
              _tag: "Insert",
              syncId: nextId(),
              modelName: ModelName.make("Webhook"),
              modelId: ModelId.make(w.id),
              syncGroups: [GROUP],
              createdAt: new Date(),
              data: w,
            })
          }),
        ),
        Effect.as(w),
      ),
    remove: (id) =>
      Effect.sleep(tail).pipe(
        Effect.flatMap(() =>
          Effect.sync(() => {
            rows.delete(id)
            broadcast({
              _tag: "Delete",
              syncId: nextId(),
              modelName: ModelName.make("Webhook"),
              modelId: id,
              syncGroups: [GROUP],
              createdAt: new Date(),
            })
          }),
        ),
      ),
    list: (orgId) =>
      Effect.sleep(list).pipe(Effect.as(Array.from(rows.values()).filter((w) => w.orgId === orgId))),
  }

  const services = ManagedRuntime.make(Layer.succeed(WebhookApi, api))

  const catchup = Layer.succeed(CatchupClient, {
    fetch: ({ from }) =>
      Effect.sleep(tail).pipe(
        Effect.as<CatchupResponse>({
          events: log.filter((e) => Number(e.syncId) > Number(from)),
          lastSyncId: SyncId.make(String(seq)),
        }),
      ),
  })

  const sync = Layer.mergeAll(
    LastSyncIdStore.layerMemory,
    catchup,
    SyncTransport.layerMemory(queue),
    EventLogStore.layerMemory,
  )

  return { services, sync }
}
