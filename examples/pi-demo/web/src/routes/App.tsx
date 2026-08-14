import { ProjectId, TodoId } from "@pi-demo/shared"
import { SubsetStatus, useLiveSync, usePartialLoad } from "@triargos/live-collection-react"
import { useLiveQuery } from "@tanstack/react-db"
import { Option, Schema } from "effect"
import { useEffect, useState, type ReactNode } from "react"
import { Outlet, Route, Routes, useParams } from "react-router-dom"
import { Button } from "@/components/ui/button.js"
import { Sidebar } from "../components/Sidebar.js"
import { TodoList } from "../components/TodoList.js"
import type { AppBundle } from "../live/collections.js"
import { consumeCreatedSession } from "../live/session.js"

function Layout({ bundle }: { readonly bundle: AppBundle }) {
  return (
    <div className="min-h-screen lg:grid lg:grid-cols-[19rem_minmax(0,1fr)]">
      <Sidebar bundle={bundle} />
      <main className="min-w-0"><Outlet /></main>
    </div>
  )
}

function StatusCard({ emoji, title, body, children }: {
  readonly emoji: string
  readonly title: string
  readonly body: string
  readonly children?: ReactNode
}) {
  return (
    <div className="grid min-h-[60vh] place-items-center p-6 text-center">
      <div>
        <div className="mb-3 text-5xl">{emoji}</div>
        <strong className="text-xl font-bold">{title}</strong>
        <p className="mx-auto mt-2 max-w-sm font-normal text-muted-foreground">{body}</p>
        {children !== undefined && <div className="mt-4">{children}</div>}
      </div>
    </div>
  )
}

/**
 * One project's quests. `usePartialLoad` is the ensure: first-ever visit issues one
 * `POST /api/sync/batch`; every later visit (and page reload) resolves locally from
 * the journal — watch the network tab stay silent.
 */
function ProjectQuests({ bundle, projectId }: { readonly bundle: AppBundle; readonly projectId: ProjectId }) {
  const todos = bundle.todosCollection()
  const subset = usePartialLoad(todos, { projectId })

  return SubsetStatus.$match(subset, {
    // Persisted rows are already on screen while the ensure runs — stale-while-revalidate.
    Loading: () => <TodoList bundle={bundle} projectId={Option.some(projectId)} syncing />,
    Ready: () => <TodoList bundle={bundle} projectId={Option.some(projectId)} />,
    Forbidden: () => (
      <StatusCard
        body="The server refused visibility for this project — a Forbidden subset, distinct from an empty one."
        emoji="🔒"
        title="This quest line is not yours."
      />
    ),
    Failed: ({ retry }) => (
      <StatusCard body="The /sync/batch fetch failed. Your local rows are untouched." emoji="📡" title="Could not reach the quest server.">
        <Button onClick={retry} type="button">Retry</Button>
      </StatusCard>
    ),
  })
}

function ProjectTodos({ bundle }: { readonly bundle: AppBundle }) {
  const { projectId } = useParams()
  const decoded = Schema.decodeUnknownOption(ProjectId)(projectId)

  if (Option.isNone(decoded)) {
    return <StatusCard body="Choose one from the quest board." emoji="🗺️" title="Unknown quest." />
  }

  return <ProjectQuests bundle={bundle} projectId={decoded.value} />
}

/**
 * The “All quests” view needs every project's subset — the completeness contract of a
 * partial collection: a query is only complete for subsets you have ensured. All the
 * ensures fire in one microtask window, so they coalesce into a single
 * `POST /api/sync/batch` no matter how many projects exist.
 */
function AllQuests({ bundle }: { readonly bundle: AppBundle }) {
  const todos = bundle.todosCollection()
  const projects = bundle.projectsCollection(bundle.session)
  const { data: projectRows } = useLiveQuery((q) => q.from({ project: projects }))
  const idsKey = projectRows.map((project) => project.id).join(",")
  const [syncedKey, setSyncedKey] = useState<string | undefined>(undefined)

  useEffect(() => {
    if (idsKey === "") {
      setSyncedKey(idsKey)
      return
    }
    let cancelled = false
    const ids = idsKey.split(",").map((id) => ProjectId.make(id))
    void Promise.all(ids.map((id) => todos.utils.loadByProjectId(id))).then(
      () => {
        if (!cancelled) setSyncedKey(idsKey)
      },
      () => {
        // Demo policy: show whatever subsets did load; the next visit retries.
        if (!cancelled) setSyncedKey(idsKey)
      },
    )
    return () => {
      cancelled = true
    }
  }, [todos, idsKey])

  return <TodoList bundle={bundle} projectId={Option.none()} syncing={syncedKey !== idsKey} />
}

export function App({ bundle }: { readonly bundle: AppBundle }) {
  useLiveSync(bundle.runtime)

  useEffect(() => {
    if (!consumeCreatedSession(bundle.session)) return

    const projects = bundle.projectsCollection(bundle.session)
    const todos = bundle.todosCollection()
    const projectId = ProjectId.make(crypto.randomUUID())
    const createdAt = new Date().toISOString()

    projects.insert({
      id: projectId,
      sessionId: bundle.session,
      name: "First adventure",
      color: "#8b5cf6",
      createdAt,
    })
    todos.insert({
      id: TodoId.make(crypto.randomUUID()),
      sessionId: bundle.session,
      projectId,
      title: "Invite a teammate with the party code",
      completed: false,
      createdAt,
    })
    todos.insert({
      id: TodoId.make(crypto.randomUUID()),
      sessionId: bundle.session,
      projectId,
      title: "Complete a quest and watch every screen update",
      completed: false,
      createdAt,
    })
  }, [bundle])

  return (
    <Routes>
      <Route element={<Layout bundle={bundle} />}>
        <Route index element={<AllQuests bundle={bundle} />} />
        <Route path="p/:projectId" element={<ProjectTodos bundle={bundle} />} />
      </Route>
    </Routes>
  )
}
