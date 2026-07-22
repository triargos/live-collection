import { ProjectId, TodoId } from "@pi-demo/shared"
import { useLiveSync } from "@triargos/live-collection-react"
import { Option, Schema } from "effect"
import { useEffect } from "react"
import { Outlet, Route, Routes, useParams } from "react-router-dom"
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

function ProjectTodos({ bundle }: { readonly bundle: AppBundle }) {
  const { projectId } = useParams()
  const decoded = Schema.decodeUnknownOption(ProjectId)(projectId)

  if (Option.isNone(decoded)) {
    return (
      <div className="grid min-h-[60vh] place-items-center p-6 text-center">
        <div><div className="mb-3 text-5xl">🗺️</div><strong className="text-xl font-bold">Unknown quest.</strong><p className="font-normal text-muted-foreground">Choose one from the quest board.</p></div>
      </div>
    )
  }

  return <TodoList bundle={bundle} projectId={decoded} />
}

export function App({ bundle }: { readonly bundle: AppBundle }) {
  useLiveSync(bundle.runtime)

  useEffect(() => {
    if (!consumeCreatedSession(bundle.session)) return

    const projects = bundle.projectsCollection(bundle.session)
    const todos = bundle.todosCollection(bundle.session)
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
        <Route index element={<TodoList bundle={bundle} projectId={Option.none()} />} />
        <Route path="p/:projectId" element={<ProjectTodos bundle={bundle} />} />
      </Route>
    </Routes>
  )
}
