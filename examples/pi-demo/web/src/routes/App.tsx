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
    <div className="app-shell">
      <Sidebar bundle={bundle} />
      <main className="content"><Outlet /></main>
    </div>
  )
}

function ProjectTodos({ bundle }: { readonly bundle: AppBundle }) {
  const { projectId } = useParams()
  const decoded = Schema.decodeUnknownOption(ProjectId)(projectId)

  if (Option.isNone(decoded)) {
    return <div className="empty-state"><strong>Unknown project.</strong><span>Choose one from the sidebar.</span></div>
  }

  return <TodoList bundle={bundle} projectId={decoded} />
}

export function App({ bundle }: { readonly bundle: AppBundle }) {
  useLiveSync(bundle.runtime, bundle.models)

  useEffect(() => {
    if (!consumeCreatedSession(bundle.session)) return

    const projects = bundle.projectsCollection(bundle.session)
    const todos = bundle.todosCollection(bundle.session)
    const projectId = ProjectId.make(crypto.randomUUID())
    const createdAt = new Date().toISOString()

    projects.insert({
      id: projectId,
      sessionId: bundle.session,
      name: "Getting started",
      color: "#8b5cf6",
      createdAt,
    })
    todos.insert({
      id: TodoId.make(crypto.randomUUID()),
      sessionId: bundle.session,
      projectId,
      title: "Share this session code with another device",
      completed: false,
      createdAt,
    })
    todos.insert({
      id: TodoId.make(crypto.randomUUID()),
      sessionId: bundle.session,
      projectId,
      title: "Watch updates arrive live",
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
