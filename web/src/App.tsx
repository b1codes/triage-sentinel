import { useCallback, useEffect, useState } from 'react'
import { Route, Routes } from 'react-router'
import { useQueryClient } from '@tanstack/react-query'
import { getSession, login, logout } from './lib/api'
import { useSentinelStream } from './lib/sse'
import { Layout } from './layout'
import { OverviewView } from './views/overview'
import { IncidentView } from './views/incident'
import { ProjectsView } from './views/projects'
import { ComingSoon } from './views/stub'

function LoginForm({ onSuccess }: { onSuccess: () => void }) {
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await login(password)
      onSuccess()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Login failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="centered">
      <h1>triage-sentinel</h1>
      <p className="sub">Sign in to continue.</p>
      <form className="login" onSubmit={submit}>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Dashboard password"
          autoFocus
          aria-label="Dashboard password"
        />
        <button type="submit" disabled={busy}>
          {busy ? 'Signing in…' : 'Sign in'}
        </button>
      </form>
      {error && <p className="error">{error}</p>}
    </main>
  )
}

export default function App() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null)
  const queryClient = useQueryClient()
  const streamState = useSentinelStream(queryClient)

  const check = useCallback(() => {
    getSession()
      .then((s) => setAuthenticated(s.authenticated))
      .catch(() => setAuthenticated(false))
  }, [])

  useEffect(check, [check])

  if (authenticated === null) {
    return (
      <main className="centered">
        <p className="sub">Loading…</p>
      </main>
    )
  }
  if (!authenticated) {
    return <LoginForm onSuccess={() => setAuthenticated(true)} />
  }

  return (
    <Routes>
      <Route
        element={
          <Layout
            streamState={streamState}
            onSignOut={() => {
              void logout().then(() => setAuthenticated(false))
            }}
          />
        }
      >
        <Route index element={<OverviewView />} />
        <Route path="incidents/:id" element={<IncidentView />} />
        <Route path="projects" element={<ProjectsView />} />
        <Route path="spend" element={<ComingSoon view="Spend" milestone="M2" />} />
        <Route path="parked" element={<ComingSoon view="Parked" milestone="M2" />} />
        <Route path="audit" element={<ComingSoon view="Audit" milestone="M5" />} />
        <Route path="*" element={<ComingSoon view="Not found" milestone="" />} />
      </Route>
    </Routes>
  )
}
