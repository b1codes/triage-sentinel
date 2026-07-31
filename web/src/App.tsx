import { useCallback, useEffect, useState } from 'react'
import {
  formatBytes,
  getHealth,
  getSession,
  login,
  logout,
  type HealthResponse,
} from './api'

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
    <>
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
    </>
  )
}

function Metric({ label, value, tone }: { label: string; value: string; tone?: 'ok' | 'bad' }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd className={tone}>{value}</dd>
    </div>
  )
}

function Dashboard({ onSignOut }: { onSignOut: () => void }) {
  const [health, setHealth] = useState<HealthResponse | null>(null)
  const [streamState, setStreamState] = useState<'connecting' | 'live' | 'closed'>('connecting')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false

    const refresh = () =>
      getHealth()
        .then((h) => {
          if (!cancelled) {
            setHealth(h)
            setError(null)
          }
        })
        .catch((err: unknown) => {
          if (!cancelled) setError(err instanceof Error ? err.message : 'Health check failed')
        })

    void refresh()
    const timer = window.setInterval(refresh, 5000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [])

  // M0 proves the SSE transport end to end. Event handling per topic arrives
  // with the incident feed in M1.
  useEffect(() => {
    const source = new EventSource('/api/stream?topics=incidents,runs,budget')
    source.onopen = () => setStreamState('live')
    source.onerror = () => setStreamState('closed')
    return () => source.close()
  }, [])

  const degraded = health?.status !== 'ok'

  return (
    <>
      <h1>triage-sentinel</h1>
      <p className="sub">
        {health ? `v${health.version}` : 'loading…'} · stream {streamState}
      </p>

      {health && (
        <dl className="metrics">
          <Metric label="Status" value={health.status} tone={degraded ? 'bad' : 'ok'} />
          <Metric label="Uptime" value={`${Math.floor(health.uptime_seconds / 60)}m`} />
          <Metric label="Resident memory" value={formatBytes(health.rss_bytes)} />
          <Metric label="Free memory" value={formatBytes(health.free_ram_bytes)} />
          <Metric label="Free disk" value={formatBytes(health.free_disk_bytes)} />
          <Metric label="Database" value={formatBytes(health.db_size_bytes)} />
          <Metric label="Schema" value={String(health.schema_version)} />
          <Metric label="Projects" value={String(health.projects)} />
          <Metric label="Goroutines" value={String(health.goroutines)} />
          <Metric label="Stream clients" value={String(health.sse_clients)} />
        </dl>
      )}

      {health?.problems?.map((problem) => (
        <p className="error" key={problem}>
          {problem}
        </p>
      ))}
      {error && <p className="error">{error}</p>}

      <footer>
        <button
          type="button"
          onClick={() => {
            void logout().then(onSignOut)
          }}
        >
          Sign out
        </button>
      </footer>
    </>
  )
}

export default function App() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null)

  const check = useCallback(() => {
    getSession()
      .then((s) => setAuthenticated(s.authenticated))
      .catch(() => setAuthenticated(false))
  }, [])

  useEffect(check, [check])

  if (authenticated === null) {
    return (
      <main>
        <p className="sub">Loading…</p>
      </main>
    )
  }

  return (
    <main>
      {authenticated ? (
        <Dashboard onSignOut={() => setAuthenticated(false)} />
      ) : (
        <LoginForm onSuccess={() => setAuthenticated(true)} />
      )}
    </main>
  )
}
