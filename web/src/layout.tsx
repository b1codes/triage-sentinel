import { NavLink, Outlet } from 'react-router'
import type { StreamState } from './lib/sse'

const NAV = [
  { to: '/', label: 'Overview', end: true },
  { to: '/projects', label: 'Projects' },
  { to: '/spend', label: 'Spend' },
  { to: '/parked', label: 'Parked' },
  { to: '/audit', label: 'Audit' },
]

export function Layout({
  streamState,
  onSignOut,
}: {
  streamState: StreamState
  onSignOut: () => void
}) {
  return (
    <div className="shell">
      <header>
        <span className="brand">triage-sentinel</span>
        <nav>
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => (isActive ? 'active' : undefined)}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
        <span className={`stream stream-${streamState}`} title={`Stream ${streamState}`}>
          {streamState}
        </span>
        <button type="button" onClick={onSignOut}>
          Sign out
        </button>
      </header>
      <main>
        <Outlet />
      </main>
    </div>
  )
}
