import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router'
import { getIncidents, getOverview, relativeTime } from '../lib/api'
import type { IncidentSummary } from '../lib/api'

const STATE_ORDER = ['received', 'triaging', 'suppressed', 'filtered', 'escalated', 'failed']

function StateTile({ label, value, tone }: { label: string; value: number; tone?: string }) {
  return (
    <div className={`tile ${tone ?? ''}`}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  )
}

function IncidentRow({ incident }: { incident: IncidentSummary }) {
  return (
    <li>
      <Link to={`/incidents/${incident.id}`} className={`row state-${incident.state}`}>
        <span className="state">{incident.state}</span>
        <span className="title">{incident.title}</span>
        <span className="meta">
          {incident.project_slug || <em>unroutable</em>} · {incident.source}
          {incident.occurrence_count > 1 && ` · ×${incident.occurrence_count}`}
        </span>
        <span className="when">{relativeTime(incident.created_at)}</span>
      </Link>
    </li>
  )
}

export function OverviewView() {
  const overview = useQuery({ queryKey: ['overview'], queryFn: getOverview })
  const feed = useQuery({
    queryKey: ['incidents', { limit: 50 }],
    queryFn: () => getIncidents({ limit: 50 }),
  })

  if (overview.isError) {
    return <p className="error">{(overview.error as Error).message}</p>
  }

  const counts = overview.data?.incidents_by_state ?? {}

  return (
    <section>
      {overview.data?.ingest_stale && (
        <p className="banner error">
          No events received since{' '}
          {overview.data.last_ingest_at ? relativeTime(overview.data.last_ingest_at) : 'startup'}.
          The subscriber may be stalled.
        </p>
      )}

      <dl className="tiles">
        {STATE_ORDER.filter((state) => counts[state]).map((state) => (
          <StateTile key={state} label={state} value={counts[state] ?? 0} />
        ))}
        <StateTile label="projects" value={overview.data?.projects ?? 0} />
      </dl>

      {(overview.data?.awaiting_triage ?? 0) > 0 && (
        <p className="note">
          {overview.data?.awaiting_triage} incident(s) awaiting classification. Tier 1 arrives in
          M2 — nothing is processing these yet.
        </p>
      )}

      <h2>Live feed</h2>
      {feed.isPending && <p className="sub">Loading…</p>}
      {feed.isError && <p className="error">{(feed.error as Error).message}</p>}
      {feed.data?.incidents.length === 0 && (
        <p className="sub">No incidents yet. The system is watching.</p>
      )}
      <ul className="feed">
        {feed.data?.incidents.map((incident) => (
          <IncidentRow key={incident.id} incident={incident} />
        ))}
      </ul>
      {feed.data && feed.data.total > feed.data.incidents.length && (
        <p className="sub">
          Showing {feed.data.incidents.length} of {feed.data.total}.
        </p>
      )}
    </section>
  )
}
