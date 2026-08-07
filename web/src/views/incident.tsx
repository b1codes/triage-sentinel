import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from 'react-router'
import { getIncident, relativeTime } from '../lib/api'

export function IncidentView() {
  const { id } = useParams<{ id: string }>()
  const incidentID = Number(id)

  const query = useQuery({
    queryKey: ['incidents', incidentID],
    queryFn: () => getIncident(incidentID),
    enabled: Number.isFinite(incidentID),
  })

  if (query.isPending) return <p className="sub">Loading…</p>
  if (query.isError) return <p className="error">{(query.error as Error).message}</p>

  const incident = query.data

  return (
    <section className="detail">
      <Link to="/" className="back">
        ← Overview
      </Link>

      <h2>{incident.title}</h2>
      <p className="sub">
        <span className={`state state-${incident.state}`}>{incident.state}</span>
        {incident.state_reason && ` · ${incident.state_reason}`}
        {' · '}
        {incident.project_slug || <em>unroutable</em>} · {incident.source} · {incident.kind}
        {incident.occurrence_count > 1 && ` · seen ${incident.occurrence_count} times`}
      </p>

      {incident.fingerprint_strategy && (
        <div className="panel">
          <h3>Grouping</h3>
          <p className="sub">
            Strategy <code>{incident.fingerprint_strategy}</code>
            {incident.fingerprint_strategy === 'all_frames' && (
              <>
                {' '}
                — no project source frames were identified. Declaring{' '}
                <code>fingerprint.source_roots</code> for this project would group it more
                precisely.
              </>
            )}
          </p>
          {incident.fingerprint_frames && incident.fingerprint_frames.length > 0 && (
            <ol className="frames">
              {incident.fingerprint_frames.map((frame, index) => (
                <li key={`${frame}-${index}`}>
                  <code>{frame}</code>
                </li>
              ))}
            </ol>
          )}
        </div>
      )}

      {incident.body && (
        <div className="panel">
          <h3>Payload</h3>
          <pre>{incident.body}</pre>
        </div>
      )}

      <div className="panel">
        <h3>Timeline</h3>
        <ol className="timeline">
          {incident.events.map((event) => (
            <li key={event.id}>
              <span className="when">{relativeTime(event.ts)}</span>
              <span className="what">
                {event.kind === 'state_change'
                  ? `${event.from_state || '∅'} → ${event.to_state}`
                  : event.kind}
              </span>
              <span className="who">{event.actor}</span>
            </li>
          ))}
        </ol>
      </div>

      {incident.metadata && Object.keys(incident.metadata).length > 0 && (
        <div className="panel">
          <h3>Metadata</h3>
          <dl className="kv">
            {Object.entries(incident.metadata).map(([key, value]) => (
              <div key={key}>
                <dt>{key}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
        </div>
      )}
    </section>
  )
}
