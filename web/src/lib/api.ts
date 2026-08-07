export type IncidentState =
  | 'received' | 'triaging' | 'filtered' | 'suppressed'
  | 'dismissed' | 'parked' | 'escalated' | 'failed'

export interface IncidentSummary {
  id: number
  project_slug: string
  source: string
  kind: string
  title: string
  state: IncidentState
  state_reason?: string
  fingerprint?: string
  occurrence_count: number
  occurred_at: string
  created_at: string
}

export interface TimelineEntry {
  id: number
  ts: string
  kind: string
  actor: string
  from_state?: string
  to_state?: string
  payload?: Record<string, unknown>
}

export interface IncidentDetail extends IncidentSummary {
  body?: string
  metadata?: Record<string, string>
  fingerprint_strategy?: string
  fingerprint_frames?: string[]
  events: TimelineEntry[]
  updated_at: string
  closed_at?: string
}

export interface IncidentPage {
  incidents: IncidentSummary[]
  total: number
  limit: number
  offset: number
}

export interface Overview {
  incidents_by_state: Record<string, number>
  projects: number
  last_ingest_at?: string
  ingest_stale: boolean
  awaiting_triage: number
}

export interface ProjectSummary {
  slug: string
  repo: string
  default_branch: string
  active: boolean
  quarantined: boolean
  quarantine_reason?: string
  incidents: number
}

export interface HealthResponse {
  status: string
  version: string
  uptime_seconds: number
  goroutines: number
  rss_bytes: number
  free_ram_bytes: number
  free_disk_bytes: number
  db_size_bytes: number
  schema_version: number
  sse_clients: number
  projects: number
  last_ingest_at?: string
  ingest_stale: boolean
  problems?: string[]
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, { credentials: 'same-origin', ...init })

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`
    try {
      const body = (await response.json()) as { error?: string }
      if (body.error) message = body.error
    } catch {
      // A non-JSON error body is not worth failing over; the status line is
      // already a usable message.
    }
    throw new Error(message)
  }
  return (await response.json()) as T
}

export const getOverview = () => request<Overview>('/api/overview')
export const getProjects = () => request<{ projects: ProjectSummary[] }>('/api/projects')
export const getHealth = () => request<HealthResponse>('/api/health')
export const getIncident = (id: number) => request<IncidentDetail>(`/api/incidents/${id}`)

export function getIncidents(params: { state?: string; project?: string; limit?: number } = {}) {
  const query = new URLSearchParams()
  if (params.state) query.set('state', params.state)
  if (params.project) query.set('project', params.project)
  if (params.limit) query.set('limit', String(params.limit))

  const suffix = query.toString() ? `?${query.toString()}` : ''
  return request<IncidentPage>(`/api/incidents${suffix}`)
}

export const getSession = () => request<{ authenticated: boolean }>('/api/session')

export const login = (password: string) =>
  request<{ authenticated: boolean }>('/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password }),
  })

export const logout = () => request<unknown>('/api/logout', { method: 'POST' })

export function formatBytes(bytes: number): string {
  if (bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  return `${(bytes / 1024 ** exponent).toFixed(exponent === 0 ? 0 : 1)} ${units[exponent]}`
}

export function relativeTime(iso: string): string {
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`
  return `${Math.floor(seconds / 86400)}d ago`
}
