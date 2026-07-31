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
  problems?: string[]
}

export interface SessionResponse {
  authenticated: boolean
}

async function getJSON<T>(path: string, alsoAcceptable: number[] = []): Promise<T> {
  const res = await fetch(path, { credentials: 'same-origin' })
  if (!res.ok && !alsoAcceptable.includes(res.status)) {
    throw new Error(`${path} returned ${res.status}`)
  }
  return (await res.json()) as T
}

// /api/health deliberately responds 503 with a full HealthResponse body
// (status "degraded", populated problems) when a check fails, so 503 is a
// valid, parseable response here rather than an error.
export const getHealth = () => getJSON<HealthResponse>('/api/health', [503])
export const getSession = () => getJSON<SessionResponse>('/api/session')

export async function login(password: string): Promise<void> {
  const res = await fetch('/api/login', {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password }),
  })
  if (!res.ok) {
    throw new Error(res.status === 401 ? 'Incorrect password' : `Login failed (${res.status})`)
  }
}

export async function logout(): Promise<void> {
  await fetch('/api/logout', { method: 'POST', credentials: 'same-origin' })
}

export function formatBytes(bytes: number): string {
  if (bytes <= 0) return '—'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let value = bytes
  let unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit++
  }
  return `${value.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`
}
