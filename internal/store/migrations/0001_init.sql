-- SPEC §5. Forward-only: never edit this file after it has been applied
-- anywhere; add a new numbered migration instead.

CREATE TABLE projects (
  slug                 TEXT PRIMARY KEY,
  repo                 TEXT NOT NULL,
  default_branch       TEXT NOT NULL,
  active               INTEGER NOT NULL DEFAULT 1,
  quarantined          INTEGER NOT NULL DEFAULT 0,
  quarantine_reason    TEXT,
  quarantined_at       TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  incidents_seen       INTEGER NOT NULL DEFAULT 0,
  created_at           TEXT NOT NULL,
  updated_at           TEXT NOT NULL
);

CREATE TABLE incidents (
  id               INTEGER PRIMARY KEY,
  project_slug     TEXT REFERENCES projects(slug),
  source           TEXT NOT NULL,
  source_ref       TEXT NOT NULL,
  kind             TEXT NOT NULL,
  fingerprint      TEXT,
  title            TEXT NOT NULL,
  body             TEXT,
  metadata_json    TEXT NOT NULL DEFAULT '{}',
  state            TEXT NOT NULL,
  state_reason     TEXT,
  tier             INTEGER NOT NULL DEFAULT 0,
  confidence       REAL,
  category         TEXT,
  occurrence_count INTEGER NOT NULL DEFAULT 1,
  cost_usd         REAL NOT NULL DEFAULT 0,
  occurred_at      TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  closed_at        TEXT
);
CREATE UNIQUE INDEX incidents_source_ref ON incidents(source, source_ref);
CREATE INDEX incidents_state            ON incidents(state, updated_at DESC);
CREATE INDEX incidents_project          ON incidents(project_slug, created_at DESC);
CREATE INDEX incidents_fingerprint      ON incidents(fingerprint, created_at DESC);

-- Append-only. Audit trail and SSE replay log.
CREATE TABLE incident_events (
  id           INTEGER PRIMARY KEY,
  incident_id  INTEGER NOT NULL REFERENCES incidents(id),
  ts           TEXT NOT NULL,
  kind         TEXT NOT NULL,
  actor        TEXT NOT NULL,
  from_state   TEXT,
  to_state     TEXT,
  payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX incident_events_incident ON incident_events(incident_id, id);

CREATE TABLE fingerprints (
  fingerprint       TEXT PRIMARY KEY,
  project_slug      TEXT NOT NULL REFERENCES projects(slug),
  first_incident_id INTEGER NOT NULL REFERENCES incidents(id),
  last_seen_at      TEXT NOT NULL,
  suppress_until    TEXT NOT NULL,
  total_occurrences INTEGER NOT NULL DEFAULT 1,
  repair_attempts   INTEGER NOT NULL DEFAULT 0,
  total_cost_usd    REAL NOT NULL DEFAULT 0
);

CREATE TABLE agent_runs (
  id                 TEXT PRIMARY KEY,
  incident_id        INTEGER NOT NULL REFERENCES incidents(id),
  tier               INTEGER NOT NULL,
  runner             TEXT NOT NULL,
  model              TEXT NOT NULL,
  pid                INTEGER,
  pgid               INTEGER,
  state              TEXT NOT NULL,
  workspace_path     TEXT,
  base_sha           TEXT,
  log_path           TEXT,
  input_tokens       INTEGER NOT NULL DEFAULT 0,
  output_tokens      INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens  INTEGER NOT NULL DEFAULT 0,
  cache_write_tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd           REAL NOT NULL DEFAULT 0,
  turns              INTEGER NOT NULL DEFAULT 0,
  exit_code          INTEGER,
  abort_reason       TEXT,
  abort_requested_at TEXT,
  killed_at          TEXT,
  started_at         TEXT NOT NULL,
  ended_at           TEXT
);
CREATE INDEX agent_runs_incident ON agent_runs(incident_id, started_at DESC);
CREATE INDEX agent_runs_live     ON agent_runs(state) WHERE state = 'running';

-- Append-only source of truth for all spend.
CREATE TABLE budget_ledger (
  id                 INTEGER PRIMARY KEY,
  ts                 TEXT NOT NULL,
  project_slug       TEXT REFERENCES projects(slug),
  incident_id        INTEGER REFERENCES incidents(id),
  run_id             TEXT REFERENCES agent_runs(id),
  tier               INTEGER NOT NULL,
  model              TEXT NOT NULL,
  input_tokens       INTEGER NOT NULL DEFAULT 0,
  output_tokens      INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens  INTEGER NOT NULL DEFAULT 0,
  cache_write_tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd           REAL NOT NULL
);
CREATE INDEX budget_ledger_ts ON budget_ledger(ts);

-- Materialized counters. Rebuildable from budget_ledger.
CREATE TABLE budget_windows (
  scope               TEXT NOT NULL,
  scope_id            TEXT,
  kind                TEXT NOT NULL,
  window_start        TEXT NOT NULL,
  window_end          TEXT NOT NULL,
  spend_usd           REAL NOT NULL DEFAULT 0,
  tier1_calls         INTEGER NOT NULL DEFAULT 0,
  tier2_runs          INTEGER NOT NULL DEFAULT 0,
  incidents_opened    INTEGER NOT NULL DEFAULT 0,
  incidents_resolved  INTEGER NOT NULL DEFAULT 0,
  incidents_escalated INTEGER NOT NULL DEFAULT 0,
  incidents_parked    INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (scope, scope_id, kind, window_start)
);

-- One row per threshold crossing. Prevents alert spam.
CREATE TABLE budget_alerts (
  id           INTEGER PRIMARY KEY,
  scope        TEXT NOT NULL,
  scope_id     TEXT,
  kind         TEXT NOT NULL,
  window_start TEXT NOT NULL,
  threshold    TEXT NOT NULL,
  fired_at     TEXT NOT NULL,
  digest_json  TEXT NOT NULL,
  cleared_at   TEXT
);
CREATE UNIQUE INDEX budget_alerts_once
  ON budget_alerts(scope, scope_id, kind, window_start, threshold);

CREATE TABLE patches (
  id               INTEGER PRIMARY KEY,
  incident_id      INTEGER NOT NULL REFERENCES incidents(id),
  run_id           TEXT NOT NULL REFERENCES agent_runs(id),
  branch           TEXT NOT NULL,
  base_sha         TEXT NOT NULL,
  head_sha         TEXT,
  files_changed    INTEGER NOT NULL DEFAULT 0,
  lines_added      INTEGER NOT NULL DEFAULT 0,
  lines_removed    INTEGER NOT NULL DEFAULT 0,
  diff_path        TEXT,
  pr_url           TEXT,
  pr_number        INTEGER,
  state            TEXT NOT NULL,
  applied_autonomy TEXT NOT NULL,
  downgrade_reason TEXT,
  verified_at      TEXT,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL
);
CREATE INDEX patches_incident ON patches(incident_id, created_at DESC);

CREATE TABLE settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT
);

CREATE TABLE ingest_cursor (
  source       TEXT PRIMARY KEY,
  last_seen_at TEXT NOT NULL,
  state_json   TEXT NOT NULL DEFAULT '{}'
);
