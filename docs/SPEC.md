# triage-sentinel — Technical Specification

**Status:** Draft v1 · **Date:** 2026-07-26 · **Derived from:** [`CONCEPT.md`](CONCEPT.md)

---

## 1. Purpose & Scope

`triage-sentinel` is a self-hosted, always-on control plane that monitors a portfolio of 25+
micro-applications, triages incoming failure signals through progressively more expensive
tiers, and — where policy permits — repairs them autonomously via an LLM coding agent.

It runs as a single Go binary on a dedicated M2 Mac Mini (8 GB RAM) and is published as one
public, 12-factor-compliant repository containing no private infrastructure detail.

### 1.1 Goals

1. **Near-zero daily oversight.** Routine failures are diagnosed and repaired without a human.
2. **Bounded, predictable spend.** No incident, project, day, week, or month can exceed a
   configured ceiling. Spend is attributable to the incident that caused it.
3. **Small resident footprint.** 15–25 MB steady-state RSS for the control plane; worker
   memory is reclaimed in full on completion.
4. **Verifiable safety.** Every autonomous change is independently verified by the control
   plane, is auditable after the fact, and can be halted instantly.
5. **Human-in-the-loop by design, not by exception.** When the system is out of its depth or
   over budget, it produces an actionable worklist rather than either flailing or going silent.

### 1.2 Non-goals (v1)

- Multi-user access control, RBAC, or public internet exposure of the dashboard.
- Managing repositories outside the operator's own GitHub account/organisations.
- Executing target-app test suites in a sandbox isolated from the host OS.
- Cross-repository or coordinated multi-repo changes within one incident.
- Deployment orchestration beyond invoking a project's own declared deploy path.

### 1.3 Design constraints

| Constraint | Consequence |
|---|---|
| 8 GB RAM shared with the OS | Control plane is Go; workers are ephemeral; `max_concurrent_agents` defaults to 1; a free-RAM floor gates spawning. |
| Host behind NAT, no public IP | All ingress is **outbound-initiated** (Pub/Sub pull). No tunnel daemon, no inbound ports. |
| Host may reboot or sleep | Ingress must be a durable queue, not a fire-and-forget webhook. Events survive downtime. |
| Public repository | All identifiers, filters, budgets, and secrets live in `.env` and `projects.yaml`, both gitignored, with committed `.example` counterparts. |
| Single-node, single-operator | SQLite over a networked database; shared-secret auth over OAuth; no HA. |

---

## 2. Architecture

```
                 ┌───────────────────────────────────────────────────────────┐
                 │  GCP                                                      │
                 │                                                           │
   GitHub ──────▶│  Cloud Run relay ──┐                                      │
   webhooks      │  (HMAC verify)     │                                      │
                 │                    ▼                                      │
   Cloud Logging─┤             Pub/Sub topic ── subscription (pull)           │
   sinks         │                                        ▲                  │
                 └────────────────────────────────────────┼──────────────────┘
                                                          │ outbound pull only
┌─────────────────────────────────────────────────────────┼──────────────────┐
│  Mac Mini — sentinel (single Go binary, ~15–25 MB RSS)   │                  │
│                                                          │                  │
│  ingest ──▶ triage(T0/T1) ──▶ budget ──▶ orchestrator ───┘                  │
│                │                              │                             │
│                ▼                              ▼                             │
│           SQLite (WAL)               workspace (git worktrees)               │
│                │                              │                             │
│                │                              ▼                             │
│                │                     runner ──▶ agent subprocess ──┐        │
│                │                              │   (ephemeral)      │        │
│                │                              ▼                    ▼        │
│                │                     verify ──▶ policy ──▶ forge  var/runs/ │
│                │                                        └─▶ notify  *.jsonl │
│                ▼                                                            │
│           bus (SSE) ──▶ httpapi ──▶ embedded React SPA                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Planes

**Control plane (Go, always on).** Owns ingestion, triage decisions, state, cost accounting,
policy, process supervision, and the dashboard. Never calls an LLM for anything other than
Tier 1 classification.

**Execution plane (ephemeral subprocess).** A Claude Agent SDK harness spawned per repair,
which reads the repository, edits files, runs the project's tests, and iterates. Killed on
completion; 100% of its memory returns to the OS.

### 2.2 Rejected alternatives

| Alternative | Why rejected |
|---|---|
| Python workers for Tier 1 (per `CONCEPT.md`) | A single classification call needs no agent loop; a 150–250 MB process to make one HTTP request is pure overhead. Tier 1 is in-process Go. |
| Pure-Go Tier 2 via the SDK Tool Runner | Requires hand-writing `read`/`write`/`edit`/`bash`/`grep` and the test-fix-retry loop — reimplementing a hardened harness with none of the hardening. |
| Anthropic Managed Agents for Tier 2 | Tool execution happens in a cloud container that knows nothing about 25 apps' local dependencies (GCP credentials, local services, `node_modules`, virtualenvs). Reproducing those environments is a larger project than this one. Deferred behind the `Runner` interface (§4.7). |
| Cloudflare Tunnel + direct webhooks | Two ingestion paths, an extra always-on daemon, and events silently lost while the host is down. |
| Rolling 7/30-day budget windows | The purpose of the weekly window is human planning cadence; you schedule a Thursday evening, not a rolling 168-hour interval. |

---

## 3. Incident Lifecycle

```
                        ┌─────────────┐
   Pub/Sub message ────▶│  received   │
                        └──────┬──────┘
                               │ Tier 0
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌──────────┐    ┌────────────┐   ┌────────────┐
        │ filtered │    │ suppressed │   │  triaging  │
        └──────────┘    └────────────┘   └─────┬──────┘
         (noise, dup)    (fingerprint          │ Tier 1
                          within window)       │
                          ┌──────────┬─────────┼──────────┐
                          ▼          ▼         ▼          ▼
                    ┌──────────┐ ┌────────┐ ┌──────┐ ┌───────────┐
                    │dismissed │ │ parked │ │failed│ │ repairing │
                    └──────────┘ └───┬────┘ └──────┘ └─────┬─────┘
                    (no code change)  │ manual release      │ Tier 2
                                      └────────────────────▶│
                                                            ▼
                                                     ┌─────────────┐
                                                     │  verifying  │──▶ failed
                                                     └──────┬──────┘
                                                            ▼
                                                     ┌─────────────┐
                                                     │  proposed   │
                                                     └──────┬──────┘
                        ┌───────────┬───────────┬───────────┼──────────┐
                        ▼           ▼           ▼           ▼          ▼
                   ┌────────┐ ┌──────────┐ ┌─────────┐ ┌─────────┐ ┌───────┐
                   │ merged │ │ rejected │ │ aborted │ │escalated│ │deployed│
                   └────────┘ └──────────┘ └─────────┘ └─────────┘ └───────┘
```

States are terminal unless noted: `filtered`, `suppressed`, `dismissed`, `merged`,
`deployed`, `rejected`, `aborted`, `failed`, `escalated`. `parked` is non-terminal and
resumable. Every transition appends one row to `incident_events`, which serves as both the
audit trail and the SSE replay log — there is no separate audit mechanism to keep in sync.

**Terminal-state semantics.** `escalated` means the system stopped and a human is required
(circuit breaker open, repeated failure, agent gave up, refusal). `failed` means an internal
error (git failure, subprocess crash, malformed agent output) — retryable by an operator.
`aborted` means a human pressed the button.

---

## 4. Module Specifications

Package layout under `internal/`. No package imports `httpapi`; `httpapi` imports everything.
`store` is imported widely but imports nothing but `config`.

### 4.1 `config` — configuration & registry

Loads `.env` (via `os.Getenv`, no dotenv library in production; `--env-file` for local dev)
and `projects.yaml`. Validates on load and **refuses to start** on any error — a
half-valid registry is worse than no registry.

Validation rules:
- Every project has a unique `slug` matching `^[a-z0-9][a-z0-9-]{1,38}[a-z0-9]$`.
- `repo` parses as `github.com/<owner>/<name>`.
- Every declared command is non-empty; `test` is mandatory.
- `autonomy` ∈ `{pr_only, auto_merge, auto_deploy}`; `auto_deploy` additionally requires a
  `commands.deploy` entry.
- Budget values are positive; every soft threshold is strictly less than its hard counterpart.
- Model IDs are checked against a known-model table (§7.2) and rejected with a clear error if
  unrecognised, rather than discovered as a 404 at spend time.

`SIGHUP` triggers a reload. A reload that fails validation logs loudly and **keeps the previous
configuration in effect**. Removing a project from the registry does not delete its incident
history; the `projects` table retains the row with `active = 0`.

### 4.2 `ingest` — ingestion router

A streaming-pull Pub/Sub subscriber plus one adapter per source. Adapters implement:

```go
type Adapter interface {
    // Match reports whether this adapter owns the message.
    Match(attrs map[string]string) bool
    // Normalize converts a raw message into a canonical event, or returns
    // ErrIgnore for messages that are structurally valid but uninteresting.
    Normalize(ctx context.Context, m *pubsub.Message) (Event, error)
}
```

```go
type Event struct {
    Source      string    // "github" | "gcplog"
    Kind        string    // "workflow_run.failed" | "issues.opened" | "log.error" | ...
    SourceRef   string    // stable per-source identity; unique with Source
    ProjectSlug string    // resolved via registry; empty ⇒ unroutable
    Title       string
    Body        string    // stack trace, log payload, issue body
    Metadata    map[string]string
    OccurredAt  time.Time
}
```

**Delivery semantics.** The subscriber acknowledges a message **immediately after the event is
durably written to SQLite**, not after it is processed. Processing happens asynchronously from
the database. This keeps the ack deadline irrelevant to repair duration (which can be minutes)
and makes crash recovery trivial: unprocessed rows are simply picked up on restart.

Pub/Sub is at-least-once, so **idempotency comes from the unique index on
`incidents(source, source_ref)`**, not from the transport. A duplicate delivery is an
`INSERT … ON CONFLICT DO UPDATE SET occurrence_count = occurrence_count + 1`.

**Unroutable events** (no matching project) are recorded in `incidents` with
`project_slug = NULL` and state `filtered`, reason `unroutable`, and surfaced in the dashboard.
They are never silently dropped — an unroutable event usually means a stale `projects.yaml`.

#### 4.2.1 GitHub path

GitHub cannot reach the host, so a small **Cloud Run relay** (in `deploy/relay/`, ~60 lines)
receives webhooks and republishes them to Pub/Sub. Defence in depth:

1. The relay verifies the `X-Hub-Signature-256` HMAC using a secret from Secret Manager and
   rejects on mismatch. This prevents the relay from becoming an open publish endpoint.
2. The relay publishes the **raw request body** (base64) plus the original headers as message
   attributes.
3. `ingest` **re-verifies** the HMAC from the forwarded signature header against the secret in
   `.env`. A compromised relay cannot inject events.

v1 subscribes to `workflow_run` (conclusion `failure`), `issues` (`opened`, `labeled`), and
`issue_comment` (for `@sentinel` commands — deferred to M5).

`SourceRef` values: `workflow_run:<run_id>`, `issue:<repo>#<number>`.

#### 4.2.2 GCP Cloud Logging path

Each project declares a log filter. A per-project logging sink routes matches to the shared
Pub/Sub topic — sinks are created by `deploy/gcp/` scripts, not by the binary, so the binary
needs no GCP admin credentials.

`SourceRef` is `gcplog:<insertId>`, globally unique per log entry. Because a crash loop emits
thousands of unique `insertId`s, `SourceRef` deduplication is insufficient here; **fingerprint
suppression (§4.3.2) is what prevents an error storm from becoming an invoice**.

### 4.3 `triage` — Tier 0 and Tier 1

#### 4.3.1 Tier 0 — local, $0.00

Ordered, short-circuiting, pure functions over an `Event`. No I/O except the final
build/lint check.

| Filter | Rejects |
|---|---|
| `Unroutable` | No matching project in the registry. |
| `Quarantined` | Project is quarantined by a breaker. |
| `Transient` | Configurable regex set: network timeouts, `ECONNRESET`, rate limits, runner-infrastructure failures, cancelled jobs. |
| `SelfInflicted` | Commits authored by the sentinel's own bot identity — prevents self-referential repair loops. |
| `Fingerprint` | Fingerprint seen within its suppression window (§4.3.2). |
| `Duplicate` | `(source, source_ref)` already present. |
| `BuildSanity` | Optional per-project `commands.healthcheck` fails for an environmental reason (missing dependency, not a code defect). |

Tier 0 must be cheap and total. Each filter is table-driven-tested with recorded real-world
payloads under `testdata/`.

#### 4.3.2 Fingerprinting

```
fingerprint = sha256(project_slug, error_class, normalize(top_n_frames))
```

`normalize` strips absolute paths, line numbers, memory addresses, UUIDs, and timestamps;
`top_n_frames` defaults to 5 and only considers frames within the project's own source tree,
so a shared dependency's stack does not collapse unrelated bugs together.

The first event for a fingerprint opens an incident and starts a **suppression window**
(default 6 h, per-project override). Subsequent events for the same fingerprint within the
window increment `incidents.occurrence_count` and append an `incident_events` row, and cost
nothing. The dashboard shows occurrence rate, so a storm is *visible* while being *silent*.

#### 4.3.3 Tier 1 — classification, ~$0.002

One in-process call to `claude-haiku-4-5` via `github.com/anthropics/anthropic-sdk-go`, using
strict structured output:

```jsonc
{
  "needs_code_change": true,
  "confidence": 0.0,          // 0..1
  "category": "logic_error",  // logic_error | config | dependency | flaky_test |
                              // infrastructure | not_a_defect | insufficient_context
  "suspected_paths": ["..."], // relative paths, may be empty
  "one_line_summary": "...",
  "estimated_difficulty": "low" // low | medium | high
}
```

The schema is declared with `strict: true`, `additionalProperties: false`, and a full
`required` list, so the response is guaranteed parseable and no defensive JSON repair is
needed.

Implementation notes that are **not** optional:

- **Do not send an `effort` parameter.** `claude-haiku-4-5` rejects it. Effort is an Opus/Sonnet
  control only.
- **Do not expect prompt caching to help.** Haiku 4.5's minimum cacheable prefix is 4096
  tokens; a triage rubric below that silently will not cache. Verify with
  `usage.cache_read_input_tokens` before assuming a saving exists.
- **Handle `stop_reason == "refusal"`.** Claude 4+ models can decline a request and return
  HTTP 200 with an empty or partial `content` array. Reading `content[0]` unconditionally will
  panic on a stack trace from a security-adjacent tool. A refusal transitions the incident to
  `escalated` with reason `classifier_refusal`; it is never treated as "no code change needed".
- **Pre-flight with `count_tokens`.** A pathological log payload is truncated to a configured
  ceiling (default 40 000 tokens) with a marker before the call, so an oversized input cannot
  produce a surprise charge.

`needs_code_change = false` → `dismissed`. `category ∈ {infrastructure, not_a_defect}` →
`dismissed`. `insufficient_context` → `escalated`. Anything else with
`confidence >= tier2_min_confidence` (default 0.5) proceeds to the budget gate.

### 4.4 `budget` — accounting & circuit breakers

The single authority on whether spending may occur. Exposes:

```go
type Decision struct {
    Allow      bool
    Verdict    Verdict // allow | park | escalate | halt
    Window     string  // the most-restrictive window that produced the verdict
    Reason     string
}

func (b *Budget) Check(ctx context.Context, projectSlug string, tier Tier) (Decision, error)
func (b *Budget) Record(ctx context.Context, e LedgerEntry) error
```

`Check` evaluates **all** windows and returns the most restrictive verdict. It is called before
every Tier 1 call and every Tier 2 spawn. `Record` is called after every LLM interaction —
including partial and aborted runs, because a killed run still cost money.

`budget_ledger` is append-only and is the source of truth. `budget_windows` holds materialized
counters so `Check` is O(1) and can be called on the hot path without scanning the ledger.
A `sentinel budget reconcile` command rebuilds `budget_windows` from the ledger and is run by
a nightly job; a drift between the two is logged as an error.

See §7 for the full budget model.

### 4.5 `workspace` — git isolation

One **mirror clone** per project at `var/repos/<slug>.git`, refreshed with `git remote update
--prune` before each run. Each repair gets a throwaway checkout:

```
git -C var/repos/<slug>.git worktree add --detach var/work/<run_id> <base_sha>
```

Worktrees are cheap, mutually isolated, and safe to run in parallel. Cleanup is
`git worktree remove --force` plus a `git worktree prune`. A startup sweep removes orphaned
worktrees from runs that did not survive a crash, and a disk-usage guard refuses to create a
worktree below a configured free-space floor.

`base_sha` is pinned at spawn time and recorded on the run, so a diff is always attributable to
a known base even if the branch moves during the repair.

### 4.6 `verify` — independent verification

**The agent's claim that tests pass is not evidence.** After a Tier 2 run reports success,
`verify` re-runs the project's declared `commands.test` in the produced worktree, from a clean
environment, with a timeout, capturing output.

- Exit 0 → proceed to `policy`.
- Non-zero → `escalated`, reason `verification_failed`, with both the agent's transcript and
  the verification output attached. This is the highest-signal escalation the system produces:
  the agent believed it was done and was wrong.
- Timeout → `escalated`, reason `verification_timeout`.

This costs zero tokens and is the single cheapest guardrail in the system. It also detects a
whole class of agent failure — tests modified or skipped to force a pass — because `verify`
additionally rejects a diff that deletes or skips tests unless
`allow_test_changes` is set for the project.

### 4.7 `runner` — worker process manager

```go
type Runner interface {
    Run(ctx context.Context, spec RunSpec, sink EventSink) (RunResult, error)
    Abort(runID string, reason string) error
}
```

v1 ships `agentsdk.Runner`. The interface exists so a `managedagents.Runner` (§2.2) can be
added as a milestone rather than a rewrite.

#### 4.7.1 Spawning

```go
cmd := exec.CommandContext(ctx, workerBin, args...)
cmd.Dir = worktreePath
cmd.Env = minimalEnv(project)          // explicit allowlist, never os.Environ()
cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
```

`Setpgid` is **load-bearing**. A coding agent spawns `node`, `npm`, `pytest`, and test servers.
Killing only the agent's PID orphans those children, which then hold RAM and ports on an 8 GB
host indefinitely. All signalling targets the process group (`-pgid`).

The environment is an explicit allowlist (`PATH`, `HOME`, `LANG`, the Anthropic API key, and
per-project variables declared in the registry). `os.Environ()` is never inherited, so the
control plane's secrets — GitHub token, Slack webhook, dashboard password — are not visible to
agent-authored code.

#### 4.7.2 Stream parsing & incremental cost

The worker emits newline-delimited JSON on stdout. The runner:

1. Appends every raw line verbatim to `var/runs/<run_id>.jsonl` — the durable record.
2. Parses each line; unrecognised event types are logged and skipped, never fatal, so a worker
   version bump degrades to reduced telemetry rather than a crashed repair.
3. On each usage-bearing event, accumulates cost from the price table (§7.2):

   ```
   Δ = in·rate_in + out·rate_out + cache_read·0.10·rate_in + cache_write·1.25·rate_in
   ```

4. If the running total crosses `max_cost_per_incident_usd`, kills the process group
   immediately and records `abort_reason = cost_cap`.
5. Forwards a compacted event to the SSE bus.

**Checking cost only at exit is not a cap, it is a receipt.** Incremental accumulation is what
makes the $1.00 ceiling real.

Stderr is captured to the same `.jsonl` as `{"type":"stderr","text":"..."}` records so a
crashed worker's diagnostics survive.

#### 4.7.3 Termination ladder

| Trigger | Behaviour |
|---|---|
| Normal completion | Worker exits; runner reaps; result parsed from final event. |
| Cost cap | Immediate `SIGKILL` to `-pgid`. No grace period — the money is already spent. |
| Wall-clock timeout (default 20 min) | `SIGTERM` to `-pgid`, 10 s grace, then `SIGKILL`. |
| Operator abort | Immediate `SIGKILL` to `-pgid`, synchronously in the HTTP handler. |
| Control-plane shutdown | `SIGTERM` to all groups, 15 s grace, `SIGKILL`, mark runs `aborted`. |

`abort_requested_at` and `killed_at` are both persisted, and the dashboard displays the measured
delta. The `<2 ms` figure from `CONCEPT.md` is presented as a measurement, never as an
unverified claim.

#### 4.7.4 Worker invocation — implementation-time verification required

The Claude Agent SDK's exact option names (turn limits, permission modes, allowed tools,
output format) **must be verified against <https://code.claude.com/docs/en/agent-sdk> at
implementation time**. This spec deliberately does not enumerate them, because inventing
plausible-looking flags produces code that fails at runtime.

Requirements the invocation must satisfy, however it is spelled:

- **API-key authentication, not subscription authentication.** Per-run cost is only
  attributable with API-key billing.
- A turn ceiling, as a second independent brake alongside the cost cap.
- A tool allowlist scoped to file operations, the project's declared commands, and git —
  no network access beyond the model API, and no ability to push or open PRs. **Delivery is
  the control plane's job** (§4.9), so a compromised or confused agent cannot publish anything.
- Machine-readable streaming output on stdout.
- Working directory pinned to the run's worktree.

If a future runner uses the raw Messages API instead of the Agent SDK, the equivalent pacing
control is `output_config.task_budget` (`{"type":"tokens","total":N}`, minimum 20 000, beta
header `task-budgets-2026-03-13`), which makes the model aware of its remaining budget so it
finishes gracefully rather than being cut off mid-edit.

### 4.8 `policy` — autonomy gates

Per-project autonomy from the registry, evaluated only after `verify` passes:

| Level | Behaviour |
|---|---|
| `pr_only` | Push branch, open PR, request review, notify. Nothing merges without a human. |
| `auto_merge` | Push, open PR, wait for required checks, merge, notify after the fact. |
| `auto_deploy` | As `auto_merge`, then invoke `commands.deploy` and poll `commands.healthcheck`. A failing healthcheck triggers `commands.rollback` if declared, and escalates either way. |

Policy is **downgraded, never upgraded**, by any of: a soft budget threshold in effect *when
`soft_mode.force_pr_only` is enabled* (off by default, since `park_tier2` already stops new
Tier 2 work — this rule only affects runs still in flight when soft mode engages), the
global kill switch, a diff touching a path in the project's `protected_paths` (default:
CI configuration, dependency lockfiles, migrations, secrets, infrastructure-as-code), a diff
exceeding `max_diff_lines`, or the project being newly registered and still inside its
`probation_incidents` count. Downgrade is always to `pr_only`, and the reason is recorded on
the patch and shown in the PR body.

### 4.9 `forge` — GitHub delivery

Branch naming: `sentinel/<incident_id>-<slug-of-summary>`. Commits are authored by a dedicated
bot identity whose email is matched by the Tier 0 `SelfInflicted` filter, closing the
self-repair loop.

PR bodies are generated by the control plane from structured run data — incident link, root
cause as classified, verification output, token and cost breakdown, transcript link, and any
policy downgrade with its reason. The agent does not write the PR description, because the PR
description is the human's primary review artifact and must be trustworthy.

All GitHub calls go through a shared client with rate-limit awareness (respecting
`X-RateLimit-Remaining` and `Retry-After`) and idempotency: re-running delivery for an incident
finds and updates the existing PR rather than opening a second one.

### 4.10 `notify` — escalation channels

```go
type Notifier interface {
    Name() string
    Notify(ctx context.Context, n Notification) error
}
```

v1 implementations, all fan-out with per-notifier error isolation (one failing channel never
blocks another, and never fails the incident):

- **`dashboard`** — always on. Writes to the `needs_human` view; cannot be disabled.
- **`github`** — comments the diagnostic state on the originating issue or PR. Reuses the token
  already required, adds no new secret, and rides GitHub's existing notification setup.
- **`slack`** — incoming webhook. Chosen over email for v1 because SMTP from a residential IP is
  unreliable and an email API adds a second vendor. An `email` notifier is a drop-in second
  implementation of this interface when wanted.

Notification kinds: `escalation`, `budget_soft`, `budget_hard`, `budget_efficiency`,
`budget_forecast`, `quarantine`, `pr_ready`, `merged`, `deploy_failed`, `ingest_stalled`.

### 4.11 `bus` — SSE engine

A hub with one bounded channel per subscriber.

```go
type Hub struct { /* … */ }
func (h *Hub) Subscribe(topics []string, lastEventID int64) (*Client, error)
func (h *Hub) Publish(topic string, ev Event)
```

Behaviour that matters on a memory-constrained host:

- **Bounded buffers, drop-oldest.** A subscriber with a full buffer (default 256 events) has its
  oldest events dropped and receives a single `resync` event instructing the client to refetch
  current state over HTTP. The server never grows an unbounded buffer for a slow browser tab.
- **Heartbeat** comment every 15 s to keep intermediaries and browsers from closing an idle
  stream.
- **Replay** via `Last-Event-ID`, served from `incident_events.id`, so a reconnecting tab does
  not miss transitions. Log lines are *not* replayed — the client refetches the tail of the
  `.jsonl` over HTTP instead, because replaying a firehose defeats the purpose.
- **Topics:** `incidents`, `runs`, `budget`, `logs:<run_id>`. A client subscribes to a
  log topic only while the corresponding view is open.

Log streaming reads from the `.jsonl` file, not from memory, so a viewer attaching mid-run
gets full history and the server holds no per-run log buffer.

### 4.12 `orchestrator` — the state machine

Owns the queue, the concurrency semaphore, and tier escalation. A single goroutine drains a
work queue backed by the `incidents` table (not an in-memory channel), so a restart resumes
exactly where it left off.

Pre-spawn gates, in order — every one of which can defer or reject:

1. Global kill switch.
2. `budget.Check` verdict.
3. Project not quarantined.
4. Concurrency semaphore (`max_concurrent_agents`, default 1).
5. **Free-RAM floor** (default 2 GB available). Below it, the spawn is requeued with
   exponential backoff. On darwin, availability is read via `vm_stat`/`sysctl`, not by
   assuming.
6. Free-disk floor for the worktree.

---

## 5. Data Model

SQLite via `modernc.org/sqlite` — pure Go, so the binary builds with `CGO_ENABLED=0` and ships
as a single static artifact. Its performance is irrelevant at a few hundred writes per day.

Pragmas at open: `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`,
`busy_timeout=5000`. One writer connection (`SetMaxOpenConns(1)` on a dedicated write handle)
plus a separate read pool — this eliminates `SQLITE_BUSY` by construction rather than by retry.

Migrations are numbered, embedded via `//go:embed`, forward-only, and applied in a transaction
at startup.

```sql
CREATE TABLE projects (
  slug            TEXT PRIMARY KEY,
  repo            TEXT NOT NULL,
  default_branch  TEXT NOT NULL,
  active          INTEGER NOT NULL DEFAULT 1,
  quarantined     INTEGER NOT NULL DEFAULT 0,
  quarantine_reason TEXT,
  quarantined_at  TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  incidents_seen  INTEGER NOT NULL DEFAULT 0,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL
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
CREATE INDEX incidents_state       ON incidents(state, updated_at DESC);
CREATE INDEX incidents_project     ON incidents(project_slug, created_at DESC);
CREATE INDEX incidents_fingerprint ON incidents(fingerprint, created_at DESC);

-- Append-only. Audit trail and SSE replay log.
CREATE TABLE incident_events (
  id           INTEGER PRIMARY KEY,
  incident_id  INTEGER NOT NULL REFERENCES incidents(id),
  ts           TEXT NOT NULL,
  kind         TEXT NOT NULL,   -- state_change | note | cost | policy | abort | ...
  actor        TEXT NOT NULL,   -- system | tier1 | tier2 | operator:<name>
  from_state   TEXT,
  to_state     TEXT,
  payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX incident_events_incident ON incident_events(incident_id, id);

CREATE TABLE fingerprints (
  fingerprint         TEXT PRIMARY KEY,
  project_slug        TEXT NOT NULL REFERENCES projects(slug),
  first_incident_id   INTEGER NOT NULL REFERENCES incidents(id),
  last_seen_at        TEXT NOT NULL,
  suppress_until      TEXT NOT NULL,
  total_occurrences   INTEGER NOT NULL DEFAULT 1,
  repair_attempts     INTEGER NOT NULL DEFAULT 0,
  total_cost_usd      REAL NOT NULL DEFAULT 0
);

CREATE TABLE agent_runs (
  id                  TEXT PRIMARY KEY,          -- ULID
  incident_id         INTEGER NOT NULL REFERENCES incidents(id),
  tier                INTEGER NOT NULL,
  runner              TEXT NOT NULL,             -- "agentsdk" | ...
  model               TEXT NOT NULL,
  pid                 INTEGER,
  pgid                INTEGER,
  state               TEXT NOT NULL,             -- running | done | killed | crashed
  workspace_path      TEXT,
  base_sha            TEXT,
  log_path            TEXT,
  input_tokens        INTEGER NOT NULL DEFAULT 0,
  output_tokens       INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens   INTEGER NOT NULL DEFAULT 0,
  cache_write_tokens  INTEGER NOT NULL DEFAULT 0,
  cost_usd            REAL NOT NULL DEFAULT 0,
  turns               INTEGER NOT NULL DEFAULT 0,
  exit_code           INTEGER,
  abort_reason        TEXT,                      -- cost_cap | timeout | operator | shutdown
  abort_requested_at  TEXT,
  killed_at           TEXT,
  started_at          TEXT NOT NULL,
  ended_at            TEXT
);
CREATE INDEX agent_runs_incident ON agent_runs(incident_id, started_at DESC);
CREATE INDEX agent_runs_live     ON agent_runs(state) WHERE state = 'running';

-- Append-only source of truth for all spend.
CREATE TABLE budget_ledger (
  id            INTEGER PRIMARY KEY,
  ts            TEXT NOT NULL,
  project_slug  TEXT REFERENCES projects(slug),
  incident_id   INTEGER REFERENCES incidents(id),
  run_id        TEXT REFERENCES agent_runs(id),
  tier          INTEGER NOT NULL,
  model         TEXT NOT NULL,
  input_tokens        INTEGER NOT NULL DEFAULT 0,
  output_tokens       INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens   INTEGER NOT NULL DEFAULT 0,
  cache_write_tokens  INTEGER NOT NULL DEFAULT 0,
  cost_usd      REAL NOT NULL
);
CREATE INDEX budget_ledger_ts ON budget_ledger(ts);

-- Materialized counters. Rebuildable from budget_ledger.
CREATE TABLE budget_windows (
  scope               TEXT NOT NULL,   -- global | project
  scope_id            TEXT,            -- NULL for global
  kind                TEXT NOT NULL,   -- day | week | month
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
  threshold    TEXT NOT NULL,   -- soft | hard | efficiency | forecast
  fired_at     TEXT NOT NULL,
  digest_json  TEXT NOT NULL,
  cleared_at   TEXT
);
CREATE UNIQUE INDEX budget_alerts_once
  ON budget_alerts(scope, scope_id, kind, window_start, threshold);

CREATE TABLE patches (
  id                INTEGER PRIMARY KEY,
  incident_id       INTEGER NOT NULL REFERENCES incidents(id),
  run_id            TEXT NOT NULL REFERENCES agent_runs(id),
  branch            TEXT NOT NULL,
  base_sha          TEXT NOT NULL,
  head_sha          TEXT,
  files_changed     INTEGER NOT NULL DEFAULT 0,
  lines_added       INTEGER NOT NULL DEFAULT 0,
  lines_removed     INTEGER NOT NULL DEFAULT 0,
  diff_path         TEXT,
  pr_url            TEXT,
  pr_number         INTEGER,
  state             TEXT NOT NULL,  -- proposed | merged | rejected | closed
  applied_autonomy  TEXT NOT NULL,
  downgrade_reason  TEXT,
  verified_at       TEXT,
  created_at        TEXT NOT NULL,
  updated_at        TEXT NOT NULL
);

CREATE TABLE settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT
);  -- paused, pause_reason, soft_mode_active, schema_version

CREATE TABLE ingest_cursor (
  source       TEXT PRIMARY KEY,
  last_seen_at TEXT NOT NULL,
  state_json   TEXT NOT NULL DEFAULT '{}'
);
```

**Logs are deliberately not in SQLite.** A Tier 2 run emits thousands of stream events; storing
them as rows would dominate database size, cause write amplification against the WAL, and make
`VACUUM` expensive on a single consumer SSD. Raw streams live at `var/runs/<run_id>.jsonl` with
only the path in `agent_runs.log_path`. Diffs likewise live at `var/patches/<patch_id>.diff`.

**Retention** (`sentinel prune`, nightly): run logs and diffs older than 30 days are deleted
(configurable), `incident_events` older than 180 days are compacted to state changes only, and
`budget_ledger` is retained indefinitely because it is the financial record.

---

## 6. Configuration

### 6.1 `.env`

```bash
# Secrets and host-specific values. Never committed; .env.example is.
ANTHROPIC_API_KEY=
GITHUB_TOKEN=
GITHUB_WEBHOOK_SECRET=
GCP_PROJECT_ID=
PUBSUB_SUBSCRIPTION=
GOOGLE_APPLICATION_CREDENTIALS=
SLACK_WEBHOOK_URL=
DASHBOARD_PASSWORD_HASH=          # bcrypt
SENTINEL_LISTEN_ADDR=127.0.0.1:8787
SENTINEL_DATA_DIR=./var
SENTINEL_TIMEZONE=UTC             # budget window alignment
SENTINEL_LOG_LEVEL=info
```

### 6.2 `projects.yaml`

`projects.example.yaml` is committed with placeholder values; the real file is gitignored.

```yaml
version: 1

budgets:
  per_incident_usd: 1.00
  daily:   { global: { soft: 7,   hard: 10  }, per_project: { hard: 2 } }
  weekly:  { global: { soft: 35,  hard: 50  } }
  monthly: { global: { soft: 110, hard: 150 } }
  week_starts_on: monday
  efficiency:
    enabled: true
    window: week
    max_cost_per_resolution_multiple: 3.0   # × trailing 4-window median
    min_resolutions_for_signal: 5
  forecast:
    enabled: true
    warn_at_fraction_of_hard: 0.9

soft_mode:
  park_tier2: true          # confirmed design decision
  downgrade_model: false
  force_pr_only: false
  min_confidence: null

defaults:
  autonomy: pr_only
  tier2_model: claude-opus-5
  tier1_model: claude-haiku-4-5
  tier2_effort: high
  max_turns: 40
  run_timeout: 20m
  suppression_window: 6h
  max_repair_attempts_per_fingerprint: 2
  max_diff_lines: 400
  probation_incidents: 3
  allow_test_changes: false
  protected_paths:
    - ".github/**"
    - "**/*.lock"
    - "**/migrations/**"
    - "**/terraform/**"
    - "Dockerfile*"
  commands:
    test: make test
    build: make build
    healthcheck: make healthcheck

runtime:
  max_concurrent_agents: 1
  min_free_ram_mb: 2048
  min_free_disk_mb: 10240
  tier2_min_confidence: 0.5
  max_input_tokens: 40000

projects:
  - slug: example-api
    repo: github.com/example/example-api
    default_branch: main
    autonomy: auto_merge
    triggers:
      workflow_run: true
      issues:
        labels: [bug, sentinel]
      gcp_log_filter: >
        severity>=ERROR AND
        resource.labels.service_name="example-api"
    commands:
      test: make test
      build: make build
      healthcheck: make healthcheck
      deploy: make deploy
      rollback: make rollback
    env:
      DATABASE_URL: postgres://localhost/example_test
```

### 6.3 Application contract

Every managed application implements:

```makefile
test:        # unit + integration suite; exit 0 iff healthy
build:       # compile or validate; exit 0 iff buildable
healthcheck: # validate local environment dependencies; exit 0 iff runnable
deploy:      # optional; required for autonomy: auto_deploy
rollback:    # optional; strongly recommended with auto_deploy
```

Contract requirements: deterministic, no interactive prompts, no network access to production,
exit codes that mean what they say, and completion within `run_timeout`. A project failing
`healthcheck` at registration time is registered but immediately quarantined with a clear
reason rather than silently failing on its first real incident.

---

## 7. Cost & Budget Model

### 7.1 Threshold ladder

Every window may declare a **soft** threshold (alert and throttle) and a **hard** threshold
(stop). `budget.Check` evaluates all applicable windows and returns the most restrictive
verdict.

| Window | Scope | Soft | Hard | Hard verdict |
|---|---|---|---|---|
| per-incident | incident | — | $1.00 | Kill run, `escalated` |
| daily | project | — | $2.00 | Quarantine project until rollover |
| daily | global | $7 | $10 | Halt Tier 1+ until rollover |
| weekly | global | $35 | $50 | Halt Tier 1+ until week rollover |
| monthly | global | $110 | $150 | Halt Tier 1+ until month rollover |

Weekly and monthly windows are **calendar-aligned** to `SENTINEL_TIMEZONE` with a configurable
week start, because the purpose of the weekly window is human planning cadence — the operator
schedules an evening, not a rolling interval.

The weekly hard stop sits well above its soft threshold intentionally: it is a backstop, not
the primary control. In normal operation the soft threshold fires, the digest arrives, and the
operator steers before the hard limit is approached.

### 7.2 Price table

Rates per million tokens, sourced from the Anthropic API pricing table and stored in config so
they can be corrected without a rebuild:

| Model | Input | Output | Notes |
|---|---:|---:|---|
| `claude-opus-5` | $5.00 | $25.00 | Default Tier 2. Thinking on by default. |
| `claude-sonnet-5` | $3.00 | $15.00 | Introductory $2.00/$10.00 through 2026-08-31. |
| `claude-haiku-4-5` | $1.00 | $5.00 | Tier 1. No `effort` parameter. |

Cache reads bill at 0.10× input; cache writes at 1.25× input (5-minute TTL). A model absent
from the table is a **startup validation error**, not a runtime surprise — the system refuses
to spend money it cannot price.

Startup also logs a reminder that this table is a local mirror of published pricing and is the
operator's responsibility to keep current; a stale table under-reports spend, which is the
dangerous direction.

Tier 2 on `claude-opus-5` requires headroom in `max_tokens` because thinking is enabled by
default and shares that ceiling with the answer; a tight limit truncates a repair mid-edit.

### 7.3 Soft mode

When any soft threshold is crossed, `settings.soft_mode_active` is set and:

1. **Tier 2 work parks.** Incidents that would escalate to Tier 2 enter `parked` with reason
   `budget_soft_hold`. They are not dismissed and not lost.
2. **Tier 0 and Tier 1 continue.** Triage costs fractions of a cent, so the system stays fully
   observant while spending nothing meaningful. Going blind to save $0.002 would be the wrong
   trade.
3. **A digest notification fires once** per threshold crossing per window.
4. **The dashboard enters a visibly throttled state** with the parked queue promoted to the
   overview and a bulk-release action available.

Soft mode clears automatically at window rollover, or manually by an operator (which is
recorded in `incident_events` as an operator action).

### 7.4 Efficiency signal

A dollar figure alone cannot distinguish a productive week from a thrashing one: $60 that
merged 40 fixes is the system working, and $60 that escalated 30 times is not. So in addition
to absolute thresholds:

```
cost_per_resolution = window.spend_usd / max(window.incidents_resolved, 1)
```

An `efficiency` alert fires when this exceeds `max_cost_per_resolution_multiple` × the trailing
four-window median, provided at least `min_resolutions_for_signal` resolutions exist in the
comparison set (below that, the ratio is noise). This detects "spending at a normal rate,
achieving nothing" — which no absolute number catches.

### 7.5 Forecast signal

```
projected = window.spend_usd × (window_duration / elapsed_duration)
```

A `forecast` alert fires when `projected > hard × warn_at_fraction_of_hard`. "At this burn rate
you reach $150 on the 22nd" is actionable on the 8th; a threshold trip on the 22nd is merely
informative. Forecasting is suppressed for the first 20% of a window, where the projection is
dominated by noise.

### 7.6 Bulk-fix digest

The payload of a `budget_soft`, `budget_efficiency`, or `budget_forecast` notification. Its
purpose is to make a dedicated human session productive, so it is a worklist rather than a
number:

1. Window spend against soft and hard thresholds, plus projected end-of-window.
2. Top 5 fingerprints by cumulative spend, with occurrence counts and repair attempts.
3. Projects ranked by cost-per-resolution against their own trailing median.
4. Repeat offenders: fingerprints that hit `max_repair_attempts_per_fingerprint`.
5. Escalation reasons grouped by frequency — a cluster of `verification_failed` means something
   different from a cluster of `cost_cap`.
6. Parked queue: count, oldest entry, and a deep link to the bulk-release view.

The digest renders as Slack blocks, as a GitHub issue comment, and as a dashboard panel from one
structured source, so the three cannot disagree.

### 7.7 Other breakers

| Breaker | Default | Action |
|---|---|---|
| Consecutive project failures | 3 | Quarantine project; requires manual clear |
| Repeat fingerprint repair attempts | 2 per 24 h | `escalated`, reason `repeat_failure` |
| Free RAM floor | 2048 MB | Requeue spawn with backoff |
| Free disk floor | 10240 MB | Requeue spawn; alert |
| Global kill switch | manual | Blocks every spawn and every merge |

---

## 8. HTTP API

All routes under `/api`; everything else serves the embedded SPA with history fallback.
Authentication is a session cookie issued against `DASHBOARD_PASSWORD_HASH`; the server binds
to loopback or the tailnet interface only.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/health` | Liveness; no auth |
| `GET` | `/api/overview` | Counters, budget windows, soft-mode state, kill switch |
| `GET` | `/api/incidents` | Filter by state, project, tier, time range; paginated |
| `GET` | `/api/incidents/{id}` | Incident with events, runs, patch |
| `POST` | `/api/incidents/{id}/retry` | Re-queue a `failed`/`escalated` incident |
| `POST` | `/api/incidents/{id}/dismiss` | Operator dismissal with a reason |
| `POST` | `/api/incidents/{id}/release` | Release one `parked` incident |
| `POST` | `/api/parked/release` | Bulk release, optionally filtered |
| `GET` | `/api/runs/{id}` | Run metadata and cost breakdown |
| `GET` | `/api/runs/{id}/log` | Log tail or range from the `.jsonl` |
| `POST` | `/api/runs/{id}/abort` | Immediate `SIGKILL` to the process group |
| `GET` | `/api/patches/{id}/diff` | Unified diff |
| `POST` | `/api/patches/{id}/approve` | Merge a `pr_only` patch |
| `POST` | `/api/patches/{id}/reject` | Close PR, delete branch, record reason |
| `GET` | `/api/budget` | Windows, thresholds, forecast, efficiency |
| `GET` | `/api/budget/digest` | Current digest, on demand |
| `GET` | `/api/projects` | Registry with health and quarantine state |
| `POST` | `/api/projects/{slug}/quarantine` | Set or clear quarantine |
| `POST` | `/api/settings/pause` | Kill switch on/off |
| `GET` | `/api/stream` | SSE; `topics`, `Last-Event-ID` |

Every mutating route requires the session cookie plus a CSRF token, records an
`incident_events` or `settings` row attributing the action to the operator, and is idempotent
where a retry could otherwise double-act.

`POST /api/runs/{id}/abort` performs the kill **synchronously before writing the response**, so
a 200 means the process group is already dead. Both timestamps are persisted for measurement.

---

## 9. Frontend Architecture

Vite + React + TypeScript, built to `web/dist`, embedded with `//go:embed` and served by the Go
HTTP server. No Node process exists at runtime. A `-dev` flag reverse-proxies to the Vite dev
server instead of serving the embedded bundle, so development has hot reload without a separate
build step.

State: TanStack Query for HTTP reads, one SSE connection multiplexed across topics feeding the
same cache. SSE events patch the query cache; a `resync` event invalidates it. There is no
second client-side state store, so the server remains the single source of truth and a dropped
connection self-heals.

| View | Contents |
|---|---|
| **Overview** | Live incident feed, budget gauges per window with soft/hard markers, forecast line, soft-mode banner, parked queue, kill switch |
| **Incident** | Timeline from `incident_events`, live log stream, diff viewer, cost breakdown, approve/reject/abort/retry |
| **Spend** | Spend by day/week/month, split by project, tier, and model; cost-per-resolution trend; efficiency and forecast alerts |
| **Parked** | The bulk-fix worklist: grouped by fingerprint and project, with select-and-release |
| **Projects** | Registry, health, quarantine state and reasons, per-project spend and success rate |
| **Audit** | Chronological operator and system actions across all incidents |

Accessibility and operational requirements: full keyboard navigation for the review flow
(approve/reject/next is the highest-frequency interaction), the abort control is
visually distinct and reachable from every view, and the log viewer virtualises rows so a
100 000-line stream does not lock the tab.

---

## 10. Security

- **Secrets never reach the execution plane.** Workers get an explicit environment allowlist;
  `os.Environ()` is never inherited. The GitHub token, Slack webhook, and dashboard hash are
  invisible to agent-authored code.
- **The agent cannot publish.** Its tool allowlist excludes push and PR creation; delivery is
  the control plane's job. A confused or compromised agent can dirty a throwaway worktree and
  nothing else.
- **Two-layer webhook verification** (§4.2.1): the relay and the control plane both verify HMAC.
- **`gitleaks` pre-commit hook** plus GitHub Push Protection on this repository. CI runs
  `gitleaks detect` and fails on any finding.
- **No secrets in the database.** Tokens are read from the environment on each use, never
  persisted. Log streams are scanned for high-entropy strings and redacted before storage.
- **Dashboard is not internet-facing.** Loopback or tailnet binding, single shared credential.
  A public deployment would require real authentication and is explicitly out of scope.
- **Protected paths** (§4.8) prevent autonomous modification of CI configuration, lockfiles,
  migrations, and infrastructure definitions — the paths where an incorrect automated change is
  hardest to reverse.

---

## 11. Testing Strategy

The orchestrator is the thing that must not fail, so it is tested more heavily than anything it
manages.

| Layer | Approach |
|---|---|
| Tier 0 filters | Table-driven over recorded real payloads in `testdata/`. Pure functions, exhaustive. |
| Ingest adapters | Golden-file normalization tests per source, including malformed and truncated messages. |
| Tier 1 client | `httptest` server returning recorded API responses, including a `refusal` response and a schema-violating response. |
| Budget | Property-style tests over window boundaries, DST transitions, timezone changes, and most-restrictive-wins precedence across all windows. |
| Store | Migration up-tests against a temp file DB; concurrent read/write to prove the single-writer design eliminates `SQLITE_BUSY`. |
| **Runner** | A **`fake-agent` test binary** that emits scripted stream JSON and can hang, fork children, exit non-zero, or emit malformed lines. This is the only honest way to test process-group `SIGKILL`, orphan cleanup, cost-cap interruption mid-stream, and shutdown draining — none of which can be verified with a mock. |
| Verify | Fixture repositories with passing, failing, and test-deleting diffs. |
| SSE hub | Slow-consumer test proving drop-oldest and `resync` rather than unbounded growth; replay correctness from `Last-Event-ID`. |
| End-to-end | A local fixture repository plus a fake Pub/Sub publisher, exercising receive → triage → repair → verify → PR with the runner faked. |

CI (GitHub Actions): `go vet`, `staticcheck`, `go test -race ./...`, `gitleaks detect`, frontend
typecheck and build, and a `CGO_ENABLED=0` cross-build to confirm the static-binary guarantee
holds.

---

## 12. Observability

Structured JSON logs via `log/slog` to stdout, with `incident_id` and `run_id` propagated
through context so any line can be traced to its cause.

`GET /api/health` reports process RSS, goroutine count, SQLite size, Pub/Sub subscriber state
and backlog, active runs, free RAM and disk, and last-successful-ingest time. A stalled
subscriber is the most likely silent failure — the system would appear healthy while seeing
nothing — so ingest staleness beyond a threshold raises an escalation notification of its own.

The RSS target (15–25 MB steady state) is asserted in a soak test rather than assumed, since it
is the constraint the entire architecture was chosen to satisfy.

---

## 13. Repository Layout

```
.
├── cmd/sentinel/                 main, flags, wiring, subcommands
├── internal/
│   ├── config/  ingest/  triage/  budget/  workspace/
│   ├── runner/  verify/  policy/  forge/  notify/
│   ├── bus/  httpapi/  store/
├── internal/store/migrations/    NNNN_name.sql (embedded)
├── internal/webassets/           //go:embed target for the built SPA
│   └── dist/                     (Vite output; .gitkeep committed)
├── web/                          React SPA source (src/, index.html, configs)
├── deploy/
│   ├── relay/                    Cloud Run GitHub→Pub/Sub relay
│   ├── gcp/                      topic, subscription, log sink scripts
│   └── launchd/                  com.sentinel.plist
├── testdata/                     recorded payloads, fixture repos
├── docs/
│   ├── CONCEPT.md
│   ├── SPEC.md
│   └── superpowers/specs/
├── var/                          gitignored runtime state
│   ├── sentinel.db  repos/  work/  runs/  patches/
├── .agents/                      b1 agent-instruction sources (tracked)
│   ├── config.yaml
│   ├── project/AGENTS.md
│   └── modules/{go,react-web}
├── AGENTS.md                     generated from .agents/ (tracked)
├── README.md
├── projects.yaml (gitignored)    projects.example.yaml
├── .env (gitignored)             .env.example
└── Makefile

Gitignored agent files: CLAUDE.md, CLAUDE.local.md, AGENTS.override.md,
.agents/local/, .agents/rules/local.md, .agents/skills/b1-*.md, and
.agents/.b1-snapshots/ — per-machine or generated, never committed.
```

`make` targets: `build`, `dev`, `test`, `lint`, `web`, `migrate`, `run`, `prune`, `reconcile`.

---

## 14. Milestones

Each milestone gets its own implementation plan and build cycle. Milestones are ordered so that
every one ends with something runnable.

**M0 — Skeleton.** `config` with validation, `store` with migrations, HTTP server, embedded SPA
shell, SSE hub, `/api/health`, launchd plist. *Done when:* the binary runs as a background
service, serves a dashboard shell, and survives a reboot.

**M1 — Ingestion.** Pub/Sub subscriber, GitHub relay, both adapters, fingerprinting and
suppression, all Tier 0 filters, incident persistence, live feed in the UI. *Done when:* real
GitHub and GCP events appear in the dashboard within seconds, storms collapse correctly, and
the system has spent $0.00. **M1 alone is a useful product — a live NOC for 25 repositories at
no marginal cost.**

**M2 — Triage & money.** Tier 1 classifier, price table, ledger, all window counters, the full
threshold ladder, soft mode with parking, efficiency and forecast signals, digest generation,
notifiers, Spend and Parked views. *Done when:* incidents are classified for fractions of a
cent, and every threshold and breaker is demonstrated with a forced test.

**M3 — Repair.** Workspace mirrors and worktrees, `Runner` interface and the Agent SDK
implementation, stream parsing, incremental cost cap, the termination ladder, abort endpoint,
independent verification. *Done when:* a real failing test in a fixture repository is fixed
autonomously, verified independently, and a forced cost cap and a forced abort both kill the
entire process group with no orphans.

**M4 — Delivery.** Policy engine, protected paths, `forge`, PR generation, approve/reject flow,
auto-merge, auto-deploy with healthcheck and rollback. *Done when:* a `pr_only` project produces
a reviewable PR and an `auto_merge` project closes the loop unattended.

**M5 — Hardening.** Retention and pruning, budget reconciliation, quarantine management, soak
test for the RSS target, `@sentinel` issue commands, `projects.example.yaml`, README, developer
guide, gitleaks and CI. *Done when:* the repository is publishable and the system has run unattended
for a week within budget.

---

## 15. Open Questions

Recorded rather than guessed. None blocks M0–M2.

1. **Agent SDK option names** (§4.7.4) must be verified against current documentation at M3.
   No option name in this spec should be treated as authoritative.
2. **Auto-deploy semantics** are specified only in outline. Before M4, the deploy/healthcheck/
   rollback contract needs to be validated against at least two real projects with genuinely
   different deployment shapes.
3. **Flaky-test handling.** Tier 1 has a `flaky_test` category, but the policy for it is
   undefined: quarantine the test, open an issue, or ignore. Needs a decision before M2 ships
   the classifier prompt.
4. **Gemini as a Tier 1 alternative.** `CONCEPT.md` mentions Gemini Flash. v1 standardises on
   `claude-haiku-4-5` for one price table and one client. Revisit only if Tier 1 volume makes
   the difference material.
5. **Multi-repo incidents.** Out of scope for v1; a breaking change in a shared library will
   surface as N independent incidents across N repositories. Acceptable for now, worth
   revisiting once real failure patterns are observed.
