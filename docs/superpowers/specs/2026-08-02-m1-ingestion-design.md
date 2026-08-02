# M1 — Ingestion: Design

**Status:** Approved · **Date:** 2026-08-02 · **Derived from:** [`SPEC.md`](../../SPEC.md) §4.2, §4.3, §4.11, §4.12, §5, §6, §8, §9, §12, §13 — milestone M1 in §14.

---

## 1. Goal

Turn `triage-sentinel` from a dashboard shell into a live NOC for 25 repositories at
no marginal cost. Real GitHub and GCP events arrive over an outbound-initiated pull,
are normalised, deduplicated, fingerprinted and filtered, persist as incidents, and
appear in the dashboard within seconds — having spent $0.00 and touched no repository.

**Done when:**

1. A real `workflow_run` failure in a registered repository appears in the dashboard within seconds.
2. A real GCP log entry matching a project's sink filter does the same.
3. An error storm collapses: 500 log entries sharing one fingerprint produce exactly
   one incident with `occurrence_count = 500`.
4. The process survives restart mid-queue and resumes without losing or double-processing an event.
5. `make check` is green and no LLM API call exists anywhere in the milestone's code.

---

## 2. Decisions

Four decisions were settled before design. Each is recorded with its rationale because
each closes off an alternative that would otherwise look reasonable later.

### 2.1 Pub/Sub transport: REST long-poll, not the gRPC client

SPEC §4.2 says "streaming-pull Pub/Sub subscriber", which implies
`cloud.google.com/go/pubsub/v2`. Measured cost of that client:

| | Current | With `pubsub/v2` |
|---|---:|---:|
| Binary (`CGO_ENABLED=0 -s -w`) | 11 MB | ~25 MB |
| Modules in graph (`go list -m all`) | 32 | ~200 |

Measured by building the current binary, and separately building a hello-world that
imports only `cloud.google.com/go/pubsub/v2` (16 MB, 199 modules on its own).

The 15–25 MB steady-state RSS target is the constraint the entire architecture was
chosen to satisfy (SPEC §1.3, §12). Adding gRPC's HTTP/2 connection state to a process
whose whole justification is a small resident footprint risks failing the project's
central goal, and a 199-module graph is a poor fit for a repository meant to be publicly
auditable.

The decisive detail is in the spec's own delivery design: messages are acked
**immediately after the durable write**, within milliseconds of receipt. Ack-deadline
extension is the hardest part of a hand-rolled pull loop, and this design never needs
it. What remains — token refresh, backoff, reconnection — is either handled by
`golang.org/x/oauth2/google` or is small and testable.

**Consequence:** SPEC §4.2 must be amended. Latency becomes long-poll-bounded rather
than push, which still satisfies "within seconds".

### 2.2 Infrastructure is provisioned for real in M1

`deploy/relay/` and `deploy/gcp/` are both built and actually deployed, so the
milestone's stated criterion — real events in the dashboard — is met rather than
simulated. IAM and auth surprises surface now, while ingestion is the only moving part,
instead of during M2 when spend is also in play.

The relay is a **separate Go module**. It publishes to Pub/Sub and therefore wants the
Google client libraries; keeping it out of the root module is what preserves §2.1's
dependency-graph result for the binary that actually runs on the Mac Mini.

### 2.3 `BuildSanity` is deferred to M3, with its seam kept

Tier 0's `BuildSanity` filter runs a project's `commands.healthcheck`, which needs a
checked-out repository and subprocess supervision — `workspace` and `runner`, neither of
which exists until M3. Pulling them forward would have M1 cloning repositories and
executing foreign Makefiles, contradicting the milestone's defining property.

It ships as a registered no-op in the correct chain position, with a test asserting it
passes everything through. M3 replaces the body without touching a call site.

**Consequence:** SPEC §4.3.1 must be amended.

### 2.4 The §9 frontend shell is built now

Router and TanStack Query arrive in M1, with `App.tsx` split into a layout plus views.
Overview (live feed) and Incident (detail) are real; Spend, Parked, Projects and Audit
are stubs. This front-loads the architecture so M2's views are additive rather than a
rewrite, and it proves §9's "SSE events patch the query cache, `resync` invalidates it"
contract while there is exactly one topic to prove it against.

---

## 3. Architecture

### 3.1 Two loops sharing only the database

SPEC §4.2's ack rule — acknowledge after the durable write, not after processing —
splits ingestion into two independent loops:

```
PULL LOOP  (synchronous; everything here precedes the ack)

   pull ──▶ Match ──▶ Normalize ──▶ route to slug
                                        │
                                        ▼
              INSERT INTO incidents (state='received')
              ON CONFLICT (source, source_ref)
                DO UPDATE SET occurrence_count = occurrence_count + 1
                                        │
                                        ▼
                                       ack


PROCESS LOOP  (asynchronous; restart-resumable)

   SELECT ... WHERE state='received' ORDER BY id
        │
        ▼
   Tier 0 chain ──▶ filtered │ suppressed │ triaging
        │
        ▼
   append incident_events ──▶ publish to bus (topic: incidents)
```

Two properties fall out for free. Crash recovery needs no special mechanism —
unprocessed rows are simply rows, picked up on the next drain. And the ack deadline is
decoupled from processing time entirely, so a slow filter can never cause redelivery.

### 3.2 Refinement: `Unroutable` and `Duplicate` move to the write boundary

SPEC §4.3.1 lists seven Tier 0 filters. Two of them are not implemented as chain
members:

- **`Duplicate`** is enforced by the existing `incidents_source_ref` UNIQUE index.
  Pub/Sub is at-least-once, and SPEC §4.2 already specifies the
  `ON CONFLICT DO UPDATE` form. A chain member would have to `SELECT` first, which is
  both redundant and racy against a concurrent duplicate delivery.
- **`Unroutable`** is decided by routing, which writes the row directly as
  `project_slug = NULL, state = 'filtered', reason = 'unroutable'` per SPEC §4.2.

Both facts are therefore established by the insert itself. The remaining five —
`Quarantined`, `Transient`, `SelfInflicted`, `Fingerprint`, `BuildSanity` (no-op) — stay
pure functions in the declared order.

### 3.3 Where incidents come to rest

An incident surviving Tier 0 enters `triaging` and stays there. M1 has no Tier 1 to hand
it to. This is the honest end state, and the Overview must display that count plainly
rather than implying a queue is being worked. M2's classifier attaches at exactly this
point.

---

## 4. Packages

`store` continues to import nothing but `config`. No package imports `httpapi`.

| Package | File | Responsibility |
|---|---|---|
| `internal/ingest` | `event.go` | `Event` (SPEC §4.2), `Adapter` interface, `ErrIgnore` sentinel |
| | `pull.go` | `Puller` interface; REST long-poll implementation; oauth2 token source; backoff |
| | `router.go` | Adapter selection by attributes; slug resolution against the registry |
| | `github.go` | `workflow_run` / `issues` normalisation; HMAC re-verification |
| | `gcplog.go` | Cloud Logging entry normalisation |
| `internal/triage` | `fingerprint.go` | `Fingerprint`, `normalize`, frame extraction |
| | `tier0.go` | `Filter` type, ordered chain, `Decision` |
| | `transient.go` | Compiled regex set from the registry |
| `internal/store` | `projects.go` | Registry → `projects` table sync |
| | `incidents.go` | Upsert, list with filters, get with events |
| | `events.go` | `incident_events` append; replay query for SSE |
| `internal/orchestrator` | `orchestrator.go` | The process loop; concurrency and tier escalation seams for M2/M3 |
| `deploy/relay` | own module | Cloud Run HMAC-verify-and-publish relay |
| `deploy/gcp` | shell | Topic, subscription, per-project log sink scripts |

### 4.1 The `Puller` seam

```go
type Puller interface {
    Pull(ctx context.Context, max int) ([]Message, error)
    Ack(ctx context.Context, ackIDs []string) error
}
```

Production wires the REST implementation. Tests wire a fake, and an `httptest` server
emulates the Pub/Sub REST surface for the transport's own tests. A `sentinel replay
<file>` subcommand feeds recorded payloads through the real adapters and process loop,
so the whole pipeline is exercisable with no GCP access at all.

### 4.2 REST transport details

- **Auth:** `golang.org/x/oauth2/google.FindDefaultCredentials` with scope
  `https://www.googleapis.com/auth/pubsub`, wrapped by `oauth2.NewClient`. Token refresh
  is handled by the library; this is the reason §2.1's hand-rolling concern is small.
- **Pull:** `POST https://pubsub.googleapis.com/v1/{subscription}:pull`,
  body `{"maxMessages": N}`.
- **Ack:** `POST https://pubsub.googleapis.com/v1/{subscription}:acknowledge`,
  body `{"ackIds": [...]}`.
- **Backoff:** exponential with jitter on 5xx, 429, and network errors; capped. A
  successful pull resets it.

**Implementation-time verification required.** `returnImmediately` is deprecated, and
the server-side hold duration for a `pull` with no available messages must be confirmed
against current Google documentation at implementation time. If the call returns empty
immediately rather than holding, the loop needs a poll interval; if it holds, it does
not. This design deliberately does not assert which, for the same reason SPEC §4.7.4
declines to enumerate Agent SDK flags.

### 4.3 Projects table sync

`store.Open` verifies `foreign_keys(1)`, and `incidents.project_slug REFERENCES
projects(slug)`. The `projects` table must therefore be populated from `projects.yaml`
before any incident can be written.

Sync runs at startup and on SIGHUP: upsert `active = 1` for every registered slug, set
`active = 0` for rows whose slug is no longer registered. Rows are never deleted —
SPEC §4.1 requires removing a project from the registry to preserve its incident history.

### 4.4 Fingerprinting

```
fingerprint = sha256(project_slug, error_class, normalize(selected_frames))
```

`normalize` strips absolute paths, line numbers, memory addresses, UUIDs, and
timestamps. `top_n_frames` defaults to 5.

Fingerprinting is the mechanism the entire cost story rests on, and its two failure
modes are **not** symmetric:

| Failure | Consequence | Backstop |
|---|---|---|
| **Over-collapse** — distinct bugs share a fingerprint | The second bug is silently suppressed for the whole window. A real failure vanishes. | **None.** |
| **Under-collapse** — one bug yields several fingerprints | Duplicate incidents; roughly proportional extra spend. | Per-incident, daily, weekly and monthly ceilings; the storm is visible in occurrence counts. |

There is a backstop for the cheap failure and none for the expensive one. Every
ambiguity below therefore resolves the same way:

> **When frame classification is uncertain, fingerprint _more_ specifically, never less.**

#### 4.4.1 Selecting frames: allowlist, then denylist, never empty

SPEC §4.3.2 restricts frames to "the project's own source tree". M1 has no checkout to
define that tree, so selection runs a three-step ladder and records which step produced
the result:

| Step | Strategy | Selection |
|---|---|---|
| 1 | `source_roots` | Project declares `fingerprint.source_roots` in the registry. A frame is own-source iff its normalised path is under one of them. **Closed set — preferred.** |
| 2 | `denylist` | No roots declared. A frame is own-source iff its path matches no known dependency directory (`vendor/`, `node_modules/`, `site-packages/`, `dist-packages/`, `.venv/`, `.cargo/registry/`, `~/go/pkg/mod/`, `.gem/`, `.m2/`). **Open-ended — a best effort.** |
| 3 | `all_frames` | Steps 1–2 selected **zero** frames. Fall back to the top N frames *including* dependency frames. |

Step 3 is the point of the whole ladder. The earlier design excluded dependency frames
unconditionally, so a stack whose every top frame lived in a dependency yielded an empty
frame set and hashed to `sha256(slug, error_class, "")` — collapsing every same-class
error in that project into one fingerprint. That is the unbacked failure mode, reached
by the most common path. Degrading to all frames is *narrower*, not broader: it can
only split fingerprints apart, never merge them.

This mirrors how Sentry groups events — in-app frames when known, all frames when not.
The difference is that Sentry's SDK supplies the in-app flag and we must infer it, which
is exactly why the fallback direction matters more than the inference does.

If no frames parse at all, strategy is `no_frames` and the hash covers
`normalize(title + first non-empty body line)` — still per-message-shape, still narrow.

#### 4.4.2 Fingerprint inputs differ by source

A stack trace is the wrong model for a CI failure, and applying it anyway would
over-collapse:

| Kind | `error_class` | Frames |
|---|---|---|
| `log.error` | Exception type, or the first line's leading token | Stack frames per §4.4.1 |
| `workflow_run.failed` | Workflow name | Failed **job** and **step** names |
| `issues.opened` / `.labeled` | — | **No fingerprint** (`NULL`) |

Human-filed issues are not storms, and `incidents.fingerprint` is already nullable;
`(source, source_ref)` deduplication is sufficient for them. Fingerprinting an issue
would only risk collapsing two unrelated bug reports.

**Implementation-time verification required.** The `workflow_run` webhook payload
describes the run, not its jobs. Whether it carries failed job and step identifiers must
be confirmed against current GitHub documentation. If it does not, the adapter fetches
`GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs` — an ordinary API call using the
token M1 already requires, with no LLM cost — and fingerprints on the failed job and
step names. Fingerprinting on workflow name alone is **not** an acceptable fallback: it
would collapse every failure of `ci.yml` into a single incident, which is precisely the
unbacked failure mode.

#### 4.4.3 Recording the evidence

Migration `0002` adds two columns to `fingerprints`:

```sql
ALTER TABLE fingerprints ADD COLUMN strategy    TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE fingerprints ADD COLUMN frames_json TEXT NOT NULL DEFAULT '[]';
```

`strategy` is one of `source_roots` / `denylist` / `all_frames` / `workflow` /
`no_frames`. `frames_json` holds the normalised frames that produced the hash.

This is what makes M1 worth its own milestone. M1 spends nothing, so it is a free
observation period: after a week of real traffic the dashboard can show which strategy
each project fell back to and which frames grouped what, and `source_roots` can be
tuned from evidence before M2 starts spending against these groupings. Without the
recorded frames it is impossible to tell *why* two things collapsed, only that they did.
The storage cost is a few short strings per distinct fingerprint.

**This contradicts an M0 rationale, deliberately.** M0's scope note argued that shipping
the complete SPEC §5 schema as one atomic `0001` would let "M1's ingestion land without
schema churn". That held for every table M1 writes; it did not anticipate needing to
record *why* a fingerprint grouped what it did, because that need only became visible
once §4.4's asymmetry was worked through. Forward-only numbered migrations exist for
exactly this, the runner already globs and sorts `migrations/*.sql`, and adding `0002`
costs nothing. Editing `0001` in place is not an option — it is already applied.

#### 4.4.4 Suppression

The first event for a fingerprint opens an incident and starts a suppression window
(`fingerprints.suppress_until`, default 6h, per-project override). Subsequent events
inside the window increment `occurrence_count`, append an `incident_events` row, and
cost nothing.

---

## 5. Configuration additions

Two new registry blocks, both validated at load. A bad regex refuses startup rather than
failing on the first real event.

```yaml
bot:
  email: sentinel@example.invalid   # SelfInflicted matches this; M4 commits as it
  name: triage-sentinel

triage:
  transient_patterns:               # compiled at load
    - "(?i)connection reset by peer"
    - "(?i)ECONNRESET"
    - "(?i)rate limit"
    - "(?i)the runner has received a shutdown signal"
    - "(?i)the operation was canceled"
```

And, per project, the optional allowlist that step 1 of §4.4.1 prefers:

```yaml
projects:
  - slug: example-api
    fingerprint:
      source_roots:                 # optional; omit to fall back to the denylist
        - "cmd/"
        - "internal/"
```

Validation: each root is non-empty, relative, and free of `..`. An absolute path or a
traversal is a startup error — a root that can never match would silently demote every
project to the weaker denylist strategy, and the whole point of §4.4.1 is that the
strategy in use is known rather than assumed.

`projects.example.yaml` gains all three blocks with these defaults.

### 5.1 Environment

M0 established that each milestone asserts the secrets it needs at wiring time rather
than in `LoadEnv`. M1 asserts, in `serve`: `GCP_PROJECT_ID`, `PUBSUB_SUBSCRIPTION`,
`GITHUB_WEBHOOK_SECRET`, `GITHUB_TOKEN`, and resolvable Google application credentials.

`GITHUB_TOKEN` is required a milestone earlier than SPEC §14 implies, because §4.4.2's
job-level fingerprinting reads the Actions API. It needs read scope only; M4 is the
milestone that first requires write. The token is read from the environment at point of
use and never persisted (SPEC §10).

A `--no-ingest` flag skips the subscriber for local dashboard work. The opt-out is
explicit and logged; silently starting without ingestion would reproduce exactly the
failure mode SPEC §12 calls the most likely silent one.

---

## 6. HTTP API

M1 is **read-only**. Every mutating route in SPEC §8 (`retry`, `dismiss`, `release`,
`abort`, `approve`, `reject`, `pause`, `quarantine`) belongs to a later milestone. This
is what keeps "spent $0.00 and changed nothing" literally true, and it lets CSRF stay
deferred exactly as M0 planned — there is still no state-changing route to protect.

| Method | Path | Notes |
|---|---|---|
| `GET` | `/api/incidents` | Filter by state, project, source, time range; paginated |
| `GET` | `/api/incidents/{id}` | Incident with its `incident_events` timeline |
| `GET` | `/api/overview` | Counters by state; ingest freshness. Budget fields land in M2 |
| `GET` | `/api/projects` | Registry with per-project incident counts and quarantine state |
| `GET` | `/api/stream` | Now carries real `incidents` events and honours `Last-Event-ID` |

### 6.1 SSE replay

M0 declared `httpapi.ReplayFunc` and wired `nil`, and documented `bus.Event.ID` as
reserved for `incident_events.id` so replay and the audit trail share one ID space. M1
supplies the store-backed implementation: `SELECT ... FROM incident_events WHERE id >
?`, mapped to `bus.Event` on topic `incidents`. Nothing about the SSE handler's shape
changes.

---

## 7. Frontend

```
web/src/
  main.tsx          router + QueryClient
  layout.tsx        nav shell, connection indicator
  lib/sse.ts        one EventSource, topics multiplexed, patches the query cache
  lib/api.ts        typed fetch wrappers
  views/
    overview.tsx    live incident feed, state counters      [real]
    incident.tsx    timeline from incident_events           [real]
    spend.tsx  parked.tsx  projects.tsx  audit.tsx          [stubs → M2/M5]
```

New dependencies: `react-router`, `@tanstack/react-query`.

A `resync` event from the bus invalidates the query cache rather than patching it, which
is the self-healing path §9 specifies for a client that fell behind. There is no second
client-side state store; the server stays the single source of truth.

---

## 8. Deploy artifacts

```
deploy/relay/          separate Go module
  main.go              HMAC verify → base64 body + headers as attributes → publish
  Dockerfile
  deploy.sh            gcloud run deploy
deploy/gcp/
  topic.sh             create the shared topic
  subscription.sh      create the pull subscription
  sink.sh <slug>       create a per-project Cloud Logging sink
```

Sinks are created by scripts, never by the binary, so the binary needs no GCP admin
credentials (SPEC §4.2.2).

**Two-layer HMAC (SPEC §4.2.1).** The relay verifies `X-Hub-Signature-256` against a
secret from Secret Manager and rejects on mismatch, so it cannot become an open publish
endpoint. `ingest/github.go` re-verifies the forwarded signature against
`GITHUB_WEBHOOK_SECRET` from `.env`, so a compromised relay cannot inject events. Both
comparisons are constant-time.

---

## 9. Failure handling

| Condition | Behaviour |
|---|---|
| HMAC mismatch | Logged as a security event; message **acked** and dropped. Acking is deliberate — nacking would let an attacker drive an unbounded redelivery loop. |
| No adapter matches | Acked, counted, not persisted. |
| `ErrIgnore` from an adapter | Acked, counted, not persisted. Structurally valid but uninteresting. |
| Malformed payload | Acked; persisted as `filtered` with reason `unparseable`, so it is visible rather than silently gone. |
| Unroutable (no project) | Persisted with `project_slug = NULL`, `filtered`, reason `unroutable`, surfaced in the dashboard. Usually means a stale `projects.yaml`. |
| Pull transport error | Exponential backoff with jitter; loop continues. |
| Stalled subscriber | `ingest_cursor.last_seen_at` drives a staleness threshold surfaced in `/api/health.problems`. |

On the last row: SPEC §12 wants a stalled subscriber to raise an escalation
*notification*, but `notify` does not exist until M2. M1 delivers the detection and the
health surface; M2 attaches the channel. This is stated so the gap is a recorded
decision rather than an oversight.

---

## 10. Testing

Per SPEC §11, and beside the code as `*_test.go`, table-driven, no assertion library.

| Target | Approach |
|---|---|
| Tier 0 filters | Table-driven over recorded real payloads in `testdata/`. Pure functions, exhaustive. |
| Fingerprinting | Normalisation strips paths, line numbers, addresses, UUIDs, timestamps. Distinct bugs must **not** collapse. Each ladder step in §4.4.1 is exercised and asserts the `strategy` it recorded. |
| **Fingerprint degradation** | **The regression test for §4.4's asymmetry: a stack whose every top frame is a dependency frame must fall back to `all_frames` and still produce *distinct* hashes for two distinct bugs. A shared `sha256(slug, error_class, "")` is the specific bug this asserts against.** |
| `workflow_run` grouping | Two different failing jobs in one workflow produce two fingerprints, not one. |
| Issue events | Carry `fingerprint IS NULL` and are deduplicated by `source_ref` alone. |
| Adapters | Golden-file normalisation per source, including malformed, truncated, and unknown-event payloads. |
| HMAC | Valid, invalid, missing, and truncated signatures; constant-time comparison used. |
| REST transport | `httptest` Pub/Sub emulator: successful pull, empty pull, 401 → refresh, 5xx → backoff, malformed JSON. |
| Store | Upsert idempotency under concurrent duplicate delivery; projects sync sets `active = 0` without deleting history. |
| SSE replay | `Last-Event-ID` correctness against `incident_events.id`. |
| **Storm** | **500 log entries, unique `insertId`s, one shared fingerprint → exactly one incident, `occurrence_count = 500`, no suppression-window leakage.** |
| End-to-end | Recorded payloads → fake puller → adapters → process loop → API → asserted dashboard state. |

The storm test is the load-bearing one. It is the difference between fingerprint
suppression working and an error storm becoming an invoice in M2.

---

## 11. Out of scope

Tier 1 classification and any LLM call · budget accounting · `notify` · workspace,
worktrees, and any repository checkout · the `runner` · `verify` · `policy` · `forge` ·
every mutating API route · CSRF · `@sentinel` issue commands · retention and pruning.

---

## 12. Spec amendments owed

The final task of the implementation plan updates `docs/SPEC.md`, mirroring how M0's
last task corrected §13:

1. **§4.2** — "streaming-pull Pub/Sub subscriber" becomes REST long-poll, with §2.1's
   measurements recorded as the rationale.
2. **§4.3.1** — `BuildSanity` marked as landing in M3, with the no-op seam noted.
3. **§4.3.1** — `Unroutable` and `Duplicate` documented as write-boundary enforcement
   rather than chain members (§3.2).
4. **§4.3.2** — replaced by §4.4: the frame-selection ladder and its
   never-broaden rule, per-source fingerprint inputs, and the recorded strategy.
   The single sentence about frames "within the project's own source tree" is not
   implementable without a checkout and must not be left as the specification.
5. **§5** — the `fingerprints.strategy` and `fingerprints.frames_json` columns, and the
   existence of migration `0002` (the schema is no longer one atomic migration).
6. **§6.2** — the `bot`, `triage`, and per-project `fingerprint` registry blocks added.
