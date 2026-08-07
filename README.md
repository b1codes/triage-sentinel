# triage-sentinel

A self-hosted control plane that watches your repositories and services, collapses
error storms into single actionable incidents, and — from M2 onward — triages and
repairs them under a hard budget ceiling.

It runs as one static Go binary on a Mac Mini, holds itself to a 15–25 MB RSS target,
and stores everything in a single SQLite file.

## What it does today (M1)

**A live NOC for 25 repositories at $0.00 marginal cost.**

- Pulls GitHub webhook deliveries and GCP Cloud Logging entries over an
  **outbound-initiated** Pub/Sub long poll — nothing inbound reaches the host.
- Normalises both sources into one `Event` shape, and records unroutable events
  rather than dropping them.
- **Fingerprints and suppresses storms.** 500 log entries sharing one root cause
  become one actionable incident with 499 suppressed and the occurrence count
  preserved. Distinct bugs stay distinct — that counterweight is tested, because
  over-collapse silently swallows real failures and nothing else catches it.
- Filters transient noise, quarantined projects, and the sentinel's own commits.
- Serves a live dashboard over SSE with replay from `Last-Event-ID`.

**It does not triage yet.** Incidents that survive Tier 0 come to rest in
`triaging` and stay there; Tier 1 classification arrives in M2. The dashboard says
so plainly rather than implying a queue is being worked.

No LLM API call exists anywhere in the codebase at this milestone.

## Quick start

```bash
# 1. Build (compiles the SPA and embeds it)
make build

# 2. Configure
cp .env.example .env
cp projects.example.yaml projects.yaml
./bin/sentinel hash-password          # paste the result into DASHBOARD_PASSWORD_HASH

# 3. Provision GCP (idempotent; see deploy/gcp/README.md)
export GCP_PROJECT_ID=your-project
./deploy/gcp/topic.sh
./deploy/gcp/subscription.sh
./deploy/gcp/sink.sh <slug> 'severity>=ERROR AND resource.labels.service_name="<slug>"'

# 4. Deploy the webhook relay, then register the printed URL on each repository
#    (content type application/json, events: Workflow runs, Issues)
./deploy/relay/deploy.sh

# 5. Run
./bin/sentinel serve
```

The dashboard listens on `127.0.0.1:8787` by default.

### Required environment

All four are asserted at startup; the binary refuses to serve without them.

| Variable | Why |
|---|---|
| `GCP_PROJECT_ID` | Pub/Sub subscription lookup |
| `PUBSUB_SUBSCRIPTION` | Fully qualified `projects/<p>/subscriptions/<n>` |
| `GITHUB_WEBHOOK_SECRET` | Shared with the relay; every forwarded signature is re-verified |
| `GITHUB_TOKEN` | **Read scope only.** Job-level fingerprinting reads the Actions API, because the `workflow_run` payload does not carry the failing job and step |

Application default credentials need `roles/pubsub.subscriber` and nothing more —
logging sinks are created by the scripts, not by the binary.

## Development

```bash
make check        # fmt-check + vet + test -race — the gate every commit passes
make dev          # run against the Vite dev server instead of embedded assets
```

Two paths let you work without any GCP access:

```bash
# Dashboard-only: start with ingestion explicitly disabled
./bin/sentinel serve --no-ingest

# Feed one recorded payload through the real adapters and process loop
make replay FILE=internal/ingest/testdata/gcplog_text_error.json
```

`--no-ingest` is an explicit opt-out, never a silent skip: starting without
ingestion because credentials happened to be missing is exactly the silent failure
this system is built to notice.

## Layout

| Path | Responsibility |
|---|---|
| `cmd/sentinel/` | CLI, lifecycle wiring, subcommands |
| `internal/config/` | Registry and environment, validated at load |
| `internal/store/` | SQLite schema, migrations, all persistence |
| `internal/triage/` | Normalisation, the fingerprint ladder, Tier 0 |
| `internal/ingest/` | Pub/Sub puller, source adapters, subscriber loop |
| `internal/orchestrator/` | The process loop draining the incident queue |
| `internal/httpapi/` | JSON API, SSE, embedded dashboard |
| `web/` | React + TypeScript dashboard |
| `deploy/relay/` | Cloud Run webhook relay — **a separate Go module** |
| `deploy/gcp/` | Topic, subscription, and per-project sink scripts |

The relay is a separate module on purpose: it needs the Google client libraries
(199 modules, gRPC included), and keeping them out of the root module is what holds
the sentinel binary at ~12 MB with 34 modules and no gRPC at all.

## Milestones

| | Milestone | Status |
|---|---|---|
| M0 | Skeleton — config, store, HTTP, SSE, launchd | ✅ Done |
| M1 | Ingestion — adapters, fingerprinting, suppression, live feed | ✅ Done |
| M2 | Triage & money — Tier 1 classifier, ledger, budget ladder | Next |
| M3 | Repair — workspace, runner, patch application | |
| M4 | Delivery — PRs, auto-merge, deploy and rollback | |
| M5 | Hardening — audit, probation, full observability |  |

See [`docs/SPEC.md`](docs/SPEC.md) for the full specification.

## Licence

See [LICENSE](LICENSE).
