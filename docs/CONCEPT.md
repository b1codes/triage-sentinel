# Project Concept Brief: Self-Healing Agent Orchestrator & NOC

## 1. Executive Summary

**Project Name:** `agent-noc` (Working Title)

**Type:** Self-Healing Infrastructure Operations & Agent Orchestrator

**Primary Goal:** Act as an autonomous "On-Call SRE Engine" to monitor, diagnose, and patch runtime errors, GitHub issues, and security vulnerabilities across a multi-tenant portfolio of 25+ micro-applications with near-zero daily human oversight.

**Key Constraints:**

* Runs on a dedicated **M2 Mac Mini (8GB RAM)** background host.
* Strictly limits AI API token spend via local pre-filtering, tier routing, and automated circuit breakers.
* Embedded rich UI dashboard for live incident monitoring, token analytics, diff reviews, and immediate (<2ms) human execution halts.
* Published as a single, open-source, 12-factor-compliant GitHub repository without exposing private infrastructure secrets or keys.

---

## 2. System Architecture & Tech Stack

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        Go Control Plane (NOC)                          │
│                                                                        │
│  ┌───────────────────────┐  ┌─────────────────┐  ┌──────────────────┐  │
│  │ Webhook Ingestion Router│  │ SQLite State &  │  │ Embedded React   │  │
│  │ (GitHub & GCP Loggers) │  │ Cost Accounting │  │ Dashboard (SSE)  │  │
│  └───────────┬───────────┘  └────────┬────────┘  └──────────────────┘  │
└──────────────┼───────────────────────┼─────────────────────────────────┘
               │ Triggers Subprocess   │ Reads/Writes State
               ▼                       ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 Ephemeral Agent Execution Plane (Workers)              │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Python / CLI Scripts (Anthropic / Gemini SDKs, Git, Local Tests) │  │
│  │ Spun up on-demand ───► Executes Task ───► Killed to free RAM     │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘

```

### Core Architecture Layers

1. **Control Plane (Go Core):**
* **Role:** Always-on 24/7 background daemon handling incoming webhooks, rate limiting, task queueing, state persistence, and OS process management.
* **RAM Footprint Target:** ~15–20 MB.
* **Persistence:** SQLite (`modernc.org/sqlite` or `mattn/go-sqlite3`) tracking incidents, token budgets, execution logs, and audit trails.


2. **Execution Plane (Ephemeral Python/CLI Workers):**
* **Role:** Task-specific agent runners invoked via Go `os/exec` when an event passes local triage.
* **Lifecycle:** Spun up on demand to fetch git context, invoke LLM APIs (Claude Code / Gemini API), execute local test suites (`make test`), write patches, and push PRs.
* **Memory Management:** Immediately terminates upon completion, releasing 100% of runtime RAM back to the operating system.


3. **Frontend Dashboard (Embedded React SPA):**
* **Role:** Single-page dashboard for real-time log streaming, token expenditure charts, incident timelines, and 1-click PR approvals or emergency aborts.
* **Deployment:** React static assets compiled and embedded directly into the Go binary via `//go:embed`. Served by the Go HTTP server with zero Node.js process dependencies at runtime.
* **Real-time Communication:** Server-Sent Events (SSE) streaming logs from Go to React in <5ms.



---

## 3. Operational Logic & Safety Guardrails

### A. The Tiered Incident Hierarchy

To eliminate unnecessary API token spend, the engine processes events through strict local filters:

* **Tier 0 (Local Shell - $0.00):** Filters log noise, transient network errors, duplicate webhooks, and executes local lint/build checks without touching an LLM.
* **Tier 1 (Fast LLM - ~$0.01/run):** Leverages low-cost models (e.g., Gemini Flash / Claude Haiku) to parse stack traces and determine if an issue requires code changes.
* **Tier 2 (Reasoning LLM - ~$0.10-$0.50/run):** Invokes high-reasoning models (Claude Sonnet/Opus) to pull local git context, analyze codebase logic, generate patches, and verify using local test suites.
* **Tier 3 (Human-in-the-Loop):** Triggered if an incident hits a hard circuit breaker or requires manual review.

### B. Circuit Breakers & Abort Mechanics

* **Hard Token Cap:** If any single agent task consumes >$1.00 in API tokens without passing unit tests, the Go control plane terminates the process, records the diagnostic state, and escalates to the user.
* **Instant Abort:** UI includes a top-level **Abort** button. Triggering it fires `POST /api/agents/{id}/abort`, causing Go to send `SIGKILL` directly to the OS Process ID (PID) in <2ms.

---

## 4. Repository & Configuration Model

### A. Single Public Repo Strategy

The engine is completely decoupled from target project details via environment variables and dynamic manifests:

* **`.env` / `.env.example`:** API keys, webhook secrets, and budget limits injected at runtime (enforced via `.gitignore`).
* **`projects.yaml`:** Dynamic project registry defining managed repositories, GCP log triggers, and test command entry points (`make test`). A sanitized `projects.example.yaml` is committed to the public repo.
* **Security Automation:** Local `gitleaks` pre-commit hooks and GitHub Push Protection enabled to prevent secret leaks.

### B. Standard Developer Interface (App Contract)

All 25+ managed applications implement a uniform contract:

```makefile
# Standard Makefile target contract expected by the agent orchestrator
test:        # Runs unit and integration test suite
build:       # Compiles or validates application build
healthcheck: # Validates local environment dependencies

```

---

## 5. Coding Agent Instructions & Prompt Target

```text
INSTRUCTIONS FOR THE CODING AGENT:

You are acting as a Senior Principal Systems & Platform Engineer. 

Using the conceptual framework detailed above, please perform the following tasks sequentially:

1. Draft a comprehensive `SPEC.md` file that breaks down this project into functional modules (Ingestion Router, SQLite Schema, Worker Process Manager, React UI Architecture, and SSE Engine).
2. Generate a proposed repository file/directory structure for a Go + Embedded React SPA project.
3. Draft a `CLAUDE.md` development guide for this repository specifying coding standards (Go idioms, React patterns, SQLite conventions) and build/run commands.
4. Provide a preliminary Go `main.go` layout demonstrating how to set up the HTTP router, embed static React assets using `//go:embed`, and establish an SSE endpoint for live log streaming.

Begin by asking any critical clarifying questions or proceed directly to drafting `SPEC.md`.

```
