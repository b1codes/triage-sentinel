# M1 — Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the M0 dashboard shell into a live NOC for 25 repositories — real GitHub and GCP events arrive over an outbound-initiated pull, are normalised, deduplicated, fingerprinted and filtered, persist as incidents, and appear in the dashboard within seconds, having spent $0.00 and touched no repository.

**Architecture:** Two loops that share only the database. A **pull loop** long-polls Pub/Sub over REST, normalises each message through a source adapter, writes it durably, then acks — so the ack deadline never interacts with processing time. A **process loop** drains `state='received'` rows through the Tier 0 filter chain and publishes transitions to the SSE bus, so a restart resumes exactly where it left off. Idempotency comes from the existing `incidents(source, source_ref)` UNIQUE index, not from the transport.

**Tech Stack:** Go 1.26 · `golang.org/x/oauth2/google` (the only new root-module dependency) · `modernc.org/sqlite` · stdlib `net/http` + `log/slog` + `crypto/hmac` · React 19 + TypeScript + Vite + `react-router` + `@tanstack/react-query` · Cloud Run relay as a **separate Go module**

**Spec:** [`docs/superpowers/specs/2026-08-02-m1-ingestion-design.md`](../specs/2026-08-02-m1-ingestion-design.md) · [`docs/SPEC.md`](../../SPEC.md) §4.2, §4.3, §4.11, §4.12, §5, §6, §8, §9, §12, §13 — milestone M1 in §14.

## Global Constraints

These carry over from M0 and apply to every task without being restated.

- **Module path:** `github.com/b1codes/triage-sentinel`. Every import uses this prefix verbatim.
- **The root module must not gain gRPC.** M1's design decision §2.1 rests on a measurement: `cloud.google.com/go/pubsub/v2` costs +14 MB of binary and takes the module graph from 32 to ~200, against a 15–25 MB RSS target. The only new root dependency permitted is `golang.org/x/oauth2`. The Cloud Run relay lives in its own module precisely so it can use the Google client libraries without affecting the binary that runs on the Mac Mini.
- **Release builds set `CGO_ENABLED=0`.** Only `modernc.org/sqlite` — never `mattn/go-sqlite3`.
- **No LLM API call may exist anywhere in this milestone.** M1 spends $0.00. Any `anthropic` import is a plan violation.
- **Tests live beside code** as `*_test.go` in the same package directory. No `tests/` tree.
- **Tests are table-driven** with `t.Run(tc.name, ...)` subtests and explicit `if got != want { t.Errorf(...) }`. No assertion library.
- **All errors wrapped** with `fmt.Errorf("doing X: %w", err)`. Package-level sentinels as `var ErrX = errors.New(...)`. Never match on error strings.
- **No mutable package-level state.** Dependencies pass through constructors. Immutable lookup tables are unexported with an exported accessor.
- **Every exported identifier gets a doc comment starting with its own name.**
- **`store` imports nothing but `config`.** This is load-bearing in M1: `store` must **not** import `bus`, so replay queries return `store` types and `httpapi` maps them to `bus.Event`.
- **No package imports `httpapi`**; `httpapi` imports everything.
- **Timezone comes from `env.Location`.** No call site uses `time.Local`.
- **All timestamps are stored as RFC3339 UTC strings**, matching the M0 schema's `TEXT` columns.
- **`make check`** (`fmt-check` + `vet` + `test -race`) is the gate on every commit.
- **Commit after every task** using the exact message given in the task's final step.

## Scope notes

Four decisions from the design doc that this plan implements and that SPEC §14's one-line M1 summary does not spell out:

1. **REST long-poll, not streaming pull** (design §2.1). SPEC §4.2 is amended in Task 21.
2. **`BuildSanity` ships as a no-op seam** (design §2.3). It needs a checkout and subprocess supervision that arrive in M3. SPEC §4.3.1 is amended in Task 21.
3. **`Unroutable` and `Duplicate` are enforced at the write boundary**, not as chain members (design §3.2) — by routing and by the existing UNIQUE index respectively.
4. **M1 is read-only.** Every mutating route in SPEC §8 belongs to a later milestone. This keeps "spent $0.00 and changed nothing" literally true, and lets CSRF stay deferred exactly as M0 planned.

**Where incidents come to rest:** an incident surviving Tier 0 enters `triaging` and stays there. M1 has no Tier 1. The Overview displays that count plainly rather than implying a queue is being worked.

## File Structure

| Path | Responsibility |
|---|---|
| `internal/config/registry.go` | **Modify** — `Bot`, `Triage`, `FingerprintConfig` types; `Project.Fingerprint` |
| `internal/config/validate.go` | **Modify** — bot email, regex compilation, source-root rules |
| `internal/config/effective.go` | **Modify** — `EffectiveProject.SourceRoots`, `.Fingerprint` |
| `internal/store/migrations/0002_fingerprint_evidence.sql` | `strategy` + `frames_json` on `fingerprints` |
| `internal/store/projects.go` | Registry → `projects` table sync; project reads |
| `internal/store/incidents.go` | Upsert-on-conflict write path; list and get |
| `internal/store/events.go` | `incident_events` append, state transition, replay query |
| `internal/store/fingerprints.go` | Suppression-window read/write |
| `internal/triage/normalize.go` | Path, line-number, address, UUID, timestamp scrubbing |
| `internal/triage/fingerprint.go` | The §4.4.1 frame-selection ladder |
| `internal/triage/tier0.go` | `Filter` type, ordered chain, `Decision` |
| `internal/triage/transient.go` | Compiled transient regex set |
| `internal/ingest/event.go` | `Event`, `Adapter`, `ErrIgnore` |
| `internal/ingest/router.go` | Adapter selection; slug resolution |
| `internal/ingest/github.go` | `workflow_run` / `issues` normalisation; HMAC re-verification |
| `internal/ingest/gcplog.go` | Cloud Logging entry normalisation |
| `internal/ingest/pull.go` | `Puller` interface; REST long-poll; oauth2; backoff |
| `internal/ingest/subscriber.go` | Pull → normalise → persist → ack loop; staleness cursor |
| `internal/orchestrator/orchestrator.go` | The process loop |
| `internal/httpapi/incidents.go` | `/api/incidents`, `/api/incidents/{id}` |
| `internal/httpapi/overview.go` | `/api/overview`, `/api/projects` |
| `internal/httpapi/replay.go` | `store` events → `bus.Event` adapter |
| `cmd/sentinel/run.go` | **Modify** — lifecycle wiring, `--no-ingest`, `replay` subcommand |
| `web/src/` | Router, QueryClient, layout, SSE hook, views |
| `deploy/relay/` | Separate Go module: HMAC verify → publish |
| `deploy/gcp/` | Topic, subscription, per-project sink scripts |

---

### Task 1: Registry additions — `bot`, `triage`, and per-project `fingerprint`

**Files:**
- Modify: `internal/config/registry.go`
- Modify: `internal/config/validate.go`
- Modify: `internal/config/effective.go`
- Modify: `internal/config/testdata/valid.yaml`
- Modify: `projects.example.yaml`
- Test: `internal/config/registry_test.go`, `internal/config/validate_test.go`, `internal/config/effective_test.go`

**Interfaces:**
- Consumes: `config.Registry`, `config.Project`, `config.EffectiveProject`, `config.ErrInvalidRegistry`, `config.decodeStructStrict` (all existing).
- Produces:
  - `config.Bot` — `struct{ Name, Email string }`, yaml `name`, `email`.
  - `config.Triage` — `struct{ TransientPatterns []string }`, yaml `transient_patterns`.
  - `config.FingerprintConfig` — `struct{ SourceRoots []string }`, yaml `source_roots`.
  - `config.Registry.Bot Bot`, `config.Registry.Triage Triage`, `config.Project.Fingerprint *FingerprintConfig`.
  - `config.EffectiveProject.SourceRoots []string`.
  - `config.CompileTransientPatterns(patterns []string) ([]*regexp.Regexp, error)`.

**Why the custom unmarshaller matters here.** `Project` has a hand-written `UnmarshalYAML` with an explicit `switch` over field names and a `default:` that errors on anything unknown (`registry.go:309`). Adding a struct field alone is **not** enough — a `fingerprint:` key would be rejected as unknown. The switch must gain a case. The existing comment on that method says exactly this: *"This switch must stay in sync with Project struct fields."*

- [ ] **Step 1: Write the failing parse test**

Add to `internal/config/registry_test.go`:

```go
func TestParseRegistryM1Blocks(t *testing.T) {
	reg, err := ParseRegistry(readTestdata(t, "valid.yaml"))
	if err != nil {
		t.Fatalf("ParseRegistry() error = %v, want nil", err)
	}

	t.Run("bot identity", func(t *testing.T) {
		if got, want := reg.Bot.Email, "sentinel@example.invalid"; got != want {
			t.Errorf("Bot.Email = %q, want %q", got, want)
		}
		if got, want := reg.Bot.Name, "triage-sentinel"; got != want {
			t.Errorf("Bot.Name = %q, want %q", got, want)
		}
	})

	t.Run("transient patterns", func(t *testing.T) {
		if len(reg.Triage.TransientPatterns) == 0 {
			t.Fatal("Triage.TransientPatterns is empty, want the default set")
		}
	})

	t.Run("per-project source roots", func(t *testing.T) {
		if reg.Projects[0].Fingerprint == nil {
			t.Fatal("Projects[0].Fingerprint = nil, want declared source_roots")
		}
		got := reg.Projects[0].Fingerprint.SourceRoots
		want := []string{"cmd/", "internal/"}
		if len(got) != len(want) {
			t.Fatalf("SourceRoots = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("SourceRoots[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("absent fingerprint block stays nil", func(t *testing.T) {
		if reg.Projects[1].Fingerprint != nil {
			t.Error("Projects[1].Fingerprint is non-nil; an omitted block must stay nil so the denylist strategy is chosen")
		}
	})
}

func TestParseRegistryRejectsUnknownFingerprintField(t *testing.T) {
	const y = `
version: 1
projects:
  - slug: example-api
    fingerprint:
      source_rootz: ["src/"]
`
	_, err := ParseRegistry([]byte(y))
	if err == nil {
		t.Fatal("ParseRegistry() error = nil, want error for a mistyped key")
	}
	if !strings.Contains(err.Error(), "source_rootz") {
		t.Errorf("error %q does not name the offending key", err.Error())
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/config/ -run 'TestParseRegistryM1Blocks|TestParseRegistryRejectsUnknownFingerprintField' -v`

Expected: FAIL — `reg.Bot undefined`, `reg.Triage undefined`, `Projects[0].Fingerprint undefined`.

- [ ] **Step 3: Add the types and wire the unmarshaller**

In `internal/config/registry.go`, add above the `Project` type:

```go
// Bot is the sentinel's own git identity. The Tier 0 SelfInflicted filter
// matches Email so the system cannot enter a self-referential repair loop
// (SPEC §4.3.1, §4.9).
type Bot struct {
	Name  string `yaml:"name"`
	Email string `yaml:"email"`
}

// Triage holds Tier 0 tuning that is not per-project.
type Triage struct {
	// TransientPatterns are compiled at load, so a malformed regex refuses
	// startup rather than failing on the first real event.
	TransientPatterns []string `yaml:"transient_patterns"`
}

// FingerprintConfig declares which paths belong to a project's own source
// tree. When present it selects the strongest frame-selection strategy
// (design §4.4.1 step 1); when absent the fingerprinter falls back to the
// dependency denylist.
type FingerprintConfig struct {
	SourceRoots []string `yaml:"source_roots"`
}

// fingerprintKnownFields backs strict decoding of the per-project block.
var fingerprintKnownFields = map[string]bool{"source_roots": true}
```

Add the field to `Project` (after `Env`):

```go
	// Fingerprint is nil when the project declares no source roots, which is
	// meaningful rather than merely empty: it selects the denylist strategy.
	Fingerprint *FingerprintConfig `yaml:"fingerprint"`
```

Add a case to the `Project.UnmarshalYAML` switch, immediately before `default:`:

```go
		case "fingerprint":
			var fp FingerprintConfig
			if err := decodeStructStrict(valNode, fingerprintKnownFields, &fp); err != nil {
				return fmt.Errorf("fingerprint: %w", err)
			}
			p.Fingerprint = &fp
```

Add the two top-level fields to `Registry`, after `Runtime`:

```go
	Bot      Bot             `yaml:"bot"`
	Triage   Triage          `yaml:"triage"`
```

- [ ] **Step 4: Update the fixture**

In `internal/config/testdata/valid.yaml`, add two top-level blocks after `runtime:`:

```yaml
bot:
  name: triage-sentinel
  email: sentinel@example.invalid

triage:
  transient_patterns:
    - "(?i)connection reset by peer"
    - "(?i)ECONNRESET"
    - "(?i)rate limit"
    - "(?i)the runner has received a shutdown signal"
    - "(?i)the operation was canceled"
```

And add to the **first** project (`example-api`) only, so the second exercises the nil case:

```yaml
    fingerprint:
      source_roots:
        - "cmd/"
        - "internal/"
```

- [ ] **Step 5: Run the parse tests to verify they pass**

Run: `go test ./internal/config/ -run 'TestParseRegistry' -v`

Expected: PASS, including the pre-existing `TestParseRegistryValid`.

- [ ] **Step 6: Write the failing validation test**

Add to `internal/config/validate_test.go`. Note `baseRegistry()` already exists in that file; these cases mutate it:

```go
func TestValidateM1Rules(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*Registry)
		wantText string
	}{
		{
			name:     "bot email missing",
			mutate:   func(r *Registry) { r.Bot.Email = "" },
			wantText: "bot.email",
		},
		{
			name:     "bot email not an address",
			mutate:   func(r *Registry) { r.Bot.Email = "not-an-email" },
			wantText: "bot.email",
		},
		{
			name:     "transient pattern does not compile",
			mutate:   func(r *Registry) { r.Triage.TransientPatterns = []string{"([unclosed"} },
			wantText: "transient_patterns",
		},
		{
			name: "source root is absolute",
			mutate: func(r *Registry) {
				r.Projects[0].Fingerprint = &FingerprintConfig{SourceRoots: []string{"/etc/"}}
			},
			wantText: "source_roots",
		},
		{
			name: "source root escapes the repo",
			mutate: func(r *Registry) {
				r.Projects[0].Fingerprint = &FingerprintConfig{SourceRoots: []string{"../other/"}}
			},
			wantText: "source_roots",
		},
		{
			name: "source root is empty",
			mutate: func(r *Registry) {
				r.Projects[0].Fingerprint = &FingerprintConfig{SourceRoots: []string{"  "}}
			},
			wantText: "source_roots",
		},
		{
			name: "fingerprint block present but declares nothing",
			mutate: func(r *Registry) {
				r.Projects[0].Fingerprint = &FingerprintConfig{}
			},
			wantText: "source_roots",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := baseRegistry()
			reg.Bot = Bot{Name: "triage-sentinel", Email: "sentinel@example.invalid"}
			tc.mutate(&reg)

			err := reg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
			if !errors.Is(err, ErrInvalidRegistry) {
				t.Errorf("errors.Is(err, ErrInvalidRegistry) = false, want true (err = %v)", err)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.wantText)
			}
		})
	}
}

func TestCompileTransientPatterns(t *testing.T) {
	t.Run("compiles and matches case-insensitively", func(t *testing.T) {
		res, err := CompileTransientPatterns([]string{"(?i)ECONNRESET"})
		if err != nil {
			t.Fatalf("CompileTransientPatterns() error = %v, want nil", err)
		}
		if len(res) != 1 {
			t.Fatalf("len = %d, want 1", len(res))
		}
		if !res[0].MatchString("read tcp: econnreset") {
			t.Error("compiled pattern did not match a lowercase occurrence")
		}
	})

	t.Run("names the offending pattern", func(t *testing.T) {
		_, err := CompileTransientPatterns([]string{"(?i)fine", "([unclosed"})
		if err == nil {
			t.Fatal("error = nil, want error")
		}
		if !strings.Contains(err.Error(), "([unclosed") {
			t.Errorf("error %q does not quote the bad pattern", err.Error())
		}
	})
}
```

- [ ] **Step 7: Run it to verify it fails**

Run: `go test ./internal/config/ -run 'TestValidateM1Rules|TestCompileTransientPatterns' -v`

Expected: FAIL — `undefined: CompileTransientPatterns`, and the validation subtests fail because no rule rejects them yet.

- [ ] **Step 8: Implement validation**

In `internal/config/validate.go`, add:

```go
// CompileTransientPatterns compiles the Tier 0 transient regex set. It is
// called both by Validate — so a malformed pattern refuses startup rather
// than failing on the first real event — and by the triage package at
// construction, so the two can never disagree about what compiles.
func CompileTransientPatterns(patterns []string) ([]*regexp.Regexp, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("compiling transient pattern %q: %w", p, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}

// validateBot checks the sentinel's own git identity. The address is
// load-bearing: the Tier 0 SelfInflicted filter matches on it, so an empty
// or malformed value would silently disable the loop-prevention guard.
func (r Registry) validateBot() []error {
	var problems []error

	if strings.TrimSpace(r.Bot.Email) == "" {
		problems = append(problems, errors.New(
			"bot.email is required; the SelfInflicted filter matches it to prevent self-repair loops"))
		return problems
	}
	if _, err := mail.ParseAddress(r.Bot.Email); err != nil {
		problems = append(problems, fmt.Errorf("bot.email %q is not a valid address: %w", r.Bot.Email, err))
	}
	return problems
}

// validateTriage compiles every transient pattern, discarding the result.
func (r Registry) validateTriage() []error {
	if _, err := CompileTransientPatterns(r.Triage.TransientPatterns); err != nil {
		return []error{fmt.Errorf("triage.transient_patterns: %w", err)}
	}
	return nil
}

// validateSourceRoots enforces that a declared root can actually match a
// relative frame path. A root that can never match would silently demote the
// project to the weaker denylist strategy, and the point of the ladder is
// that the strategy in use is known rather than assumed.
func validateSourceRoots(slug string, fp *FingerprintConfig) []error {
	if fp == nil {
		return nil // absent is valid: it selects the denylist strategy
	}

	var problems []error
	if len(fp.SourceRoots) == 0 {
		problems = append(problems, fmt.Errorf(
			"project %q declares a fingerprint block with no source_roots; omit the block entirely to use the denylist", slug))
		return problems
	}

	for _, root := range fp.SourceRoots {
		trimmed := strings.TrimSpace(root)
		switch {
		case trimmed == "":
			problems = append(problems, fmt.Errorf("project %q: source_roots contains an empty entry", slug))
		case path.IsAbs(trimmed) || strings.HasPrefix(trimmed, "/"):
			problems = append(problems, fmt.Errorf(
				"project %q: source_roots entry %q must be relative to the repository root", slug, root))
		case trimmed == ".." || strings.HasPrefix(trimmed, "../") || strings.Contains(trimmed, "/../"):
			problems = append(problems, fmt.Errorf(
				"project %q: source_roots entry %q must not traverse outside the repository", slug, root))
		}
	}
	return problems
}
```

Add the imports `net/mail`, `path`, and `regexp` to the file's import block.

Wire the two registry-level checks into `Validate` by appending their results alongside the existing `validateBudgets` / `validateSoftMode` / `validateDefaults` / `validateRuntime` / `validateProjects` calls, and call `validateSourceRoots(p.Slug, p.Fingerprint)` from inside `validateProjects`'s per-project loop.

- [ ] **Step 9: Run the validation tests to verify they pass**

Run: `go test ./internal/config/ -v`

Expected: PASS, including every pre-existing test. If `TestValidateAcceptsBaseRegistry` now fails, `baseRegistry()` needs a `Bot` — add `Bot: Bot{Name: "triage-sentinel", Email: "sentinel@example.invalid"}` to it rather than weakening the rule.

- [ ] **Step 10: Surface source roots on EffectiveProject**

Add to the `EffectiveProject` struct in `internal/config/effective.go`, after `ProtectedPaths`:

```go
	// SourceRoots is empty when the project declares none, which selects the
	// denylist frame-selection strategy rather than meaning "no frames".
	SourceRoots []string
```

And inside `EffectiveProject`, after the `if p.AllowTestChanges != nil` block:

```go
		if p.Fingerprint != nil {
			eff.SourceRoots = append([]string(nil), p.Fingerprint.SourceRoots...)
		}
```

Add to `internal/config/effective_test.go`:

```go
func TestEffectiveProjectSourceRoots(t *testing.T) {
	reg, err := ParseRegistry(readTestdata(t, "valid.yaml"))
	if err != nil {
		t.Fatalf("ParseRegistry() error = %v, want nil", err)
	}

	t.Run("declared roots are resolved", func(t *testing.T) {
		eff, ok := reg.EffectiveProject("example-api")
		if !ok {
			t.Fatal("EffectiveProject(example-api) not found")
		}
		if len(eff.SourceRoots) != 2 {
			t.Fatalf("SourceRoots = %v, want 2 entries", eff.SourceRoots)
		}
	})

	t.Run("absent roots resolve to empty not nil-panic", func(t *testing.T) {
		eff, ok := reg.EffectiveProject("example-worker")
		if !ok {
			t.Fatal("EffectiveProject(example-worker) not found")
		}
		if len(eff.SourceRoots) != 0 {
			t.Errorf("SourceRoots = %v, want empty", eff.SourceRoots)
		}
	})

	t.Run("caller cannot corrupt the registry", func(t *testing.T) {
		eff, _ := reg.EffectiveProject("example-api")
		eff.SourceRoots[0] = "clobbered"
		again, _ := reg.EffectiveProject("example-api")
		if again.SourceRoots[0] == "clobbered" {
			t.Error("EffectiveProject returns a shared slice; a caller can corrupt the registry")
		}
	})
}
```

- [ ] **Step 11: Run the effective-project tests**

Run: `go test ./internal/config/ -run TestEffectiveProject -v`

Expected: PASS.

- [ ] **Step 12: Update `projects.example.yaml`**

Add the same `bot:` and `triage:` blocks used in the fixture, and add a commented `fingerprint:` block to the example project so an operator sees the option:

```yaml
    # Optional. Declaring source roots selects the strongest fingerprint
    # grouping strategy. Omit the block to fall back to a dependency-directory
    # denylist. See docs/SPEC.md §4.3.2.
    fingerprint:
      source_roots:
        - "cmd/"
        - "internal/"
```

- [ ] **Step 13: Verify the example file actually loads**

Run: `go run ./cmd/sentinel validate -config projects.example.yaml -env-file /dev/null 2>&1 | head -20`

Expected: it fails on `DASHBOARD_PASSWORD_HASH` (an env problem, not a registry one). That is the correct outcome — it proves the registry parsed and validated before env checking rejected it. If instead you see `unknown field` or `source_roots`, the example file is wrong.

- [ ] **Step 14: Run the gate and commit**

Run: `make check`

```bash
git add internal/config projects.example.yaml
git commit -m "feat(config): bot identity, transient patterns, and fingerprint source roots

Three registry additions M1 needs, all validated at load:

- bot.email is required and must parse as an address. The Tier 0
  SelfInflicted filter matches on it, so an empty value would silently
  disable the self-repair-loop guard rather than fail loudly.
- triage.transient_patterns compile at load via CompileTransientPatterns,
  which the triage package also calls at construction so the two cannot
  disagree about what compiles.
- Per-project fingerprint.source_roots selects the strongest frame
  selection strategy. A present-but-empty block is an error: omitting it
  means denylist, and a root that can never match would silently demote
  the project without anyone knowing which strategy was in use.

Project.UnmarshalYAML gains a fingerprint case. The struct field alone is
not enough — that switch rejects unknown keys by default, exactly as its
existing comment warns.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: Migration `0002` — fingerprint evidence columns

**Files:**
- Create: `internal/store/migrations/0002_fingerprint_evidence.sql`
- Test: `internal/store/migrate_test.go`

**Interfaces:**
- Consumes: `store.Migrate`, `store.SchemaVersion`, `store.Open` (existing).
- Produces: `fingerprints.strategy TEXT NOT NULL DEFAULT 'unknown'` and `fingerprints.frames_json TEXT NOT NULL DEFAULT '[]'`.

**Why a second migration exists at all.** M0's scope note argued that shipping the complete SPEC §5 schema as one atomic `0001` would let M1 land "without schema churn". That held for every table M1 *writes*; it did not anticipate needing to record *why* a fingerprint grouped what it did. Editing `0001` is not an option — it is already applied. The runner already globs and sorts `migrations/*.sql` (`migrate.go:19`, `:169`), so no runner change is needed.

- [ ] **Step 1: Write the failing test**

Add to `internal/store/migrate_test.go`:

```go
func TestMigrate0002AddsFingerprintEvidence(t *testing.T) {
	db := openTestDB(t)

	if _, err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate() error = %v, want nil", err)
	}

	version, err := SchemaVersion(context.Background(), db)
	if err != nil {
		t.Fatalf("SchemaVersion() error = %v, want nil", err)
	}
	if version < 2 {
		t.Fatalf("SchemaVersion() = %d, want at least 2", version)
	}

	t.Run("columns exist with safe defaults", func(t *testing.T) {
		_, err := db.Writer().Exec(`
			INSERT INTO projects (slug, repo, default_branch, created_at, updated_at)
			VALUES ('p', 'github.com/o/p', 'main', '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z')`)
		if err != nil {
			t.Fatalf("seeding project: %v", err)
		}
		_, err = db.Writer().Exec(`
			INSERT INTO incidents (project_slug, source, source_ref, kind, title, state, occurred_at, created_at, updated_at)
			VALUES ('p', 'gcplog', 'gcplog:1', 'log.error', 't', 'received', '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z')`)
		if err != nil {
			t.Fatalf("seeding incident: %v", err)
		}
		// Deliberately omit strategy and frames_json to prove the defaults apply.
		_, err = db.Writer().Exec(`
			INSERT INTO fingerprints (fingerprint, project_slug, first_incident_id, last_seen_at, suppress_until)
			VALUES ('abc', 'p', 1, '2026-08-02T00:00:00Z', '2026-08-02T06:00:00Z')`)
		if err != nil {
			t.Fatalf("inserting fingerprint: %v", err)
		}

		var strategy, frames string
		err = db.Reader().QueryRow(
			`SELECT strategy, frames_json FROM fingerprints WHERE fingerprint = 'abc'`,
		).Scan(&strategy, &frames)
		if err != nil {
			t.Fatalf("selecting evidence columns: %v", err)
		}
		if strategy != "unknown" {
			t.Errorf("strategy default = %q, want %q", strategy, "unknown")
		}
		if frames != "[]" {
			t.Errorf("frames_json default = %q, want %q", frames, "[]")
		}
	})
}
```

If `openTestDB` does not already exist in the package's tests, add it:

```go
func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/store/ -run TestMigrate0002 -v`

Expected: FAIL — `SchemaVersion() = 1, want at least 2`.

- [ ] **Step 3: Write the migration**

Create `internal/store/migrations/0002_fingerprint_evidence.sql`:

```sql
-- Records how each fingerprint was derived, so grouping quality can be tuned
-- from evidence. M1 spends $0.00, which makes it a free observation period:
-- after real traffic the dashboard can show which strategy a project fell
-- back to and which frames grouped what, and source_roots can be tuned before
-- M2 begins spending against these groupings.
--
-- Forward-only: never edit this file after it has been applied anywhere.

-- One of: source_roots | denylist | all_frames | workflow | no_frames.
-- 'unknown' is the default only for rows written before this migration.
ALTER TABLE fingerprints ADD COLUMN strategy TEXT NOT NULL DEFAULT 'unknown';

-- The normalised frames that produced the hash, as a JSON array of strings.
-- Without these it is impossible to tell why two errors collapsed, only that
-- they did.
ALTER TABLE fingerprints ADD COLUMN frames_json TEXT NOT NULL DEFAULT '[]';
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/store/ -run TestMigrate0002 -v`

Expected: PASS.

- [ ] **Step 5: Confirm migrations remain idempotent**

Run: `go test ./internal/store/ -v`

Expected: PASS. The pre-existing test that applies migrations twice and asserts the second run applies zero must still pass — if it now reports 2 applied on a second run, the runner is not recording `0002` in `schema_migrations`.

- [ ] **Step 6: Run the gate and commit**

Run: `make check`

```bash
git add internal/store/migrations/0002_fingerprint_evidence.sql internal/store/migrate_test.go
git commit -m "feat(store): record fingerprint strategy and frames

Adds fingerprints.strategy and fingerprints.frames_json so grouping
decisions carry their own evidence. Without the frames it is impossible
to tell why two errors collapsed, only that they did — and over-collapse
is the failure mode nothing else in the system catches.

This knowingly contradicts M0's 'complete schema in one atomic 0001'
rationale. That held for every table M1 writes; it did not anticipate
needing to record why a fingerprint grouped what it did. Forward-only
migrations exist for exactly this, and 0001 is already applied.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: `store` — projects table sync

**Files:**
- Create: `internal/store/projects.go`
- Test: `internal/store/projects_test.go`

**Interfaces:**
- Consumes: `store.DB`, `store.Open`, `store.Migrate`.
- Produces:
  - `store.ProjectRow` — `struct{ Slug, Repo, DefaultBranch string }`, the registry's view.
  - `store.Project` — `struct{ Slug, Repo, DefaultBranch string; Active, Quarantined bool; QuarantineReason string; ConsecutiveFailures, IncidentsSeen int }`, the database's view.
  - `store.SyncProjects(ctx context.Context, db *DB, rows []ProjectRow, now time.Time) error`
  - `store.ListProjects(ctx context.Context, db *DB) ([]Project, error)`
  - `store.GetProject(ctx context.Context, db *DB, slug string) (Project, bool, error)`

**Why this must exist before anything writes an incident.** `store.Open` verifies `foreign_keys(1)` (`store.go:104`), and `incidents.project_slug REFERENCES projects(slug)`. Until the `projects` table is populated from `projects.yaml`, **every incident insert fails on a foreign-key violation**. This is the first thing to build and the easiest to forget.

- [ ] **Step 1: Write the failing test**

Create `internal/store/projects_test.go`:

```go
package store

import (
	"context"
	"testing"
	"time"
)

func syncedDB(t *testing.T) (*DB, time.Time) {
	t.Helper()
	db := openTestDB(t)
	if _, err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate() error = %v, want nil", err)
	}
	return db, time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
}

func TestSyncProjectsInsertsAndUpdates(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	rows := []ProjectRow{
		{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"},
		{Slug: "worker", Repo: "github.com/o/worker", DefaultBranch: "main"},
	}
	if err := SyncProjects(ctx, db, rows, now); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	got, err := ListProjects(ctx, db)
	if err != nil {
		t.Fatalf("ListProjects() error = %v, want nil", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(ListProjects()) = %d, want 2", len(got))
	}
	for _, p := range got {
		if !p.Active {
			t.Errorf("project %q Active = false, want true", p.Slug)
		}
	}

	t.Run("re-sync updates repo without duplicating", func(t *testing.T) {
		rows[0].Repo = "github.com/o/api-renamed"
		if err := SyncProjects(ctx, db, rows, now.Add(time.Hour)); err != nil {
			t.Fatalf("SyncProjects() error = %v, want nil", err)
		}
		again, err := ListProjects(ctx, db)
		if err != nil {
			t.Fatalf("ListProjects() error = %v, want nil", err)
		}
		if len(again) != 2 {
			t.Fatalf("len = %d, want 2; sync must upsert rather than insert", len(again))
		}
		p, ok, err := GetProject(ctx, db, "api")
		if err != nil || !ok {
			t.Fatalf("GetProject(api) = %v, %v, want found", ok, err)
		}
		if p.Repo != "github.com/o/api-renamed" {
			t.Errorf("Repo = %q, want the updated value", p.Repo)
		}
	})
}

func TestSyncProjectsDeactivatesRatherThanDeletes(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	both := []ProjectRow{
		{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"},
		{Slug: "worker", Repo: "github.com/o/worker", DefaultBranch: "main"},
	}
	if err := SyncProjects(ctx, db, both, now); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	// worker is removed from the registry.
	if err := SyncProjects(ctx, db, both[:1], now.Add(time.Hour)); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	worker, ok, err := GetProject(ctx, db, "worker")
	if err != nil {
		t.Fatalf("GetProject() error = %v, want nil", err)
	}
	if !ok {
		t.Fatal("worker row was deleted; SPEC §4.1 requires incident history to survive deregistration")
	}
	if worker.Active {
		t.Error("worker Active = true, want false after removal from the registry")
	}

	t.Run("re-adding reactivates", func(t *testing.T) {
		if err := SyncProjects(ctx, db, both, now.Add(2*time.Hour)); err != nil {
			t.Fatalf("SyncProjects() error = %v, want nil", err)
		}
		w, _, err := GetProject(ctx, db, "worker")
		if err != nil {
			t.Fatalf("GetProject() error = %v, want nil", err)
		}
		if !w.Active {
			t.Error("worker Active = false, want true after being re-registered")
		}
	})
}

func TestSyncProjectsPreservesQuarantine(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	rows := []ProjectRow{{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"}}
	if err := SyncProjects(ctx, db, rows, now); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	// A breaker quarantines the project (M2 owns the breaker; this simulates it).
	_, err := db.Writer().ExecContext(ctx,
		`UPDATE projects SET quarantined = 1, quarantine_reason = 'consecutive_failures' WHERE slug = 'api'`)
	if err != nil {
		t.Fatalf("quarantining: %v", err)
	}

	if err := SyncProjects(ctx, db, rows, now.Add(time.Hour)); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	p, _, err := GetProject(ctx, db, "api")
	if err != nil {
		t.Fatalf("GetProject() error = %v, want nil", err)
	}
	if !p.Quarantined {
		t.Error("Quarantined = false; a SIGHUP reload must not silently clear a breaker")
	}
	if p.QuarantineReason != "consecutive_failures" {
		t.Errorf("QuarantineReason = %q, want it preserved", p.QuarantineReason)
	}
}

func TestSyncProjectsIsAtomic(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	// A duplicate slug makes the second insert fail. Nothing may persist.
	rows := []ProjectRow{
		{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"},
		{Slug: "api", Repo: "", DefaultBranch: ""},
	}
	_ = SyncProjects(ctx, db, rows, now)

	got, err := ListProjects(ctx, db)
	if err != nil {
		t.Fatalf("ListProjects() error = %v, want nil", err)
	}
	if len(got) == 1 && got[0].Repo == "" {
		t.Error("a partial sync persisted; SyncProjects must run in one transaction")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/store/ -run TestSyncProjects -v`

Expected: FAIL — `undefined: ProjectRow`, `undefined: SyncProjects`, `undefined: ListProjects`, `undefined: GetProject`.

- [ ] **Step 3: Write the implementation**

Create `internal/store/projects.go`:

```go
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrSyncProjects is returned when the registry cannot be reconciled with the
// projects table.
var ErrSyncProjects = errors.New("syncing projects")

// ProjectRow is the registry's view of a project — the fields projects.yaml
// owns. Everything else in the projects table is state the sentinel maintains.
type ProjectRow struct {
	Slug          string
	Repo          string
	DefaultBranch string
}

// Project is the database's view of a project, including operational state the
// registry does not describe.
type Project struct {
	Slug                string
	Repo                string
	DefaultBranch       string
	Active              bool
	Quarantined         bool
	QuarantineReason    string
	ConsecutiveFailures int
	IncidentsSeen       int
}

// SyncProjects reconciles the projects table with the registry. Registered
// slugs are upserted with active = 1; rows whose slug is no longer registered
// are set to active = 0.
//
// Rows are never deleted. SPEC §4.1 requires that removing a project from the
// registry preserves its incident history, and incidents.project_slug is a
// foreign key into this table — a delete would either fail or orphan history.
//
// The upsert deliberately touches only the registry-owned columns. Quarantine
// state and failure counters are maintained by breakers, and a SIGHUP reload
// must not silently clear a breaker that is holding a project back.
//
// The whole reconciliation runs in one transaction, so a malformed registry
// cannot leave the table half-updated.
func SyncProjects(ctx context.Context, db *DB, rows []ProjectRow, now time.Time) error {
	ts := now.UTC().Format(time.RFC3339)

	tx, err := db.Writer().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: beginning transaction: %w", ErrSyncProjects, err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, r := range rows {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO projects (slug, repo, default_branch, active, created_at, updated_at)
			VALUES (?, ?, ?, 1, ?, ?)
			ON CONFLICT(slug) DO UPDATE SET
				repo           = excluded.repo,
				default_branch = excluded.default_branch,
				active         = 1,
				updated_at     = excluded.updated_at`,
			r.Slug, r.Repo, r.DefaultBranch, ts, ts)
		if err != nil {
			return fmt.Errorf("%w: upserting %q: %w", ErrSyncProjects, r.Slug, err)
		}
	}

	if err := deactivateUnlisted(ctx, tx, rows, ts); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: committing: %w", ErrSyncProjects, err)
	}
	return nil
}

// deactivateUnlisted sets active = 0 on every row whose slug is absent from
// rows. It builds the NOT IN list with placeholders rather than string
// interpolation, so a slug can never be injected into the statement.
func deactivateUnlisted(ctx context.Context, tx *sql.Tx, rows []ProjectRow, ts string) error {
	if len(rows) == 0 {
		_, err := tx.ExecContext(ctx,
			`UPDATE projects SET active = 0, updated_at = ? WHERE active = 1`, ts)
		if err != nil {
			return fmt.Errorf("%w: deactivating all: %w", ErrSyncProjects, err)
		}
		return nil
	}

	args := make([]any, 0, len(rows)+1)
	args = append(args, ts)
	placeholders := make([]byte, 0, len(rows)*2)
	for i, r := range rows {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, r.Slug)
	}

	query := `UPDATE projects SET active = 0, updated_at = ?
	          WHERE active = 1 AND slug NOT IN (` + string(placeholders) + `)`

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%w: deactivating unlisted: %w", ErrSyncProjects, err)
	}
	return nil
}

const projectColumns = `slug, repo, default_branch, active, quarantined,
	COALESCE(quarantine_reason, ''), consecutive_failures, incidents_seen`

func scanProject(scan func(...any) error) (Project, error) {
	var p Project
	err := scan(&p.Slug, &p.Repo, &p.DefaultBranch, &p.Active, &p.Quarantined,
		&p.QuarantineReason, &p.ConsecutiveFailures, &p.IncidentsSeen)
	return p, err
}

// ListProjects returns every project, active or not, ordered by slug.
func ListProjects(ctx context.Context, db *DB) ([]Project, error) {
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT `+projectColumns+` FROM projects ORDER BY slug`)
	if err != nil {
		return nil, fmt.Errorf("listing projects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Project
	for rows.Next() {
		p, err := scanProject(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scanning project: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating projects: %w", err)
	}
	return out, nil
}

// GetProject returns one project by slug, reporting false when absent.
func GetProject(ctx context.Context, db *DB, slug string) (Project, bool, error) {
	row := db.Reader().QueryRowContext(ctx,
		`SELECT `+projectColumns+` FROM projects WHERE slug = ?`, slug)

	p, err := scanProject(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Project{}, false, nil
	}
	if err != nil {
		return Project{}, false, fmt.Errorf("getting project %q: %w", slug, err)
	}
	return p, true, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/store/ -run TestSyncProjects -v`

Expected: PASS — all four tests including the atomicity and quarantine-preservation cases.

- [ ] **Step 5: Run the gate and commit**

Run: `make check`

```bash
git add internal/store/projects.go internal/store/projects_test.go
git commit -m "feat(store): reconcile the projects table with the registry

Nothing can write an incident until this exists: foreign_keys is verified
on at open and incidents.project_slug references projects(slug), so every
insert would fail until the table is populated.

Deregistering a project sets active = 0 rather than deleting the row —
SPEC §4.1 requires incident history to survive, and the foreign key would
either block the delete or orphan that history.

The upsert touches only registry-owned columns. Quarantine state and
failure counters belong to breakers, and a SIGHUP reload must not
silently clear a breaker that is holding a project back.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: `store` — the incident write path

**Files:**
- Create: `internal/store/incidents.go`
- Test: `internal/store/incidents_test.go`

**Interfaces:**
- Consumes: `store.DB`, `store.SyncProjects`, `store.ProjectRow` (Task 3).
- Produces:
  - `store.IngestParams` — `struct{ ProjectSlug, Source, SourceRef, Kind, Title, Body, State, StateReason string; Metadata map[string]string; OccurredAt time.Time }`. An empty `ProjectSlug` means unroutable and is stored as SQL `NULL`.
  - `store.IngestResult` — `struct{ ID int64; IsNew bool; OccurrenceCount int; State string }`.
  - `store.IngestIncident(ctx context.Context, db *DB, p IngestParams, now time.Time) (IngestResult, error)`

**Why `IsNew` is derived from `occurrence_count`.** SQLite's `changes()` cannot distinguish an insert from an `ON CONFLICT` update, and a `SELECT` first would race a concurrent duplicate delivery. Because a fresh row always starts at `occurrence_count = 1` and the conflict branch always increments, `RETURNING occurrence_count` settles it in one statement with no race.

- [ ] **Step 1: Write the failing test**

Create `internal/store/incidents_test.go`:

```go
package store

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"
)

func ingestFixture(t *testing.T) (*DB, context.Context, time.Time) {
	t.Helper()
	db, now := syncedDB(t)
	ctx := context.Background()
	err := SyncProjects(ctx, db, []ProjectRow{
		{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"},
	}, now)
	if err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}
	return db, ctx, now
}

func sampleParams() IngestParams {
	return IngestParams{
		ProjectSlug: "api",
		Source:      "gcplog",
		SourceRef:   "gcplog:insert-1",
		Kind:        "log.error",
		Title:       "TypeError: undefined is not a function",
		Body:        "at handler (src/index.js:12)",
		Metadata:    map[string]string{"severity": "ERROR"},
		State:       "received",
		OccurredAt:  time.Date(2026, 8, 2, 11, 0, 0, 0, time.UTC),
	}
}

func TestIngestIncidentInsertsThenDeduplicates(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	first, err := IngestIncident(ctx, db, sampleParams(), now)
	if err != nil {
		t.Fatalf("IngestIncident() error = %v, want nil", err)
	}
	if !first.IsNew {
		t.Error("IsNew = false on the first delivery, want true")
	}
	if first.OccurrenceCount != 1 {
		t.Errorf("OccurrenceCount = %d, want 1", first.OccurrenceCount)
	}
	if first.ID == 0 {
		t.Error("ID = 0, want an assigned rowid")
	}

	second, err := IngestIncident(ctx, db, sampleParams(), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("IngestIncident() error = %v, want nil", err)
	}
	if second.IsNew {
		t.Error("IsNew = true on a redelivery; idempotency must come from the unique index")
	}
	if second.ID != first.ID {
		t.Errorf("ID = %d, want the original %d", second.ID, first.ID)
	}
	if second.OccurrenceCount != 2 {
		t.Errorf("OccurrenceCount = %d, want 2", second.OccurrenceCount)
	}

	var rows int
	if err := db.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM incidents`).Scan(&rows); err != nil {
		t.Fatalf("counting incidents: %v", err)
	}
	if rows != 1 {
		t.Errorf("incident rows = %d, want 1", rows)
	}
}

func TestIngestIncidentStoresUnroutableAsNull(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	p := sampleParams()
	p.ProjectSlug = ""
	p.State = "filtered"
	p.StateReason = "unroutable"

	res, err := IngestIncident(ctx, db, p, now)
	if err != nil {
		t.Fatalf("IngestIncident() error = %v, want nil; an unroutable event must persist, not be dropped", err)
	}

	var slug sql.NullString
	var state, reason string
	err = db.Reader().QueryRowContext(ctx,
		`SELECT project_slug, state, COALESCE(state_reason, '') FROM incidents WHERE id = ?`, res.ID,
	).Scan(&slug, &state, &reason)
	if err != nil {
		t.Fatalf("reading incident: %v", err)
	}
	if slug.Valid {
		t.Errorf("project_slug = %q, want NULL", slug.String)
	}
	if state != "filtered" || reason != "unroutable" {
		t.Errorf("state/reason = %q/%q, want filtered/unroutable", state, reason)
	}
}

func TestIngestIncidentSeparatesSourceNamespaces(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	a := sampleParams()
	a.Source, a.SourceRef = "gcplog", "42"
	b := sampleParams()
	b.Source, b.SourceRef = "github", "42"

	if _, err := IngestIncident(ctx, db, a, now); err != nil {
		t.Fatalf("IngestIncident(a) error = %v", err)
	}
	res, err := IngestIncident(ctx, db, b, now)
	if err != nil {
		t.Fatalf("IngestIncident(b) error = %v", err)
	}
	if !res.IsNew {
		t.Error("the same ref under a different source collapsed; the unique index is on (source, source_ref)")
	}
}

func TestIngestIncidentSerialisesMetadata(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	t.Run("nil metadata becomes an empty object", func(t *testing.T) {
		p := sampleParams()
		p.SourceRef, p.Metadata = "gcplog:nil-meta", nil
		res, err := IngestIncident(ctx, db, p, now)
		if err != nil {
			t.Fatalf("IngestIncident() error = %v", err)
		}
		var raw string
		if err := db.Reader().QueryRowContext(ctx,
			`SELECT metadata_json FROM incidents WHERE id = ?`, res.ID).Scan(&raw); err != nil {
			t.Fatalf("reading metadata: %v", err)
		}
		if raw != "{}" {
			t.Errorf("metadata_json = %q, want %q; the column is NOT NULL", raw, "{}")
		}
	})
}

func TestIngestIncidentConcurrentDuplicatesCollapse(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := IngestIncident(ctx, db, sampleParams(), now); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent IngestIncident() error = %v, want nil", err)
	}

	var rows, count int
	err := db.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*), MAX(occurrence_count) FROM incidents`).Scan(&rows, &count)
	if err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 1 {
		t.Errorf("incident rows = %d, want 1", rows)
	}
	if count != n {
		t.Errorf("occurrence_count = %d, want %d; no delivery may be lost", count, n)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/store/ -run TestIngestIncident -v`

Expected: FAIL — `undefined: IngestParams`, `undefined: IngestIncident`.

- [ ] **Step 3: Write the implementation**

Create `internal/store/incidents.go`:

```go
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrIngest is returned when an event cannot be recorded as an incident.
var ErrIngest = errors.New("ingesting incident")

// IngestParams is one normalised event on its way to becoming an incident.
type IngestParams struct {
	// ProjectSlug is empty for an unroutable event, which is stored as NULL
	// rather than dropped — an unroutable event usually means a stale
	// projects.yaml and must stay visible (SPEC §4.2).
	ProjectSlug string

	Source      string
	SourceRef   string
	Kind        string
	Title       string
	Body        string
	State       string
	StateReason string

	Metadata   map[string]string
	OccurredAt time.Time
}

// IngestResult reports what the write did.
type IngestResult struct {
	ID              int64
	IsNew           bool
	OccurrenceCount int
	State           string
}

// IngestIncident records an event, collapsing a redelivery into the existing
// row. Pub/Sub is at-least-once, so idempotency comes from the unique index on
// incidents(source, source_ref) rather than from the transport (SPEC §4.2).
//
// IsNew is derived from the returned occurrence_count rather than from
// changes(): SQLite cannot distinguish an insert from an ON CONFLICT update,
// and a SELECT beforehand would race a concurrent duplicate delivery. A fresh
// row always starts at 1 and the conflict branch always increments, so the
// returned count settles it in a single statement.
//
// The conflict branch deliberately leaves state, title and body alone. The
// first delivery's classification is authoritative; a redelivery is evidence
// of recurrence, not a correction.
func IngestIncident(ctx context.Context, db *DB, p IngestParams, now time.Time) (IngestResult, error) {
	metadata, err := marshalMetadata(p.Metadata)
	if err != nil {
		return IngestResult{}, err
	}

	slug := sql.NullString{String: p.ProjectSlug, Valid: p.ProjectSlug != ""}
	reason := sql.NullString{String: p.StateReason, Valid: p.StateReason != ""}
	ts := now.UTC().Format(time.RFC3339)
	occurred := p.OccurredAt.UTC().Format(time.RFC3339)

	var res IngestResult
	err = db.Writer().QueryRowContext(ctx, `
		INSERT INTO incidents (
			project_slug, source, source_ref, kind, title, body, metadata_json,
			state, state_reason, occurrence_count, occurred_at, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
		ON CONFLICT(source, source_ref) DO UPDATE SET
			occurrence_count = incidents.occurrence_count + 1,
			updated_at       = excluded.updated_at
		RETURNING id, occurrence_count, state`,
		slug, p.Source, p.SourceRef, p.Kind, p.Title, p.Body, metadata,
		p.State, reason, occurred, ts, ts,
	).Scan(&res.ID, &res.OccurrenceCount, &res.State)
	if err != nil {
		return IngestResult{}, fmt.Errorf("%w: %s/%s: %w", ErrIngest, p.Source, p.SourceRef, err)
	}

	res.IsNew = res.OccurrenceCount == 1
	return res, nil
}

// marshalMetadata renders metadata for the NOT NULL metadata_json column. A nil
// map becomes "{}" rather than "null", which would violate the column's
// contract and break every reader.
func marshalMetadata(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("%w: encoding metadata: %w", ErrIngest, err)
	}
	return string(encoded), nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./internal/store/ -run TestIngestIncident -v`

Expected: PASS. `TestIngestIncidentConcurrentDuplicatesCollapse` is the important one — it proves the single-writer pool plus the unique index makes at-least-once delivery safe with no application-level locking. If it reports `occurrence_count` below 20, a delivery was lost.

- [ ] **Step 5: Run the gate and commit**

Run: `make check`

```bash
git add internal/store/incidents.go internal/store/incidents_test.go
git commit -m "feat(store): idempotent incident write path

INSERT ... ON CONFLICT(source, source_ref) DO UPDATE increments
occurrence_count, so Pub/Sub's at-least-once delivery is absorbed by the
unique index rather than by application-level checking.

IsNew comes from the returned occurrence_count, not changes(): SQLite
cannot distinguish an insert from a conflict update, and a SELECT first
would race a concurrent duplicate. A fresh row starts at 1 and the
conflict branch increments, so one statement settles it.

The conflict branch leaves state, title and body alone — the first
delivery's classification is authoritative, and a redelivery is evidence
of recurrence rather than a correction.

Unroutable events persist with project_slug NULL rather than being
dropped; they usually mean a stale projects.yaml and must stay visible.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: `store` — incident events, transitions, and the replay query

**Files:**
- Create: `internal/store/events.go`
- Test: `internal/store/events_test.go`

**Interfaces:**
- Consumes: `store.DB`, `store.IngestIncident`, `store.IngestParams` (Task 4).
- Produces:
  - `store.IncidentEvent` — `struct{ ID, IncidentID int64; TS time.Time; Kind, Actor, FromState, ToState string; Payload json.RawMessage }`.
  - `store.ErrStaleTransition` sentinel.
  - `store.AppendEvent(ctx context.Context, db *DB, ev IncidentEvent, now time.Time) (int64, error)`
  - `store.Transition(ctx context.Context, db *DB, t TransitionParams, now time.Time) (int64, error)` where `TransitionParams` is `struct{ IncidentID int64; From, To, Reason, Actor string; Payload json.RawMessage }`. Returns the new `incident_events.id`.
  - `store.EventsAfter(ctx context.Context, db *DB, afterID int64, limit int) ([]IncidentEvent, error)`
  - `store.EventsForIncident(ctx context.Context, db *DB, incidentID int64) ([]IncidentEvent, error)`

**Two constraints this task must respect.**

1. **`store` must not import `bus`.** SPEC §4 states `store` imports nothing but `config`. `EventsAfter` therefore returns `[]IncidentEvent`, and Task 16 adds the `httpapi` adapter that maps them to `bus.Event`. Returning `[]bus.Event` from here would invert the dependency graph.
2. **`incident_events.id` is the SSE ID space.** M0 documented this on `bus.Event.ID` (`bus.go:16-20`): replay and the audit trail share one sequence, so a hub-local counter cannot silently diverge from it. `Transition` returns the event ID precisely so the caller can publish with it.

**Why the compare-and-swap.** `Transition` updates with `WHERE id = ? AND state = ?`. If the row already moved, zero rows change and it returns `ErrStaleTransition` instead of writing a transition that never happened. Without this, two workers racing on the same incident would both append plausible-looking audit rows describing contradictory histories.

- [ ] **Step 1: Write the failing test**

Create `internal/store/events_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func seededIncident(t *testing.T) (*DB, context.Context, time.Time, int64) {
	t.Helper()
	db, ctx, now := ingestFixture(t)
	res, err := IngestIncident(ctx, db, sampleParams(), now)
	if err != nil {
		t.Fatalf("IngestIncident() error = %v, want nil", err)
	}
	return db, ctx, now, res.ID
}

func TestTransitionMovesStateAndReturnsEventID(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	eventID, err := Transition(ctx, db, TransitionParams{
		IncidentID: id,
		From:       "received",
		To:         "triaging",
		Actor:      "tier0",
	}, now)
	if err != nil {
		t.Fatalf("Transition() error = %v, want nil", err)
	}
	if eventID == 0 {
		t.Fatal("Transition() returned event ID 0; it is the SSE Last-Event-ID sequence")
	}

	var state string
	if err := db.Reader().QueryRowContext(ctx,
		`SELECT state FROM incidents WHERE id = ?`, id).Scan(&state); err != nil {
		t.Fatalf("reading state: %v", err)
	}
	if state != "triaging" {
		t.Errorf("state = %q, want %q", state, "triaging")
	}

	events, err := EventsForIncident(ctx, db, id)
	if err != nil {
		t.Fatalf("EventsForIncident() error = %v, want nil", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Kind != "state_change" {
		t.Errorf("Kind = %q, want %q", ev.Kind, "state_change")
	}
	if ev.FromState != "received" || ev.ToState != "triaging" {
		t.Errorf("from/to = %q/%q, want received/triaging", ev.FromState, ev.ToState)
	}
	if ev.Actor != "tier0" {
		t.Errorf("Actor = %q, want %q", ev.Actor, "tier0")
	}
	if string(ev.Payload) != "{}" {
		t.Errorf("Payload = %q, want %q; the column is NOT NULL", string(ev.Payload), "{}")
	}
}

func TestTransitionRejectsStaleFromState(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	if _, err := Transition(ctx, db, TransitionParams{
		IncidentID: id, From: "received", To: "triaging", Actor: "tier0",
	}, now); err != nil {
		t.Fatalf("first Transition() error = %v, want nil", err)
	}

	_, err := Transition(ctx, db, TransitionParams{
		IncidentID: id, From: "received", To: "filtered", Actor: "tier0",
	}, now)
	if !errors.Is(err, ErrStaleTransition) {
		t.Fatalf("second Transition() error = %v, want ErrStaleTransition", err)
	}

	events, err := EventsForIncident(ctx, db, id)
	if err != nil {
		t.Fatalf("EventsForIncident() error = %v, want nil", err)
	}
	if len(events) != 1 {
		t.Errorf("len(events) = %d, want 1; a rejected transition must not append an audit row", len(events))
	}
}

func TestTransitionSetsClosedAtForTerminalStates(t *testing.T) {
	tests := []struct {
		name       string
		to         string
		wantClosed bool
	}{
		{name: "filtered is terminal", to: "filtered", wantClosed: true},
		{name: "suppressed is terminal", to: "suppressed", wantClosed: true},
		{name: "dismissed is terminal", to: "dismissed", wantClosed: true},
		{name: "escalated is terminal", to: "escalated", wantClosed: true},
		{name: "triaging is not terminal", to: "triaging", wantClosed: false},
		{name: "parked is not terminal", to: "parked", wantClosed: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, ctx, now, id := seededIncident(t)

			if _, err := Transition(ctx, db, TransitionParams{
				IncidentID: id, From: "received", To: tc.to, Actor: "tier0",
			}, now); err != nil {
				t.Fatalf("Transition() error = %v, want nil", err)
			}

			var closedAt *string
			if err := db.Reader().QueryRowContext(ctx,
				`SELECT closed_at FROM incidents WHERE id = ?`, id).Scan(&closedAt); err != nil {
				t.Fatalf("reading closed_at: %v", err)
			}
			if got := closedAt != nil; got != tc.wantClosed {
				t.Errorf("closed_at set = %v, want %v", got, tc.wantClosed)
			}
		})
	}
}

func TestEventsAfterDrivesReplay(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	first, err := Transition(ctx, db, TransitionParams{
		IncidentID: id, From: "received", To: "triaging", Actor: "tier0",
	}, now)
	if err != nil {
		t.Fatalf("Transition() error = %v", err)
	}
	second, err := AppendEvent(ctx, db, IncidentEvent{
		IncidentID: id,
		Kind:       "note",
		Actor:      "system",
		Payload:    json.RawMessage(`{"occurrences":3}`),
	}, now)
	if err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}

	if second <= first {
		t.Fatalf("event IDs not monotonic: %d then %d", first, second)
	}

	got, err := EventsAfter(ctx, db, first, 100)
	if err != nil {
		t.Fatalf("EventsAfter() error = %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1; replay must be strictly after the given ID", len(got))
	}
	if got[0].ID != second {
		t.Errorf("ID = %d, want %d", got[0].ID, second)
	}
	if string(got[0].Payload) != `{"occurrences":3}` {
		t.Errorf("Payload = %q, want the stored JSON", string(got[0].Payload))
	}

	t.Run("limit is honoured", func(t *testing.T) {
		all, err := EventsAfter(ctx, db, 0, 1)
		if err != nil {
			t.Fatalf("EventsAfter() error = %v", err)
		}
		if len(all) != 1 {
			t.Errorf("len = %d, want 1", len(all))
		}
	})

	t.Run("timestamps round-trip as UTC", func(t *testing.T) {
		all, err := EventsAfter(ctx, db, 0, 100)
		if err != nil {
			t.Fatalf("EventsAfter() error = %v", err)
		}
		for _, ev := range all {
			if ev.TS.IsZero() {
				t.Error("TS is zero; the stored timestamp did not parse")
			}
			if ev.TS.Location() != time.UTC {
				t.Errorf("TS location = %v, want UTC", ev.TS.Location())
			}
		}
	})
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/store/ -run 'TestTransition|TestEventsAfter' -v`

Expected: FAIL — `undefined: Transition`, `undefined: TransitionParams`, `undefined: AppendEvent`, `undefined: EventsAfter`.

- [ ] **Step 3: Write the implementation**

Create `internal/store/events.go`:

```go
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrStaleTransition is returned when an incident's state no longer matches the
// expected from-state, meaning another worker moved it first.
var ErrStaleTransition = errors.New("incident state changed concurrently")

// ErrEvents is returned when the incident_events table cannot be read or written.
var ErrEvents = errors.New("incident events")

// emptyPayload is the NOT NULL default for payload_json.
const emptyPayload = "{}"

// terminalStates never transition again, so reaching one stamps closed_at.
// parked is deliberately absent: SPEC §3 calls it non-terminal and resumable.
var terminalStates = map[string]bool{
	"filtered": true, "suppressed": true, "dismissed": true, "merged": true,
	"deployed": true, "rejected": true, "aborted": true, "failed": true,
	"escalated": true,
}

// IsTerminalState reports whether a state is final (SPEC §3).
func IsTerminalState(state string) bool { return terminalStates[state] }

// IncidentEvent is one row of the append-only audit trail. Its id is also the
// SSE Last-Event-ID sequence, so replay and the audit trail cannot diverge
// (SPEC §3, §4.11).
type IncidentEvent struct {
	ID         int64
	IncidentID int64
	TS         time.Time
	Kind       string // state_change | note | cost | policy | abort
	Actor      string // system | tier0 | tier1 | tier2 | operator:<name>
	FromState  string
	ToState    string
	Payload    json.RawMessage
}

// TransitionParams describes one state change.
type TransitionParams struct {
	IncidentID int64
	From       string
	To         string
	Reason     string
	Actor      string
	Payload    json.RawMessage
}

// AppendEvent adds a row to the audit trail and returns its id.
func AppendEvent(ctx context.Context, db *DB, ev IncidentEvent, now time.Time) (int64, error) {
	id, err := appendEvent(ctx, db.Writer(), ev, now)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// execer is satisfied by both *sql.DB and *sql.Tx, so the append logic is
// shared between the standalone and in-transaction paths.
type execer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func appendEvent(ctx context.Context, q execer, ev IncidentEvent, now time.Time) (int64, error) {
	payload := string(ev.Payload)
	if len(ev.Payload) == 0 {
		payload = emptyPayload
	}
	ts := now.UTC().Format(time.RFC3339)

	var id int64
	err := q.QueryRowContext(ctx, `
		INSERT INTO incident_events (incident_id, ts, kind, actor, from_state, to_state, payload_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		ev.IncidentID, ts, ev.Kind, ev.Actor,
		nullIfEmpty(ev.FromState), nullIfEmpty(ev.ToState), payload,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("%w: appending %s for incident %d: %w", ErrEvents, ev.Kind, ev.IncidentID, err)
	}
	return id, nil
}

// Transition moves an incident to a new state and appends the matching audit
// row in one transaction, returning the new event id for SSE publication.
//
// The UPDATE carries `AND state = ?`, so a row another worker already moved
// changes zero rows and yields ErrStaleTransition rather than an audit entry
// describing a transition that never happened. Two workers racing the same
// incident would otherwise both append plausible but contradictory histories.
func Transition(ctx context.Context, db *DB, t TransitionParams, now time.Time) (int64, error) {
	ts := now.UTC().Format(time.RFC3339)

	tx, err := db.Writer().BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("%w: beginning transaction: %w", ErrEvents, err)
	}
	defer func() { _ = tx.Rollback() }()

	closedAt := sql.NullString{String: ts, Valid: IsTerminalState(t.To)}

	res, err := tx.ExecContext(ctx, `
		UPDATE incidents
		   SET state = ?, state_reason = ?, updated_at = ?,
		       closed_at = CASE WHEN ? THEN ? ELSE closed_at END
		 WHERE id = ? AND state = ?`,
		t.To, nullIfEmpty(t.Reason), ts,
		closedAt.Valid, ts,
		t.IncidentID, t.From)
	if err != nil {
		return 0, fmt.Errorf("%w: updating incident %d: %w", ErrEvents, t.IncidentID, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%w: counting affected rows: %w", ErrEvents, err)
	}
	if affected == 0 {
		return 0, fmt.Errorf("%w: incident %d is no longer in state %q",
			ErrStaleTransition, t.IncidentID, t.From)
	}

	id, err := appendEvent(ctx, tx, IncidentEvent{
		IncidentID: t.IncidentID,
		Kind:       "state_change",
		Actor:      t.Actor,
		FromState:  t.From,
		ToState:    t.To,
		Payload:    t.Payload,
	}, now)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("%w: committing transition: %w", ErrEvents, err)
	}
	return id, nil
}

const eventColumns = `id, incident_id, ts, kind, actor,
	COALESCE(from_state, ''), COALESCE(to_state, ''), payload_json`

func scanEvents(rows *sql.Rows) ([]IncidentEvent, error) {
	var out []IncidentEvent
	for rows.Next() {
		var (
			ev      IncidentEvent
			ts      string
			payload string
		)
		if err := rows.Scan(&ev.ID, &ev.IncidentID, &ts, &ev.Kind, &ev.Actor,
			&ev.FromState, &ev.ToState, &payload); err != nil {
			return nil, fmt.Errorf("%w: scanning: %w", ErrEvents, err)
		}
		parsed, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return nil, fmt.Errorf("%w: parsing ts %q: %w", ErrEvents, ts, err)
		}
		ev.TS = parsed.UTC()
		ev.Payload = json.RawMessage(payload)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterating: %w", ErrEvents, err)
	}
	return out, nil
}

// EventsAfter returns audit rows with an id strictly greater than afterID, in
// id order. It backs SSE replay from Last-Event-ID (SPEC §4.11).
func EventsAfter(ctx context.Context, db *DB, afterID int64, limit int) ([]IncidentEvent, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT `+eventColumns+` FROM incident_events WHERE id > ? ORDER BY id LIMIT ?`,
		afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: replaying after %d: %w", ErrEvents, afterID, err)
	}
	defer func() { _ = rows.Close() }()
	return scanEvents(rows)
}

// EventsForIncident returns one incident's full timeline in id order.
func EventsForIncident(ctx context.Context, db *DB, incidentID int64) ([]IncidentEvent, error) {
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT `+eventColumns+` FROM incident_events WHERE incident_id = ? ORDER BY id`,
		incidentID)
	if err != nil {
		return nil, fmt.Errorf("%w: reading timeline for %d: %w", ErrEvents, incidentID, err)
	}
	defer func() { _ = rows.Close() }()
	return scanEvents(rows)
}

// nullIfEmpty maps "" to SQL NULL, keeping optional TEXT columns genuinely
// absent rather than storing an empty string that readers must special-case.
func nullIfEmpty(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./internal/store/ -run 'TestTransition|TestEventsAfter' -v`

Expected: PASS. `TestTransitionRejectsStaleFromState` is the one that matters most — if it fails, concurrent workers can write contradictory audit histories.

- [ ] **Step 5: Run the gate and commit**

Run: `make check`

```bash
git add internal/store/events.go internal/store/events_test.go
git commit -m "feat(store): incident transitions, audit trail, and replay query

Transition updates state and appends the audit row in one transaction and
returns the new incident_events.id, which is the SSE Last-Event-ID
sequence — M0 reserved bus.Event.ID for exactly this so replay and the
audit trail cannot silently diverge.

The UPDATE carries AND state = ?, so an incident another worker already
moved changes zero rows and yields ErrStaleTransition. Without the
compare-and-swap, two workers racing one incident would both append
plausible but contradictory histories.

EventsAfter returns []IncidentEvent rather than []bus.Event: store
imports nothing but config (SPEC 4), so httpapi owns the mapping.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: `store` — fingerprint suppression windows

**Files:**
- Create: `internal/store/fingerprints.go`
- Test: `internal/store/fingerprints_test.go`

**Interfaces:**
- Consumes: `store.DB`, `store.IngestIncident` (Task 4), migration `0002` (Task 2).
- Produces:
  - `store.FingerprintRecord` — `struct{ Fingerprint, ProjectSlug, Strategy string; FirstIncidentID int64; LastSeenAt, SuppressUntil time.Time; TotalOccurrences, RepairAttempts int; Frames []string }`.
  - `store.ObserveFingerprint(ctx context.Context, db *DB, o FingerprintObservation, now time.Time) (FingerprintOutcome, error)` where `FingerprintObservation` is `struct{ Fingerprint, ProjectSlug, Strategy string; Frames []string; IncidentID int64; Window time.Duration }` and `FingerprintOutcome` is `struct{ Suppressed bool; FirstIncidentID int64; TotalOccurrences int; SuppressUntil time.Time }`.
  - `store.GetFingerprint(ctx context.Context, db *DB, fingerprint string) (FingerprintRecord, bool, error)`

**The behaviour that prevents an invoice.** The first event for a fingerprint opens the window and reports `Suppressed: false`. Every event inside the window reports `Suppressed: true` and increments `total_occurrences`. This is what stops a crash loop emitting thousands of unique `insertId`s from becoming thousands of Tier 1 calls in M2 — `(source, source_ref)` dedup cannot help there, because every entry genuinely is unique.

- [ ] **Step 1: Write the failing test**

Create `internal/store/fingerprints_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func observation(incidentID int64) FingerprintObservation {
	return FingerprintObservation{
		Fingerprint: "fp-abc",
		ProjectSlug: "api",
		Strategy:    "source_roots",
		Frames:      []string{"src/index.js", "src/handler.js"},
		IncidentID:  incidentID,
		Window:      6 * time.Hour,
	}
}

func TestObserveFingerprintOpensThenSuppresses(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	first, err := ObserveFingerprint(ctx, db, observation(id), now)
	if err != nil {
		t.Fatalf("ObserveFingerprint() error = %v, want nil", err)
	}
	if first.Suppressed {
		t.Error("Suppressed = true on the first sighting, want false")
	}
	if first.TotalOccurrences != 1 {
		t.Errorf("TotalOccurrences = %d, want 1", first.TotalOccurrences)
	}
	if want := now.Add(6 * time.Hour); !first.SuppressUntil.Equal(want) {
		t.Errorf("SuppressUntil = %v, want %v", first.SuppressUntil, want)
	}

	second, err := ObserveFingerprint(ctx, db, observation(id), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ObserveFingerprint() error = %v, want nil", err)
	}
	if !second.Suppressed {
		t.Error("Suppressed = false inside the window, want true")
	}
	if second.TotalOccurrences != 2 {
		t.Errorf("TotalOccurrences = %d, want 2", second.TotalOccurrences)
	}
	if second.FirstIncidentID != id {
		t.Errorf("FirstIncidentID = %d, want %d; suppressed events attach to the original", second.FirstIncidentID, id)
	}
}

func TestObserveFingerprintReopensAfterWindow(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	if _, err := ObserveFingerprint(ctx, db, observation(id), now); err != nil {
		t.Fatalf("ObserveFingerprint() error = %v", err)
	}

	after := now.Add(6*time.Hour + time.Second)
	out, err := ObserveFingerprint(ctx, db, observation(id), after)
	if err != nil {
		t.Fatalf("ObserveFingerprint() error = %v", err)
	}
	if out.Suppressed {
		t.Error("Suppressed = true past the window, want false; the window must reopen")
	}
	if want := after.Add(6 * time.Hour); !out.SuppressUntil.Equal(want) {
		t.Errorf("SuppressUntil = %v, want %v", out.SuppressUntil, want)
	}
	if out.TotalOccurrences != 2 {
		t.Errorf("TotalOccurrences = %d, want 2; the lifetime count must not reset", out.TotalOccurrences)
	}
}

func TestObserveFingerprintRecordsEvidence(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	if _, err := ObserveFingerprint(ctx, db, observation(id), now); err != nil {
		t.Fatalf("ObserveFingerprint() error = %v", err)
	}

	rec, ok, err := GetFingerprint(ctx, db, "fp-abc")
	if err != nil || !ok {
		t.Fatalf("GetFingerprint() = %v, %v, want found", ok, err)
	}
	if rec.Strategy != "source_roots" {
		t.Errorf("Strategy = %q, want %q", rec.Strategy, "source_roots")
	}
	if len(rec.Frames) != 2 || rec.Frames[0] != "src/index.js" {
		t.Errorf("Frames = %v, want the recorded frames", rec.Frames)
	}

	t.Run("frames are stored as a JSON array", func(t *testing.T) {
		var raw string
		if err := db.Reader().QueryRowContext(ctx,
			`SELECT frames_json FROM fingerprints WHERE fingerprint = 'fp-abc'`).Scan(&raw); err != nil {
			t.Fatalf("reading frames_json: %v", err)
		}
		var decoded []string
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			t.Errorf("frames_json %q is not a JSON array: %v", raw, err)
		}
	})
}

func TestObserveFingerprintStormCollapses(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	const storm = 500
	opened := 0
	for i := range storm {
		out, err := ObserveFingerprint(ctx, db, observation(id), now.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("ObserveFingerprint(%d) error = %v", i, err)
		}
		if !out.Suppressed {
			opened++
		}
	}

	if opened != 1 {
		t.Errorf("opened %d windows, want exactly 1; a storm must collapse to one incident", opened)
	}

	rec, _, err := GetFingerprint(ctx, db, "fp-abc")
	if err != nil {
		t.Fatalf("GetFingerprint() error = %v", err)
	}
	if rec.TotalOccurrences != storm {
		t.Errorf("TotalOccurrences = %d, want %d; the storm must remain visible while silent", rec.TotalOccurrences, storm)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/store/ -run TestObserveFingerprint -v`

Expected: FAIL — `undefined: FingerprintObservation`, `undefined: ObserveFingerprint`, `undefined: GetFingerprint`.

- [ ] **Step 3: Write the implementation**

Create `internal/store/fingerprints.go`:

```go
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrFingerprint is returned when a fingerprint cannot be read or recorded.
var ErrFingerprint = errors.New("fingerprint")

// FingerprintObservation is one sighting of a fingerprint.
type FingerprintObservation struct {
	Fingerprint string
	ProjectSlug string
	Strategy    string   // source_roots | denylist | all_frames | workflow | no_frames
	Frames      []string // the normalised frames that produced the hash
	IncidentID  int64
	Window      time.Duration
}

// FingerprintOutcome reports whether this sighting is suppressed.
type FingerprintOutcome struct {
	Suppressed       bool
	FirstIncidentID  int64
	TotalOccurrences int
	SuppressUntil    time.Time
}

// FingerprintRecord is a stored fingerprint with its evidence.
type FingerprintRecord struct {
	Fingerprint      string
	ProjectSlug      string
	Strategy         string
	Frames           []string
	FirstIncidentID  int64
	LastSeenAt       time.Time
	SuppressUntil    time.Time
	TotalOccurrences int
	RepairAttempts   int
}

// ObserveFingerprint records a sighting and reports whether it falls inside an
// open suppression window.
//
// The first sighting opens a window and reports Suppressed false. Sightings
// inside the window report true, increment total_occurrences, and cost nothing
// — this is what stops a crash loop emitting thousands of unique insertIds from
// becoming thousands of Tier 1 calls in M2, where (source, source_ref)
// deduplication cannot help because every entry genuinely is unique
// (SPEC §4.2.2, §4.3.2).
//
// Past the window the fingerprint reopens: a bug that recurs the next day is a
// new incident, not a footnote on a stale one. total_occurrences is a lifetime
// counter and deliberately does not reset.
//
// The read and the write share one transaction, so two concurrent sightings
// cannot both decide they are first.
func ObserveFingerprint(ctx context.Context, db *DB, o FingerprintObservation, now time.Time) (FingerprintOutcome, error) {
	frames, err := json.Marshal(nonNilFrames(o.Frames))
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: encoding frames: %w", ErrFingerprint, err)
	}

	tx, err := db.Writer().BeginTx(ctx, nil)
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: beginning transaction: %w", ErrFingerprint, err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		firstIncidentID  int64
		suppressUntilRaw string
		totalOccurrences int
	)
	err = tx.QueryRowContext(ctx,
		`SELECT first_incident_id, suppress_until, total_occurrences
		   FROM fingerprints WHERE fingerprint = ?`, o.Fingerprint,
	).Scan(&firstIncidentID, &suppressUntilRaw, &totalOccurrences)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		out, err := insertFingerprint(ctx, tx, o, string(frames), now)
		if err != nil {
			return FingerprintOutcome{}, err
		}
		if err := tx.Commit(); err != nil {
			return FingerprintOutcome{}, fmt.Errorf("%w: committing: %w", ErrFingerprint, err)
		}
		return out, nil

	case err != nil:
		return FingerprintOutcome{}, fmt.Errorf("%w: reading %q: %w", ErrFingerprint, o.Fingerprint, err)
	}

	suppressUntil, err := time.Parse(time.RFC3339, suppressUntilRaw)
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: parsing suppress_until %q: %w",
			ErrFingerprint, suppressUntilRaw, err)
	}

	inWindow := now.Before(suppressUntil.UTC())
	newUntil := suppressUntil.UTC()
	if !inWindow {
		newUntil = now.UTC().Add(o.Window)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE fingerprints
		   SET total_occurrences = total_occurrences + 1,
		       last_seen_at      = ?,
		       suppress_until    = ?,
		       strategy          = ?,
		       frames_json       = ?
		 WHERE fingerprint = ?`,
		now.UTC().Format(time.RFC3339), newUntil.Format(time.RFC3339),
		o.Strategy, string(frames), o.Fingerprint)
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: updating %q: %w", ErrFingerprint, o.Fingerprint, err)
	}

	if err := tx.Commit(); err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: committing: %w", ErrFingerprint, err)
	}

	return FingerprintOutcome{
		Suppressed:       inWindow,
		FirstIncidentID:  firstIncidentID,
		TotalOccurrences: totalOccurrences + 1,
		SuppressUntil:    newUntil,
	}, nil
}

func insertFingerprint(ctx context.Context, tx *sql.Tx, o FingerprintObservation, frames string, now time.Time) (FingerprintOutcome, error) {
	until := now.UTC().Add(o.Window)

	_, err := tx.ExecContext(ctx, `
		INSERT INTO fingerprints (
			fingerprint, project_slug, first_incident_id, last_seen_at,
			suppress_until, total_occurrences, strategy, frames_json
		) VALUES (?, ?, ?, ?, ?, 1, ?, ?)`,
		o.Fingerprint, o.ProjectSlug, o.IncidentID,
		now.UTC().Format(time.RFC3339), until.Format(time.RFC3339),
		o.Strategy, frames)
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: inserting %q: %w", ErrFingerprint, o.Fingerprint, err)
	}

	return FingerprintOutcome{
		Suppressed:       false,
		FirstIncidentID:  o.IncidentID,
		TotalOccurrences: 1,
		SuppressUntil:    until,
	}, nil
}

// GetFingerprint returns one stored fingerprint, reporting false when absent.
func GetFingerprint(ctx context.Context, db *DB, fingerprint string) (FingerprintRecord, bool, error) {
	var (
		rec        FingerprintRecord
		lastSeen   string
		until      string
		framesJSON string
	)
	err := db.Reader().QueryRowContext(ctx, `
		SELECT fingerprint, project_slug, first_incident_id, last_seen_at,
		       suppress_until, total_occurrences, repair_attempts, strategy, frames_json
		  FROM fingerprints WHERE fingerprint = ?`, fingerprint,
	).Scan(&rec.Fingerprint, &rec.ProjectSlug, &rec.FirstIncidentID, &lastSeen,
		&until, &rec.TotalOccurrences, &rec.RepairAttempts, &rec.Strategy, &framesJSON)

	if errors.Is(err, sql.ErrNoRows) {
		return FingerprintRecord{}, false, nil
	}
	if err != nil {
		return FingerprintRecord{}, false, fmt.Errorf("%w: getting %q: %w", ErrFingerprint, fingerprint, err)
	}

	if rec.LastSeenAt, err = time.Parse(time.RFC3339, lastSeen); err != nil {
		return FingerprintRecord{}, false, fmt.Errorf("%w: parsing last_seen_at: %w", ErrFingerprint, err)
	}
	if rec.SuppressUntil, err = time.Parse(time.RFC3339, until); err != nil {
		return FingerprintRecord{}, false, fmt.Errorf("%w: parsing suppress_until: %w", ErrFingerprint, err)
	}
	if err := json.Unmarshal([]byte(framesJSON), &rec.Frames); err != nil {
		return FingerprintRecord{}, false, fmt.Errorf("%w: decoding frames_json: %w", ErrFingerprint, err)
	}

	rec.LastSeenAt = rec.LastSeenAt.UTC()
	rec.SuppressUntil = rec.SuppressUntil.UTC()
	return rec, true, nil
}

// nonNilFrames guarantees json.Marshal emits [] rather than null, which the
// frames_json column's default and every reader assume.
func nonNilFrames(frames []string) []string {
	if frames == nil {
		return []string{}
	}
	return frames
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./internal/store/ -run TestObserveFingerprint -v`

Expected: PASS. `TestObserveFingerprintStormCollapses` must report exactly one opened window across 500 sightings.

- [ ] **Step 5: Run the gate and commit**

Run: `make check`

```bash
git add internal/store/fingerprints.go internal/store/fingerprints_test.go
git commit -m "feat(store): fingerprint suppression windows with recorded evidence

The first sighting opens a window; sightings inside it are suppressed and
increment a lifetime counter. This is what stops a crash loop emitting
thousands of unique insertIds from becoming thousands of Tier 1 calls in
M2 — (source, source_ref) dedup cannot help there, because every entry
genuinely is unique.

Past the window a fingerprint reopens: a bug that recurs the next day is
a new incident, not a footnote on a stale one. total_occurrences is a
lifetime counter and does not reset, so a storm stays visible while being
silent.

Read and write share one transaction, so two concurrent sightings cannot
both decide they are first. Strategy and frames are recorded on every
sighting, so grouping quality can be tuned from evidence before M2 spends
against it.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: `store` — incident reads and state counters

**Files:**
- Create: `internal/store/incidents_read.go`
- Test: `internal/store/incidents_read_test.go`

**Interfaces:**
- Consumes: Tasks 4–6.
- Produces:
  - `store.Incident` — `struct{ ID int64; ProjectSlug, Source, SourceRef, Kind, Fingerprint, Title, Body, State, StateReason, Category string; Tier int; Confidence *float64; OccurrenceCount int; CostUSD float64; Metadata map[string]string; OccurredAt, CreatedAt, UpdatedAt time.Time; ClosedAt *time.Time }`.
  - `store.IncidentFilter` — `struct{ States, Projects, Sources []string; Since, Until *time.Time; Limit, Offset int }`.
  - `store.ListIncidents(ctx context.Context, db *DB, f IncidentFilter) ([]Incident, int, error)` — returns the page and the total matching count.
  - `store.GetIncident(ctx context.Context, db *DB, id int64) (Incident, bool, error)`
  - `store.CountByState(ctx context.Context, db *DB) (map[string]int, error)`
  - `store.MarkFingerprint(ctx context.Context, db *DB, incidentID int64, fingerprint string) error`

- [ ] **Step 1: Write the failing test**

Create `internal/store/incidents_read_test.go`:

```go
package store

import (
	"context"
	"testing"
	"time"
)

func seedMany(t *testing.T, db *DB, ctx context.Context, now time.Time) {
	t.Helper()
	seeds := []struct {
		ref, source, state string
	}{
		{"a", "gcplog", "received"},
		{"b", "gcplog", "triaging"},
		{"c", "github", "triaging"},
		{"d", "github", "filtered"},
	}
	for i, s := range seeds {
		p := sampleParams()
		p.SourceRef, p.Source, p.State = s.ref, s.source, s.state
		p.OccurredAt = now.Add(time.Duration(i) * time.Minute)
		if _, err := IngestIncident(ctx, db, p, now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("seeding %s: %v", s.ref, err)
		}
	}
}

func TestListIncidentsFiltersAndPaginates(t *testing.T) {
	db, ctx, now := ingestFixture(t)
	seedMany(t, db, ctx, now)

	tests := []struct {
		name      string
		filter    IncidentFilter
		wantPage  int
		wantTotal int
	}{
		{name: "no filter returns all", filter: IncidentFilter{}, wantPage: 4, wantTotal: 4},
		{name: "by state", filter: IncidentFilter{States: []string{"triaging"}}, wantPage: 2, wantTotal: 2},
		{name: "by source", filter: IncidentFilter{Sources: []string{"github"}}, wantPage: 2, wantTotal: 2},
		{name: "by several states", filter: IncidentFilter{States: []string{"received", "filtered"}}, wantPage: 2, wantTotal: 2},
		{name: "limit caps the page but not the total", filter: IncidentFilter{Limit: 1}, wantPage: 1, wantTotal: 4},
		{name: "offset walks the page", filter: IncidentFilter{Limit: 2, Offset: 2}, wantPage: 2, wantTotal: 4},
		{name: "offset past the end is empty not an error", filter: IncidentFilter{Limit: 2, Offset: 99}, wantPage: 0, wantTotal: 4},
		{name: "unknown state matches nothing", filter: IncidentFilter{States: []string{"nope"}}, wantPage: 0, wantTotal: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, total, err := ListIncidents(ctx, db, tc.filter)
			if err != nil {
				t.Fatalf("ListIncidents() error = %v, want nil", err)
			}
			if len(got) != tc.wantPage {
				t.Errorf("len(page) = %d, want %d", len(got), tc.wantPage)
			}
			if total != tc.wantTotal {
				t.Errorf("total = %d, want %d", total, tc.wantTotal)
			}
		})
	}
}

func TestListIncidentsOrdersNewestFirst(t *testing.T) {
	db, ctx, now := ingestFixture(t)
	seedMany(t, db, ctx, now)

	got, _, err := ListIncidents(ctx, db, IncidentFilter{})
	if err != nil {
		t.Fatalf("ListIncidents() error = %v", err)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].CreatedAt.Before(got[i].CreatedAt) {
			t.Fatalf("results are not newest-first at index %d", i)
		}
	}
}

func TestGetIncidentRoundTrips(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	got, ok, err := GetIncident(ctx, db, id)
	if err != nil || !ok {
		t.Fatalf("GetIncident() = %v, %v, want found", ok, err)
	}
	if got.Title != sampleParams().Title {
		t.Errorf("Title = %q, want %q", got.Title, sampleParams().Title)
	}
	if got.Metadata["severity"] != "ERROR" {
		t.Errorf("Metadata = %v, want severity ERROR", got.Metadata)
	}
	if got.OccurredAt.Location() != time.UTC {
		t.Errorf("OccurredAt location = %v, want UTC", got.OccurredAt.Location())
	}
	if got.ClosedAt != nil {
		t.Errorf("ClosedAt = %v, want nil for an open incident", got.ClosedAt)
	}
	_ = now

	t.Run("absent id reports false without erroring", func(t *testing.T) {
		_, ok, err := GetIncident(ctx, db, 999999)
		if err != nil {
			t.Fatalf("GetIncident() error = %v, want nil", err)
		}
		if ok {
			t.Error("ok = true for a missing incident")
		}
	})
}

func TestCountByState(t *testing.T) {
	db, ctx, now := ingestFixture(t)
	seedMany(t, db, ctx, now)

	counts, err := CountByState(ctx, db)
	if err != nil {
		t.Fatalf("CountByState() error = %v, want nil", err)
	}
	if counts["triaging"] != 2 {
		t.Errorf("counts[triaging] = %d, want 2", counts["triaging"])
	}
	if counts["received"] != 1 {
		t.Errorf("counts[received] = %d, want 1", counts["received"])
	}
	if _, present := counts["merged"]; present {
		t.Error("counts includes a state with no rows; only observed states belong here")
	}
}

func TestMarkFingerprint(t *testing.T) {
	db, ctx, _, id := seededIncident(t)

	if err := MarkFingerprint(ctx, db, id, "fp-xyz"); err != nil {
		t.Fatalf("MarkFingerprint() error = %v, want nil", err)
	}
	got, _, err := GetIncident(ctx, db, id)
	if err != nil {
		t.Fatalf("GetIncident() error = %v", err)
	}
	if got.Fingerprint != "fp-xyz" {
		t.Errorf("Fingerprint = %q, want %q", got.Fingerprint, "fp-xyz")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/store/ -run 'TestListIncidents|TestGetIncident|TestCountByState|TestMarkFingerprint' -v`

Expected: FAIL — `undefined: IncidentFilter`, `undefined: ListIncidents`.

- [ ] **Step 3: Write the implementation**

Create `internal/store/incidents_read.go`:

```go
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// defaultIncidentLimit bounds an unfiltered page so a dashboard request can
// never pull the whole table into memory.
const defaultIncidentLimit = 100

// Incident is a stored incident.
type Incident struct {
	ID              int64
	ProjectSlug     string // empty when unroutable
	Source          string
	SourceRef       string
	Kind            string
	Fingerprint     string
	Title           string
	Body            string
	State           string
	StateReason     string
	Category        string
	Tier            int
	Confidence      *float64
	OccurrenceCount int
	CostUSD         float64
	Metadata        map[string]string
	OccurredAt      time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ClosedAt        *time.Time
}

// IncidentFilter narrows a listing. Empty slices and nil times mean "any".
type IncidentFilter struct {
	States   []string
	Projects []string
	Sources  []string
	Since    *time.Time
	Until    *time.Time
	Limit    int
	Offset   int
}

// where builds the shared predicate and its arguments. Every value is bound as
// a placeholder, so a caller-supplied state or slug can never alter the query.
func (f IncidentFilter) where() (string, []any) {
	var clauses []string
	var args []any

	inClause := func(column string, values []string) {
		if len(values) == 0 {
			return
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")
		clauses = append(clauses, column+" IN ("+placeholders+")")
		for _, v := range values {
			args = append(args, v)
		}
	}

	inClause("state", f.States)
	inClause("project_slug", f.Projects)
	inClause("source", f.Sources)

	if f.Since != nil {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, f.Since.UTC().Format(time.RFC3339))
	}
	if f.Until != nil {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, f.Until.UTC().Format(time.RFC3339))
	}

	if len(clauses) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

const incidentColumns = `id, COALESCE(project_slug, ''), source, source_ref, kind,
	COALESCE(fingerprint, ''), title, COALESCE(body, ''), metadata_json,
	state, COALESCE(state_reason, ''), tier, confidence, COALESCE(category, ''),
	occurrence_count, cost_usd, occurred_at, created_at, updated_at, closed_at`

func scanIncident(scan func(...any) error) (Incident, error) {
	var (
		in           Incident
		metadataJSON string
		occurred     string
		created      string
		updated      string
		closed       sql.NullString
	)
	err := scan(&in.ID, &in.ProjectSlug, &in.Source, &in.SourceRef, &in.Kind,
		&in.Fingerprint, &in.Title, &in.Body, &metadataJSON,
		&in.State, &in.StateReason, &in.Tier, &in.Confidence, &in.Category,
		&in.OccurrenceCount, &in.CostUSD, &occurred, &created, &updated, &closed)
	if err != nil {
		return Incident{}, err
	}

	if err := json.Unmarshal([]byte(metadataJSON), &in.Metadata); err != nil {
		return Incident{}, fmt.Errorf("decoding metadata for incident %d: %w", in.ID, err)
	}

	for _, f := range []struct {
		raw string
		dst *time.Time
	}{{occurred, &in.OccurredAt}, {created, &in.CreatedAt}, {updated, &in.UpdatedAt}} {
		parsed, err := time.Parse(time.RFC3339, f.raw)
		if err != nil {
			return Incident{}, fmt.Errorf("parsing timestamp %q on incident %d: %w", f.raw, in.ID, err)
		}
		*f.dst = parsed.UTC()
	}

	if closed.Valid {
		parsed, err := time.Parse(time.RFC3339, closed.String)
		if err != nil {
			return Incident{}, fmt.Errorf("parsing closed_at on incident %d: %w", in.ID, err)
		}
		utc := parsed.UTC()
		in.ClosedAt = &utc
	}
	return in, nil
}

// ListIncidents returns one page of incidents, newest first, together with the
// total number matching the filter. The total is returned so the dashboard can
// paginate without a second round trip.
func ListIncidents(ctx context.Context, db *DB, f IncidentFilter) ([]Incident, int, error) {
	predicate, args := f.where()

	var total int
	if err := db.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM incidents`+predicate, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("counting incidents: %w", err)
	}

	limit := f.Limit
	if limit <= 0 {
		limit = defaultIncidentLimit
	}
	offset := max(f.Offset, 0)

	pageArgs := append(append([]any{}, args...), limit, offset)
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT `+incidentColumns+` FROM incidents`+predicate+
			` ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("listing incidents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Incident
	for rows.Next() {
		in, err := scanIncident(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scanning incident: %w", err)
		}
		out = append(out, in)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterating incidents: %w", err)
	}
	return out, total, nil
}

// GetIncident returns one incident, reporting false when absent.
func GetIncident(ctx context.Context, db *DB, id int64) (Incident, bool, error) {
	row := db.Reader().QueryRowContext(ctx,
		`SELECT `+incidentColumns+` FROM incidents WHERE id = ?`, id)

	in, err := scanIncident(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Incident{}, false, nil
	}
	if err != nil {
		return Incident{}, false, fmt.Errorf("getting incident %d: %w", id, err)
	}
	return in, true, nil
}

// CountByState returns the number of incidents in each observed state. States
// with no rows are absent rather than zero, so the caller decides which states
// are worth displaying.
func CountByState(ctx context.Context, db *DB) (map[string]int, error) {
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT state, COUNT(*) FROM incidents GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("counting incidents by state: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[string]int)
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, fmt.Errorf("scanning state count: %w", err)
		}
		counts[state] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating state counts: %w", err)
	}
	return counts, nil
}

// MarkFingerprint attaches a computed fingerprint to an incident. It is
// separate from IngestIncident because fingerprinting needs the body, which is
// only available once the process loop picks the row up.
func MarkFingerprint(ctx context.Context, db *DB, incidentID int64, fingerprint string) error {
	_, err := db.Writer().ExecContext(ctx,
		`UPDATE incidents SET fingerprint = ? WHERE id = ?`, fingerprint, incidentID)
	if err != nil {
		return fmt.Errorf("marking fingerprint on incident %d: %w", incidentID, err)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./internal/store/ -run 'TestListIncidents|TestGetIncident|TestCountByState|TestMarkFingerprint' -v`

Expected: PASS.

- [ ] **Step 5: Run the gate and commit**

Run: `make check`

```bash
git add internal/store/incidents_read.go internal/store/incidents_read_test.go
git commit -m "feat(store): incident listing, retrieval, and state counters

ListIncidents returns the page and the total matching count together, so
the dashboard paginates without a second round trip. Every filter value
is bound as a placeholder — a caller-supplied state or slug can never
alter the query — and an unfiltered page is capped so a request cannot
pull the whole table into memory.

CountByState omits states with no rows rather than zero-filling, leaving
the caller to decide which states are worth displaying.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 8: `triage` — normalisation and the fingerprint ladder

**Files:**
- Create: `internal/triage/normalize.go`
- Create: `internal/triage/fingerprint.go`
- Test: `internal/triage/normalize_test.go`
- Test: `internal/triage/fingerprint_test.go`

**Interfaces:**
- Consumes: `config.EffectiveProject.SourceRoots` (Task 1).
- Produces:
  - `triage.Normalize(line string) string`
  - `triage.ExtractFrames(body string) []string`
  - `triage.ErrorClass(title, body string) string`
  - `triage.Strategy` — a `string` type with constants `StrategySourceRoots`, `StrategyDenylist`, `StrategyAllFrames`, `StrategyWorkflow`, `StrategyNoFrames`.
  - `triage.FingerprintInput` — `struct{ ProjectSlug, ErrorClass string; Frames []string; SourceRoots []string }`.
  - `triage.FingerprintResult` — `struct{ Hash string; Strategy Strategy; Frames []string }`.
  - `triage.ComputeFingerprint(in FingerprintInput) FingerprintResult`
  - `triage.WorkflowFingerprint(projectSlug, workflow string, jobSteps []string) FingerprintResult`

**This is the heart of the milestone.** Read design doc §4.4 before writing a line. The two failure modes are not symmetric:

- **Over-collapse** — distinct bugs share a fingerprint, so the second is silently suppressed for the whole window. **Nothing in the system catches this.**
- **Under-collapse** — one bug yields several fingerprints, costing roughly proportional extra spend. Bounded by every budget ceiling and visible in occurrence counts.

Therefore: **when frame classification is uncertain, fingerprint more specifically, never less.** Step 3 of the ladder exists solely to guarantee the frame set is never empty, because `sha256(slug, error_class, "")` is the over-collapse case reached by the most common path.

- [ ] **Step 1: Write the failing normalisation test**

Create `internal/triage/normalize_test.go`:

```go
package triage

import "testing"

func TestNormalize(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "strips a line and column suffix",
			in:   "at handler (src/index.js:12:34)",
			want: "at handler (src/index.js)",
		},
		{
			name: "strips a bare line number",
			in:   "src/app/main.go:118",
			want: "src/app/main.go",
		},
		{
			name: "strips an absolute prefix down to a repo-relative path",
			in:   "/Users/someone/code/api/src/index.js:12",
			want: "src/index.js",
		},
		{
			name: "strips a hex memory address",
			in:   "panic at 0x00c0000b4180",
			want: "panic at 0xADDR",
		},
		{
			name: "strips a uuid",
			in:   "request 3f2504e0-4f89-11d3-9a0c-0305e82c3301 failed",
			want: "request UUID failed",
		},
		{
			name: "strips an rfc3339 timestamp",
			in:   "2026-08-02T11:04:05Z handler failed",
			want: "TIMESTAMP handler failed",
		},
		{
			name: "strips a bare integer that is not part of a path",
			in:   "retry attempt 4718 of 5000",
			want: "retry attempt N of N",
		},
		{
			name: "collapses runs of whitespace",
			in:   "at   handler    (src/a.js)",
			want: "at handler (src/a.js)",
		},
		{
			name: "leaves an already-clean frame untouched",
			in:   "at handler (src/index.js)",
			want: "at handler (src/index.js)",
		},
		{
			name: "empty input stays empty",
			in:   "",
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Normalize(tc.in); got != tc.want {
				t.Errorf("Normalize(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeIsStableAcrossRuns(t *testing.T) {
	// Two occurrences of one bug differ only in volatile detail. If normalisation
	// does not erase that difference, every occurrence becomes its own
	// fingerprint and suppression never engages.
	a := "2026-08-02T11:04:05Z at handler (/home/runner/work/api/src/index.js:12:9) req=3f2504e0-4f89-11d3-9a0c-0305e82c3301"
	b := "2026-08-02T14:55:01Z at handler (/home/runner/work/api/src/index.js:12:40) req=a1b2c3d4-1111-2222-3333-444455556666"

	if Normalize(a) != Normalize(b) {
		t.Errorf("two occurrences of one bug normalised differently:\n a = %q\n b = %q", Normalize(a), Normalize(b))
	}
}

func TestExtractFrames(t *testing.T) {
	const nodeTrace = `TypeError: Cannot read properties of undefined
    at handler (/app/src/index.js:12:9)
    at Layer.handle (/app/node_modules/express/lib/router/layer.js:95:5)
    at next (/app/node_modules/express/lib/router/route.js:137:13)`

	frames := ExtractFrames(nodeTrace)
	if len(frames) != 3 {
		t.Fatalf("len(frames) = %d, want 3; got %v", len(frames), frames)
	}
	if frames[0] != "at handler (src/index.js)" {
		t.Errorf("frames[0] = %q, want the normalised first frame", frames[0])
	}

	t.Run("a message with no frames yields none", func(t *testing.T) {
		if got := ExtractFrames("database connection refused"); len(got) != 0 {
			t.Errorf("ExtractFrames() = %v, want empty", got)
		}
	})
}

func TestErrorClass(t *testing.T) {
	tests := []struct {
		name  string
		title string
		body  string
		want  string
	}{
		{name: "typed exception from body", title: "", body: "TypeError: x is undefined\n    at a (src/a.js:1)", want: "TypeError"},
		{name: "go panic", title: "", body: "panic: runtime error: index out of range", want: "panic"},
		{name: "python exception", title: "", body: "ValueError: invalid literal for int()", want: "ValueError"},
		{name: "falls back to the normalised title", title: "Deploy failed for api", body: "", want: "Deploy failed for api"},
		{name: "empty everywhere", title: "", body: "", want: "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorClass(tc.title, tc.body); got != tc.want {
				t.Errorf("ErrorClass(%q, %q) = %q, want %q", tc.title, tc.body, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/triage/ -v`

Expected: FAIL — the package does not exist yet.

- [ ] **Step 3: Write the normalisation implementation**

Create `internal/triage/normalize.go`:

```go
// Package triage implements Tier 0 — the local, zero-cost filters and the
// fingerprinting that collapses an error storm into a single incident.
package triage

import (
	"regexp"
	"strings"
)

// Volatile detail that differs between two occurrences of the same bug. If any
// of this survived into a fingerprint, every occurrence would hash differently
// and suppression would never engage.
var (
	reTimestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?`)
	reUUID      = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	reHexAddr   = regexp.MustCompile(`0x[0-9a-fA-F]+`)
	reLineCol   = regexp.MustCompile(`(\.[a-zA-Z0-9]+):\d+(?::\d+)?`)
	reBareInt   = regexp.MustCompile(`\b\d+\b`)
	reSpaces    = regexp.MustCompile(`\s+`)
)

// absolutePrefixes are checkout-root markers. Everything up to and including
// one of these is machine-specific: the same bug on a laptop, a CI runner and a
// container would otherwise produce three different fingerprints.
var absolutePrefixes = []string{
	"/home/runner/work/", "/github/workspace/", "/workspace/", "/app/",
	"/usr/src/app/", "/var/task/", "/srv/", "/opt/app/",
}

// Normalize erases volatile detail from one line so two occurrences of the same
// bug produce identical text. Order matters: timestamps and UUIDs are removed
// before bare integers, or their digits would be eaten piecemeal and the
// resulting text would depend on which pattern happened to run first.
func Normalize(line string) string {
	if line == "" {
		return ""
	}

	out := reTimestamp.ReplaceAllString(line, "TIMESTAMP")
	out = reUUID.ReplaceAllString(out, "UUID")
	out = reHexAddr.ReplaceAllString(out, "0xADDR")
	out = stripAbsolutePrefix(out)
	out = reLineCol.ReplaceAllString(out, "$1")
	out = reBareInt.ReplaceAllString(out, "N")
	out = reSpaces.ReplaceAllString(out, " ")

	return strings.TrimSpace(out)
}

// stripAbsolutePrefix reduces an absolute checkout path to a repo-relative one.
// A path with no recognised marker is left alone rather than guessed at — a
// wrong guess here would merge unrelated paths, and merging is the direction
// with no backstop.
func stripAbsolutePrefix(s string) string {
	for _, prefix := range absolutePrefixes {
		if idx := strings.Index(s, prefix); idx >= 0 {
			return s[:idx] + s[idx+len(prefix):]
		}
	}
	// A generic /Users/<name>/code/<repo>/ or /root/<repo>/ style path: keep
	// everything from the last recognised source directory onward.
	for _, marker := range []string{"/src/", "/internal/", "/cmd/", "/lib/", "/app/", "/pkg/"} {
		if idx := strings.LastIndex(s, marker); idx > 0 {
			return s[:strings.LastIndex(s[:idx+1], " ")+1] + s[idx+1:]
		}
	}
	return s
}

// reFrameLine matches a stack frame in the common shapes: a leading "at " (Node,
// Java), a leading "File " (Python), or an indented path:line pair (Go).
var reFrameLine = regexp.MustCompile(`^\s*(at\s+|File\s+|\S+\.(go|js|ts|py|rb|java|kt|rs|php):\d+)`)

// maxFrames is how many frames a fingerprint considers (SPEC §4.3.2).
const maxFrames = 5

// ExtractFrames returns up to maxFrames normalised stack frames from a message
// body, in the order they appear.
func ExtractFrames(body string) []string {
	if body == "" {
		return nil
	}

	var frames []string
	for _, line := range strings.Split(body, "\n") {
		if !reFrameLine.MatchString(line) {
			continue
		}
		if normalised := Normalize(line); normalised != "" {
			frames = append(frames, normalised)
		}
		if len(frames) == maxFrames {
			break
		}
	}
	return frames
}

// reTypedError matches a leading exception class such as "TypeError:" or
// "ValueError:". Go panics are matched separately because "panic:" is lowercase.
var reTypedError = regexp.MustCompile(`^\s*([A-Z][A-Za-z0-9_]*(?:Error|Exception|Fault))\b`)

// ErrorClass identifies the kind of failure. It prefers a typed exception from
// the body, then a Go panic, then the normalised title.
func ErrorClass(title, body string) string {
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if m := reTypedError.FindStringSubmatch(trimmed); m != nil {
			return m[1]
		}
		if strings.HasPrefix(trimmed, "panic:") {
			return "panic"
		}
		break // only the first non-empty line can carry the class
	}

	if normalised := Normalize(title); normalised != "" {
		return normalised
	}
	return "unknown"
}
```

- [ ] **Step 4: Run the normalisation tests to verify they pass**

Run: `go test ./internal/triage/ -run 'TestNormalize|TestExtractFrames|TestErrorClass' -v`

Expected: PASS. If `TestNormalizeIsStableAcrossRuns` fails, fingerprinting is broken in the dangerous direction — suppression will never engage and M2 will pay for every occurrence.

- [ ] **Step 5: Write the failing fingerprint-ladder test**

Create `internal/triage/fingerprint_test.go`:

```go
package triage

import "testing"

func TestComputeFingerprintLadder(t *testing.T) {
	appFrame := "at handler (src/index.js)"
	depFrame := "at Layer.handle (node_modules/express/lib/router/layer.js)"

	tests := []struct {
		name         string
		in           FingerprintInput
		wantStrategy Strategy
		wantFrames   int
	}{
		{
			name: "declared source roots win",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
				Frames:      []string{depFrame, appFrame},
				SourceRoots: []string{"src/"},
			},
			wantStrategy: StrategySourceRoots,
			wantFrames:   1,
		},
		{
			name: "no roots declared falls back to the denylist",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
				Frames: []string{depFrame, appFrame},
			},
			wantStrategy: StrategyDenylist,
			wantFrames:   1,
		},
		{
			name: "all frames are dependencies so all are used",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
				Frames: []string{depFrame, depFrame + "2"},
			},
			wantStrategy: StrategyAllFrames,
			wantFrames:   2,
		},
		{
			name: "roots declared but none match falls through to all frames",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
				Frames:      []string{depFrame},
				SourceRoots: []string{"cmd/"},
			},
			wantStrategy: StrategyAllFrames,
			wantFrames:   1,
		},
		{
			name: "no frames at all",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
			},
			wantStrategy: StrategyNoFrames,
			wantFrames:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeFingerprint(tc.in)
			if got.Strategy != tc.wantStrategy {
				t.Errorf("Strategy = %q, want %q", got.Strategy, tc.wantStrategy)
			}
			if len(got.Frames) != tc.wantFrames {
				t.Errorf("len(Frames) = %d, want %d (frames = %v)", len(got.Frames), tc.wantFrames, got.Frames)
			}
			if got.Hash == "" {
				t.Error("Hash is empty")
			}
		})
	}
}

// TestFingerprintNeverOverCollapses is the regression test for design §4.4's
// asymmetry. Over-collapse silently suppresses a real failure and nothing in the
// system catches it, so this is the single most important test in the package.
func TestFingerprintNeverOverCollapses(t *testing.T) {
	t.Run("two distinct dependency-only bugs stay distinct", func(t *testing.T) {
		a := ComputeFingerprint(FingerprintInput{
			ProjectSlug: "api", ErrorClass: "TypeError",
			Frames: []string{"at Layer.handle (node_modules/express/lib/router/layer.js)"},
		})
		b := ComputeFingerprint(FingerprintInput{
			ProjectSlug: "api", ErrorClass: "TypeError",
			Frames: []string{"at Pool.query (node_modules/pg/lib/pool.js)"},
		})

		if a.Hash == b.Hash {
			t.Fatal("two distinct bugs share a fingerprint; excluding all frames left an empty frame set and collapsed them")
		}
		if a.Strategy != StrategyAllFrames || b.Strategy != StrategyAllFrames {
			t.Errorf("strategies = %q/%q, want all_frames for both", a.Strategy, b.Strategy)
		}
	})

	t.Run("same class different project stays distinct", func(t *testing.T) {
		a := ComputeFingerprint(FingerprintInput{ProjectSlug: "api", ErrorClass: "TypeError"})
		b := ComputeFingerprint(FingerprintInput{ProjectSlug: "worker", ErrorClass: "TypeError"})
		if a.Hash == b.Hash {
			t.Error("two projects share a fingerprint; project_slug must be part of the hash")
		}
	})

	t.Run("same frames different class stays distinct", func(t *testing.T) {
		frames := []string{"at handler (src/index.js)"}
		a := ComputeFingerprint(FingerprintInput{ProjectSlug: "api", ErrorClass: "TypeError", Frames: frames})
		b := ComputeFingerprint(FingerprintInput{ProjectSlug: "api", ErrorClass: "RangeError", Frames: frames})
		if a.Hash == b.Hash {
			t.Error("two error classes share a fingerprint")
		}
	})
}

func TestComputeFingerprintIsDeterministic(t *testing.T) {
	in := FingerprintInput{
		ProjectSlug: "api", ErrorClass: "TypeError",
		Frames: []string{"at handler (src/index.js)"}, SourceRoots: []string{"src/"},
	}
	if ComputeFingerprint(in).Hash != ComputeFingerprint(in).Hash {
		t.Error("ComputeFingerprint is not deterministic")
	}
}

func TestWorkflowFingerprint(t *testing.T) {
	a := WorkflowFingerprint("api", "ci.yml", []string{"test", "Run unit tests"})
	b := WorkflowFingerprint("api", "ci.yml", []string{"lint", "Run staticcheck"})

	if a.Hash == b.Hash {
		t.Fatal("two failing jobs in one workflow share a fingerprint; workflow name alone would collapse every ci.yml failure into one incident")
	}
	if a.Strategy != StrategyWorkflow {
		t.Errorf("Strategy = %q, want %q", a.Strategy, StrategyWorkflow)
	}
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `go test ./internal/triage/ -run 'TestComputeFingerprint|TestFingerprintNever|TestWorkflowFingerprint' -v`

Expected: FAIL — `undefined: ComputeFingerprint`, `undefined: FingerprintInput`.

- [ ] **Step 7: Write the ladder implementation**

Create `internal/triage/fingerprint.go`:

```go
package triage

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Strategy names how a fingerprint's frames were selected. It is persisted on
// the fingerprint so grouping quality can be tuned from evidence rather than
// guessed at (design §4.4.3).
type Strategy string

// The frame-selection strategies, in ladder order.
const (
	StrategySourceRoots Strategy = "source_roots"
	StrategyDenylist    Strategy = "denylist"
	StrategyAllFrames   Strategy = "all_frames"
	StrategyWorkflow    Strategy = "workflow"
	StrategyNoFrames    Strategy = "no_frames"
)

// dependencyDirs are the open-ended denylist used when a project declares no
// source roots. It is a best effort by construction — no list can enumerate
// every ecosystem's vendor directory — which is exactly why the ladder never
// lets an empty selection through.
var dependencyDirs = []string{
	"vendor/", "node_modules/", "site-packages/", "dist-packages/",
	".venv/", "venv/", ".cargo/registry/", "go/pkg/mod/", ".gem/", ".m2/",
	".gradle/caches/", ".nuget/packages/", "bundle/",
}

// FingerprintInput is everything needed to group one failure.
type FingerprintInput struct {
	ProjectSlug string
	ErrorClass  string
	Frames      []string // already normalised
	SourceRoots []string // empty selects the denylist strategy
}

// FingerprintResult is a hash together with the evidence that produced it.
type FingerprintResult struct {
	Hash     string
	Strategy Strategy
	Frames   []string
}

// ComputeFingerprint groups a failure, recording which strategy selected its
// frames.
//
// The ladder is ordered by confidence and, critically, never yields an empty
// frame set. An empty set would hash to sha256(slug, class, "") and collapse
// every same-class failure in the project into one fingerprint — the
// over-collapse failure mode, which silently suppresses real failures and which
// nothing else in the system catches. Falling back to all frames can only split
// fingerprints apart, never merge them, so it is always the safe direction
// (design §4.4.1).
func ComputeFingerprint(in FingerprintInput) FingerprintResult {
	strategy, frames := selectFrames(in)
	return FingerprintResult{
		Hash:     hashParts(in.ProjectSlug, in.ErrorClass, strings.Join(frames, "\n")),
		Strategy: strategy,
		Frames:   frames,
	}
}

func selectFrames(in FingerprintInput) (Strategy, []string) {
	if len(in.Frames) == 0 {
		return StrategyNoFrames, nil
	}

	if len(in.SourceRoots) > 0 {
		if own := filterFrames(in.Frames, func(f string) bool {
			return matchesAnyRoot(f, in.SourceRoots)
		}); len(own) > 0 {
			return StrategySourceRoots, own
		}
		// Declared roots matched nothing. Fall through rather than return
		// empty: a mis-declared root must not silently collapse the project.
		return StrategyAllFrames, capFrames(in.Frames)
	}

	if own := filterFrames(in.Frames, func(f string) bool {
		return !isDependencyFrame(f)
	}); len(own) > 0 {
		return StrategyDenylist, own
	}

	return StrategyAllFrames, capFrames(in.Frames)
}

func filterFrames(frames []string, keep func(string) bool) []string {
	var out []string
	for _, f := range frames {
		if keep(f) {
			out = append(out, f)
		}
		if len(out) == maxFrames {
			break
		}
	}
	return out
}

func capFrames(frames []string) []string {
	if len(frames) > maxFrames {
		return frames[:maxFrames]
	}
	return frames
}

// matchesAnyRoot reports whether a frame's path falls under a declared root.
// The frame text embeds the path rather than being one, so this is a substring
// test against a normalised, slash-prefixed root.
func matchesAnyRoot(frame string, roots []string) bool {
	for _, root := range roots {
		cleaned := strings.Trim(strings.TrimSpace(root), "/")
		if cleaned == "" {
			continue
		}
		if strings.Contains(frame, cleaned+"/") {
			return true
		}
	}
	return false
}

func isDependencyFrame(frame string) bool {
	for _, dir := range dependencyDirs {
		if strings.Contains(frame, dir) {
			return true
		}
	}
	return false
}

// WorkflowFingerprint groups a CI failure by its failing job and step rather
// than by a stack trace, which CI failures do not have.
//
// jobSteps must identify the failing job and step. Grouping on the workflow
// name alone is not acceptable: it would collapse every failure of ci.yml into
// a single incident, which is the over-collapse mode this design exists to
// prevent (design §4.4.2).
func WorkflowFingerprint(projectSlug, workflow string, jobSteps []string) FingerprintResult {
	frames := make([]string, 0, len(jobSteps))
	for _, s := range jobSteps {
		if normalised := Normalize(s); normalised != "" {
			frames = append(frames, normalised)
		}
	}
	return FingerprintResult{
		Hash:     hashParts(projectSlug, "workflow:"+workflow, strings.Join(frames, "\n")),
		Strategy: StrategyWorkflow,
		Frames:   frames,
	}
}

// hashParts joins its parts with a separator that cannot occur in any of them,
// so ("ab", "c") and ("a", "bc") cannot produce the same digest.
func hashParts(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
```

- [ ] **Step 8: Run the fingerprint tests to verify they pass**

Run: `go test ./internal/triage/ -v`

Expected: PASS. `TestFingerprintNeverOverCollapses` is the one that must never be weakened — if a future change makes it fail, the system has started silently swallowing real failures.

- [ ] **Step 9: Run the gate and commit**

Run: `make check`

```bash
git add internal/triage/normalize.go internal/triage/fingerprint.go \
        internal/triage/normalize_test.go internal/triage/fingerprint_test.go
git commit -m "feat(triage): normalisation and the fingerprint selection ladder

Frame selection runs declared source_roots, then a dependency denylist,
then all frames including dependencies — and never yields an empty set.
An empty set hashes to sha256(slug, class, \"\") and collapses every
same-class failure in a project into one fingerprint, silently
suppressing real failures for the whole window with nothing to catch it.
Falling back to all frames can only split fingerprints apart, never merge
them, so it is always the safe direction.

Declared roots that match nothing fall through to all_frames rather than
returning empty, so a mis-declared root cannot silently collapse a
project.

WorkflowFingerprint groups CI failures by failing job and step. Grouping
on workflow name alone would collapse every ci.yml failure into one
incident, which is the mode this design exists to prevent.

Normalisation order is load-bearing: timestamps and UUIDs are erased
before bare integers, or their digits would be eaten piecemeal and the
result would depend on pattern ordering.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 9: `triage` — the Tier 0 filter chain

**Files:**
- Create: `internal/triage/tier0.go`
- Test: `internal/triage/tier0_test.go`

**Interfaces:**
- Consumes: `config.CompileTransientPatterns` (Task 1).
- Produces:
  - `triage.Verdict` — `string` type with `VerdictPass`, `VerdictFiltered`, `VerdictSuppressed`.
  - `triage.Subject` — `struct{ ProjectSlug, Kind, Title, Body, AuthorEmail string; Quarantined, Suppressed bool }`.
  - `triage.Decision` — `struct{ Verdict Verdict; Filter, Reason string }`.
  - `triage.ChainOptions` — `struct{ TransientPatterns []*regexp.Regexp; BotEmail string }`.
  - `triage.Chain` and `triage.NewChain(opts ChainOptions) *Chain`.
  - `(*Chain).Evaluate(s Subject) Decision`
  - `(*Chain).FilterNames() []string`

**Chain order and the two absences.** SPEC §4.3.1 lists seven filters. `Unroutable` and `Duplicate` are enforced at the write boundary instead (design §3.2) — by routing and by the `incidents(source, source_ref)` UNIQUE index. The chain is therefore `Quarantined → Transient → SelfInflicted → Fingerprint → BuildSanity`, short-circuiting on the first rejection.

**Purity is deliberate.** Every filter is a pure function of `Subject`. Suppression needs a database round trip, so the caller performs it (Task 15 via `store.ObserveFingerprint`) and passes the result in as `Subject.Suppressed`. This keeps the chain exhaustively table-testable with no fixtures.

**`BuildSanity` is a registered no-op.** It needs a checkout and subprocess supervision that arrive in M3 (design §2.3). It occupies its correct chain position with a test asserting it passes everything through, so M3 replaces the body without touching a call site.

- [ ] **Step 1: Write the failing test**

Create `internal/triage/tier0_test.go`:

```go
package triage

import (
	"regexp"
	"testing"
)

func testChain(t *testing.T) *Chain {
	t.Helper()
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)connection reset by peer`),
		regexp.MustCompile(`(?i)ECONNRESET`),
		regexp.MustCompile(`(?i)the operation was canceled`),
	}
	return NewChain(ChainOptions{
		TransientPatterns: patterns,
		BotEmail:          "sentinel@example.invalid",
	})
}

func TestChainEvaluate(t *testing.T) {
	tests := []struct {
		name        string
		subject     Subject
		wantVerdict Verdict
		wantFilter  string
	}{
		{
			name:        "clean subject passes",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", Body: "at handler (src/a.js)"},
			wantVerdict: VerdictPass,
		},
		{
			name:        "quarantined project is filtered",
			subject:     Subject{ProjectSlug: "api", Quarantined: true, Title: "TypeError"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "Quarantined",
		},
		{
			name:        "transient match in the body",
			subject:     Subject{ProjectSlug: "api", Title: "job failed", Body: "read tcp: connection reset by peer"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "Transient",
		},
		{
			name:        "transient match in the title",
			subject:     Subject{ProjectSlug: "api", Title: "ECONNRESET talking to upstream"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "Transient",
		},
		{
			name:        "cancelled job is transient",
			subject:     Subject{ProjectSlug: "api", Title: "The operation was canceled."},
			wantVerdict: VerdictFiltered,
			wantFilter:  "Transient",
		},
		{
			name:        "our own commit is self-inflicted",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", AuthorEmail: "sentinel@example.invalid"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "SelfInflicted",
		},
		{
			name:        "self-inflicted match is case-insensitive",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", AuthorEmail: "Sentinel@Example.Invalid"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "SelfInflicted",
		},
		{
			name:        "a human commit is not self-inflicted",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", AuthorEmail: "person@example.com"},
			wantVerdict: VerdictPass,
		},
		{
			name:        "suppressed fingerprint",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", Suppressed: true},
			wantVerdict: VerdictSuppressed,
			wantFilter:  "Fingerprint",
		},
	}

	chain := testChain(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chain.Evaluate(tc.subject)
			if got.Verdict != tc.wantVerdict {
				t.Errorf("Verdict = %q, want %q (reason %q)", got.Verdict, tc.wantVerdict, got.Reason)
			}
			if tc.wantFilter != "" && got.Filter != tc.wantFilter {
				t.Errorf("Filter = %q, want %q", got.Filter, tc.wantFilter)
			}
			if got.Verdict != VerdictPass && got.Reason == "" {
				t.Error("a rejection carries no reason; the dashboard needs one")
			}
		})
	}
}

func TestChainShortCircuitsInOrder(t *testing.T) {
	// A quarantined project whose body is also transient must report
	// Quarantined: the first matching filter wins, and evaluation stops.
	got := testChain(t).Evaluate(Subject{
		ProjectSlug: "api",
		Quarantined: true,
		Body:        "connection reset by peer",
		Suppressed:  true,
	})
	if got.Filter != "Quarantined" {
		t.Errorf("Filter = %q, want %q; the chain must short-circuit on the first match", got.Filter, "Quarantined")
	}
}

func TestChainOrderMatchesSpec(t *testing.T) {
	// Unroutable and Duplicate are enforced at the write boundary (design §3.2),
	// so they are deliberately absent here.
	want := []string{"Quarantined", "Transient", "SelfInflicted", "Fingerprint", "BuildSanity"}
	got := testChain(t).FilterNames()

	if len(got) != len(want) {
		t.Fatalf("FilterNames() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("FilterNames()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildSanityIsANoOpUntilM3(t *testing.T) {
	// BuildSanity needs a checkout and subprocess supervision, which arrive in
	// M3. It holds its chain position so M3 is a body swap with no call-site
	// churn. If this test starts failing, BuildSanity was implemented — update
	// it rather than deleting it.
	got := testChain(t).Evaluate(Subject{ProjectSlug: "api", Title: "anything at all"})
	if got.Verdict != VerdictPass {
		t.Errorf("Verdict = %q, want pass; BuildSanity must not reject in M1", got.Verdict)
	}
}

func TestChainWithNoPatternsStillPasses(t *testing.T) {
	chain := NewChain(ChainOptions{BotEmail: "sentinel@example.invalid"})
	if got := chain.Evaluate(Subject{ProjectSlug: "api", Title: "ECONNRESET"}); got.Verdict != VerdictPass {
		t.Errorf("Verdict = %q, want pass when no transient patterns are configured", got.Verdict)
	}
}

func TestChainWithNoBotEmailDoesNotFilterEveryone(t *testing.T) {
	// An empty BotEmail must never match an empty AuthorEmail, or every event
	// without commit attribution would be discarded as self-inflicted.
	chain := NewChain(ChainOptions{})
	if got := chain.Evaluate(Subject{ProjectSlug: "api", Title: "TypeError"}); got.Verdict != VerdictPass {
		t.Errorf("Verdict = %q, want pass; an unset bot email must not filter unattributed events", got.Verdict)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/triage/ -run 'TestChain|TestBuildSanity' -v`

Expected: FAIL — `undefined: NewChain`, `undefined: Subject`.

- [ ] **Step 3: Write the implementation**

Create `internal/triage/tier0.go`:

```go
package triage

import (
	"regexp"
	"strings"
)

// Verdict is the outcome of the Tier 0 chain.
type Verdict string

// The three Tier 0 outcomes. Suppressed is distinct from Filtered because a
// suppressed event is a recurrence of a known problem, while a filtered one is
// noise — the dashboard and the budget digest treat them differently.
const (
	VerdictPass       Verdict = "pass"
	VerdictFiltered   Verdict = "filtered"
	VerdictSuppressed Verdict = "suppressed"
)

// Subject is everything a Tier 0 filter may consider. Every filter is a pure
// function of this struct: suppression needs a database round trip, so the
// caller performs it and passes the result in as Suppressed. That keeps the
// whole chain exhaustively table-testable with no fixtures.
type Subject struct {
	ProjectSlug string
	Kind        string
	Title       string
	Body        string

	// AuthorEmail is the commit author when the source carries one. Empty
	// means unattributed, which must never match the bot identity.
	AuthorEmail string

	Quarantined bool
	Suppressed  bool
}

// Decision is a chain outcome together with the filter that produced it.
type Decision struct {
	Verdict Verdict
	Filter  string
	Reason  string
}

// ChainOptions configures the filters.
type ChainOptions struct {
	// TransientPatterns come from the registry, already compiled by
	// config.CompileTransientPatterns so a malformed regex refuses startup.
	TransientPatterns []*regexp.Regexp

	// BotEmail is the sentinel's own git identity. Matching it closes the
	// self-repair loop (SPEC §4.3.1, §4.9).
	BotEmail string
}

// filter is one named, short-circuiting check.
type filter struct {
	name  string
	check func(Subject) (Verdict, string)
}

// Chain is the ordered Tier 0 filter set.
type Chain struct {
	filters []filter
}

// NewChain builds the Tier 0 chain in SPEC §4.3.1 order.
//
// Unroutable and Duplicate are deliberately absent: both are enforced at the
// write boundary, by routing and by the incidents(source, source_ref) unique
// index respectively (design §3.2). Including them here would mean re-querying
// for facts the insert already established, and would race a concurrent
// duplicate delivery.
func NewChain(opts ChainOptions) *Chain {
	botEmail := strings.ToLower(strings.TrimSpace(opts.BotEmail))

	return &Chain{filters: []filter{
		{
			name: "Quarantined",
			check: func(s Subject) (Verdict, string) {
				if s.Quarantined {
					return VerdictFiltered, "project is quarantined"
				}
				return VerdictPass, ""
			},
		},
		{
			name: "Transient",
			check: func(s Subject) (Verdict, string) {
				for _, re := range opts.TransientPatterns {
					if re.MatchString(s.Title) || re.MatchString(s.Body) {
						return VerdictFiltered, "matched transient pattern " + re.String()
					}
				}
				return VerdictPass, ""
			},
		},
		{
			name: "SelfInflicted",
			check: func(s Subject) (Verdict, string) {
				author := strings.ToLower(strings.TrimSpace(s.AuthorEmail))
				// An unset bot email must never match an unattributed event,
				// or every event without commit attribution would vanish.
				if botEmail == "" || author == "" {
					return VerdictPass, ""
				}
				if author == botEmail {
					return VerdictFiltered, "authored by the sentinel's own bot identity"
				}
				return VerdictPass, ""
			},
		},
		{
			name: "Fingerprint",
			check: func(s Subject) (Verdict, string) {
				if s.Suppressed {
					return VerdictSuppressed, "fingerprint seen within its suppression window"
				}
				return VerdictPass, ""
			},
		},
		{
			name: "BuildSanity",
			check: func(Subject) (Verdict, string) {
				// No-op until M3. This filter runs a project's
				// commands.healthcheck, which needs a checkout and subprocess
				// supervision that the workspace and runner packages provide
				// from M3 (design §2.3). It holds its chain position so that
				// milestone is a body swap with no call-site churn.
				return VerdictPass, ""
			},
		},
	}}
}

// Evaluate runs the chain, stopping at the first rejection.
func (c *Chain) Evaluate(s Subject) Decision {
	for _, f := range c.filters {
		verdict, reason := f.check(s)
		if verdict != VerdictPass {
			return Decision{Verdict: verdict, Filter: f.name, Reason: reason}
		}
	}
	return Decision{Verdict: VerdictPass}
}

// FilterNames returns the chain's filters in evaluation order.
func (c *Chain) FilterNames() []string {
	names := make([]string, 0, len(c.filters))
	for _, f := range c.filters {
		names = append(names, f.name)
	}
	return names
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/triage/ -v`

Expected: PASS — all Tier 0 tests plus the earlier fingerprint and normalisation ones.

- [ ] **Step 5: Run the gate and commit**

Run: `make check`

```bash
git add internal/triage/tier0.go internal/triage/tier0_test.go
git commit -m "feat(triage): Tier 0 filter chain

Quarantined, Transient, SelfInflicted, Fingerprint, BuildSanity — in SPEC
order, short-circuiting on the first rejection.

Unroutable and Duplicate are deliberately absent: both are enforced at the
write boundary, by routing and by the incidents(source, source_ref)
unique index. Including them here would re-query for facts the insert
already established and would race a concurrent duplicate delivery.

Every filter is a pure function of Subject. Suppression needs a database
round trip, so the caller performs it and passes the result in, which
keeps the chain exhaustively table-testable with no fixtures.

Two guards worth naming: an unset bot email never matches an
unattributed event, or every event without commit attribution would
vanish as self-inflicted; and BuildSanity is a registered no-op until M3,
holding its chain position so that milestone is a body swap.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 10: `ingest` — event types, adapter contract, and routing

**Files:**
- Create: `internal/ingest/event.go`
- Create: `internal/ingest/router.go`
- Test: `internal/ingest/router_test.go`

**Interfaces:**
- Consumes: `config.Registry`, `config.EffectiveProject` (Task 1).
- Produces:
  - `ingest.Message` — `struct{ ID, AckID string; Data []byte; Attributes map[string]string; PublishTime time.Time }`. The transport-neutral message shape.
  - `ingest.Event` — SPEC §4.2's canonical event, plus `AuthorEmail string` for the `SelfInflicted` filter.
  - `ingest.ErrIgnore`, `ingest.ErrNoAdapter`, `ingest.ErrMalformed` sentinels.
  - `ingest.Adapter` interface — `Name() string`, `Match(attrs map[string]string) bool`, `Normalize(ctx context.Context, m Message) (Event, error)`.
  - `ingest.Resolver` interface — `SlugForRepo(repo string) (string, bool)`, `SlugForLabels(labels map[string]string) (string, bool)`.
  - `ingest.RegistryResolver` and `ingest.NewRegistryResolver(reg config.Registry) *RegistryResolver`.
  - `ingest.Router` and `ingest.NewRouter(adapters ...Adapter) *Router`; `(*Router).Route(ctx context.Context, m Message) (Event, error)`.

**Why three sentinels rather than one.** They drive different handling in Task 15's subscriber, and conflating them would either lose events or spam the log:
- `ErrIgnore` — structurally valid but uninteresting (a `workflow_run` that succeeded). Ack, count, do not persist.
- `ErrNoAdapter` — no adapter claimed the message. Ack, count, log at warn.
- `ErrMalformed` — an adapter claimed it and could not parse it. Ack and **persist** as `filtered`/`unparseable`, so it stays visible rather than silently gone.

- [ ] **Step 1: Write the failing test**

Create `internal/ingest/router_test.go`:

```go
package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/config"
)

// stubAdapter is a controllable adapter for routing tests. Real adapter
// behaviour is tested in the github and gcplog test files.
type stubAdapter struct {
	name     string
	matchKey string
	event    Event
	err      error
}

func (s stubAdapter) Name() string { return s.name }

func (s stubAdapter) Match(attrs map[string]string) bool {
	_, ok := attrs[s.matchKey]
	return ok
}

func (s stubAdapter) Normalize(context.Context, Message) (Event, error) {
	if s.err != nil {
		return Event{}, s.err
	}
	return s.event, nil
}

func TestRouterSelectsTheMatchingAdapter(t *testing.T) {
	router := NewRouter(
		stubAdapter{name: "github", matchKey: "x-github-event", event: Event{Source: "github"}},
		stubAdapter{name: "gcplog", matchKey: "logging.googleapis.com/timestamp", event: Event{Source: "gcplog"}},
	)

	tests := []struct {
		name       string
		attrs      map[string]string
		wantSource string
		wantErr    error
	}{
		{
			name:       "github attributes route to github",
			attrs:      map[string]string{"x-github-event": "workflow_run"},
			wantSource: "github",
		},
		{
			name:       "logging attributes route to gcplog",
			attrs:      map[string]string{"logging.googleapis.com/timestamp": "2026-08-02T00:00:00Z"},
			wantSource: "gcplog",
		},
		{
			name:    "unclaimed message reports ErrNoAdapter",
			attrs:   map[string]string{"unrelated": "1"},
			wantErr: ErrNoAdapter,
		},
		{
			name:    "no attributes at all reports ErrNoAdapter",
			attrs:   nil,
			wantErr: ErrNoAdapter,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := router.Route(context.Background(), Message{Attributes: tc.attrs})
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Route() error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Route() error = %v, want nil", err)
			}
			if ev.Source != tc.wantSource {
				t.Errorf("Source = %q, want %q", ev.Source, tc.wantSource)
			}
		})
	}
}

func TestRouterPropagatesAdapterErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "ignore propagates unchanged", err: ErrIgnore},
		{name: "malformed propagates unchanged", err: ErrMalformed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := NewRouter(stubAdapter{name: "s", matchKey: "k", err: tc.err})
			_, err := router.Route(context.Background(), Message{Attributes: map[string]string{"k": "v"}})
			if !errors.Is(err, tc.err) {
				t.Errorf("Route() error = %v, want %v", err, tc.err)
			}
		})
	}
}

func TestRouterUsesFirstMatchingAdapter(t *testing.T) {
	router := NewRouter(
		stubAdapter{name: "first", matchKey: "k", event: Event{Source: "first"}},
		stubAdapter{name: "second", matchKey: "k", event: Event{Source: "second"}},
	)
	ev, err := router.Route(context.Background(), Message{Attributes: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	if ev.Source != "first" {
		t.Errorf("Source = %q, want %q; registration order must decide", ev.Source, "first")
	}
}

func registryFixture(t *testing.T) config.Registry {
	t.Helper()
	return config.Registry{
		Projects: []config.Project{
			{Slug: "example-api", Repo: "github.com/example/example-api", DefaultBranch: "main"},
			{Slug: "example-worker", Repo: "github.com/example/example-worker", DefaultBranch: "main"},
		},
	}
}

func TestRegistryResolverSlugForRepo(t *testing.T) {
	r := NewRegistryResolver(registryFixture(t))

	tests := []struct {
		name     string
		repo     string
		wantSlug string
		wantOK   bool
	}{
		{name: "owner/name form", repo: "example/example-api", wantSlug: "example-api", wantOK: true},
		{name: "full host form", repo: "github.com/example/example-api", wantSlug: "example-api", wantOK: true},
		{name: "case-insensitive", repo: "Example/Example-API", wantSlug: "example-api", wantOK: true},
		{name: "unknown repo", repo: "example/not-registered", wantOK: false},
		{name: "empty", repo: "", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slug, ok := r.SlugForRepo(tc.repo)
			if ok != tc.wantOK {
				t.Fatalf("SlugForRepo(%q) ok = %v, want %v", tc.repo, ok, tc.wantOK)
			}
			if ok && slug != tc.wantSlug {
				t.Errorf("slug = %q, want %q", slug, tc.wantSlug)
			}
		})
	}
}

func TestRegistryResolverSlugForLabels(t *testing.T) {
	r := NewRegistryResolver(registryFixture(t))

	tests := []struct {
		name     string
		labels   map[string]string
		wantSlug string
		wantOK   bool
	}{
		{
			name:     "service_name matches a slug",
			labels:   map[string]string{"service_name": "example-api"},
			wantSlug: "example-api", wantOK: true,
		},
		{
			name:     "function_name matches a slug",
			labels:   map[string]string{"function_name": "example-worker"},
			wantSlug: "example-worker", wantOK: true,
		},
		{
			name:     "an explicit project_slug label wins",
			labels:   map[string]string{"project_slug": "example-api", "service_name": "example-worker"},
			wantSlug: "example-api", wantOK: true,
		},
		{name: "no label matches", labels: map[string]string{"service_name": "unknown"}, wantOK: false},
		{name: "nil labels", labels: nil, wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slug, ok := r.SlugForLabels(tc.labels)
			if ok != tc.wantOK {
				t.Fatalf("SlugForLabels(%v) ok = %v, want %v", tc.labels, ok, tc.wantOK)
			}
			if ok && slug != tc.wantSlug {
				t.Errorf("slug = %q, want %q", slug, tc.wantSlug)
			}
		})
	}
}

func TestEventZeroValueIsUsable(t *testing.T) {
	var ev Event
	if ev.OccurredAt != (time.Time{}) {
		t.Error("zero Event has a non-zero OccurredAt")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/ingest/ -v`

Expected: FAIL — the package does not exist yet.

- [ ] **Step 3: Write the event and adapter contract**

Create `internal/ingest/event.go`:

```go
// Package ingest pulls events from Pub/Sub, normalises them through per-source
// adapters, and routes them to a registered project.
package ingest

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors an adapter or the router may return. They are distinct
// because the subscriber handles each differently, and conflating them would
// either lose events or spam the log (design §9):
//
//   - ErrIgnore    structurally valid but uninteresting. Ack, count, do not persist.
//   - ErrNoAdapter no adapter claimed the message. Ack, count, log at warn.
//   - ErrMalformed an adapter claimed it and could not parse it. Ack and persist
//     as filtered/unparseable, so it stays visible rather than silently gone.
var (
	ErrIgnore    = errors.New("event is uninteresting")
	ErrNoAdapter = errors.New("no adapter claimed the message")
	ErrMalformed = errors.New("event payload is malformed")
)

// Message is one Pub/Sub message, independent of the transport that fetched it.
// Keeping it transport-neutral is what lets the REST puller, a fake, and any
// future client share every adapter.
type Message struct {
	ID          string
	AckID       string
	Data        []byte
	Attributes  map[string]string
	PublishTime time.Time
}

// Event is a normalised event ready to become an incident (SPEC §4.2).
type Event struct {
	Source    string // "github" | "gcplog"
	Kind      string // "workflow_run.failed" | "issues.opened" | "log.error"
	SourceRef string // stable per-source identity; unique with Source

	// ProjectSlug is empty when the event is unroutable, which is recorded
	// rather than dropped — it usually means a stale projects.yaml.
	ProjectSlug string

	Title string
	Body  string

	// AuthorEmail carries commit attribution when the source has it, so the
	// Tier 0 SelfInflicted filter can close the self-repair loop.
	AuthorEmail string

	Metadata   map[string]string
	OccurredAt time.Time

	// Workflow and JobSteps are populated for CI failures, which have no stack
	// trace and are fingerprinted by failing job and step instead
	// (design §4.4.2).
	Workflow string
	JobSteps []string
}

// Adapter normalises one source's messages.
type Adapter interface {
	// Name identifies the adapter in logs and metrics.
	Name() string

	// Match reports whether this adapter owns the message, judged from
	// attributes alone so the body need not be parsed twice.
	Match(attrs map[string]string) bool

	// Normalize converts a raw message into a canonical event. It returns
	// ErrIgnore for messages that are structurally valid but uninteresting,
	// and ErrMalformed for ones it claimed but cannot parse.
	Normalize(ctx context.Context, m Message) (Event, error)
}

// Resolver maps source-specific identity onto a registered project slug.
type Resolver interface {
	// SlugForRepo accepts either "owner/name" or "github.com/owner/name".
	SlugForRepo(repo string) (string, bool)

	// SlugForLabels resolves a Cloud Logging resource's labels.
	SlugForLabels(labels map[string]string) (string, bool)
}
```

- [ ] **Step 4: Write the router and resolver**

Create `internal/ingest/router.go`:

```go
package ingest

import (
	"context"
	"fmt"
	"strings"

	"github.com/b1codes/triage-sentinel/internal/config"
)

// Router dispatches a message to the first adapter that claims it.
type Router struct {
	adapters []Adapter
}

// NewRouter builds a router. Registration order is significant: the first
// adapter whose Match returns true wins, so a broad adapter must be registered
// after a narrow one.
func NewRouter(adapters ...Adapter) *Router {
	return &Router{adapters: adapters}
}

// Route normalises a message through its owning adapter. It returns
// ErrNoAdapter when nothing claims the message, and propagates the adapter's
// own error otherwise.
func (r *Router) Route(ctx context.Context, m Message) (Event, error) {
	for _, a := range r.adapters {
		if !a.Match(m.Attributes) {
			continue
		}
		ev, err := a.Normalize(ctx, m)
		if err != nil {
			return Event{}, fmt.Errorf("adapter %s: %w", a.Name(), err)
		}
		return ev, nil
	}
	return Event{}, fmt.Errorf("%w: message %s", ErrNoAdapter, m.ID)
}

// RegistryResolver resolves project slugs from the loaded registry.
type RegistryResolver struct {
	byRepo map[string]string
	bySlug map[string]string
}

// labelKeys are the Cloud Logging resource labels consulted, in priority order.
// project_slug is first so an operator can pin routing explicitly when a
// service name and a slug diverge.
var labelKeys = []string{"project_slug", "service_name", "function_name", "job", "namespace_name"}

// NewRegistryResolver indexes a registry for lookup. Repository keys are
// lowercased and reduced to "owner/name", so github.com/Owner/Repo and
// owner/repo resolve identically.
func NewRegistryResolver(reg config.Registry) *RegistryResolver {
	r := &RegistryResolver{
		byRepo: make(map[string]string, len(reg.Projects)),
		bySlug: make(map[string]string, len(reg.Projects)),
	}
	for _, p := range reg.Projects {
		r.byRepo[normalizeRepo(p.Repo)] = p.Slug
		r.bySlug[strings.ToLower(p.Slug)] = p.Slug
	}
	return r
}

// SlugForRepo resolves a repository reference to a slug.
func (r *RegistryResolver) SlugForRepo(repo string) (string, bool) {
	if repo == "" {
		return "", false
	}
	slug, ok := r.byRepo[normalizeRepo(repo)]
	return slug, ok
}

// SlugForLabels resolves a Cloud Logging resource's labels to a slug by
// matching known label values against registered slugs.
//
// A logging sink cannot attach arbitrary attributes to the messages it
// publishes, so routing must be inferred from the log entry itself. When no
// label matches, the event is recorded as unroutable and shown in the
// dashboard rather than dropped — which is the signal to fix either the sink
// or the naming (SPEC §4.2).
func (r *RegistryResolver) SlugForLabels(labels map[string]string) (string, bool) {
	for _, key := range labelKeys {
		value, present := labels[key]
		if !present || value == "" {
			continue
		}
		if slug, ok := r.bySlug[strings.ToLower(value)]; ok {
			return slug, true
		}
	}
	return "", false
}

// normalizeRepo reduces a repository reference to a lowercase "owner/name".
func normalizeRepo(repo string) string {
	trimmed := strings.ToLower(strings.TrimSpace(repo))
	trimmed = strings.TrimSuffix(trimmed, ".git")
	trimmed = strings.TrimPrefix(trimmed, "https://")
	trimmed = strings.TrimPrefix(trimmed, "github.com/")

	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 {
		return trimmed
	}
	return parts[len(parts)-2] + "/" + parts[len(parts)-1]
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/ingest/ -v`

Expected: PASS.

- [ ] **Step 6: Run the gate and commit**

Run: `make check`

```bash
git add internal/ingest/event.go internal/ingest/router.go internal/ingest/router_test.go
git commit -m "feat(ingest): event contract, adapter interface, and routing

Message is transport-neutral, so the REST puller, a fake, and any future
client share every adapter.

Three sentinels rather than one, because the subscriber handles each
differently and conflating them would lose events or spam the log:
ErrIgnore acks and drops, ErrNoAdapter acks and warns, ErrMalformed acks
and persists as filtered/unparseable so it stays visible.

RegistryResolver matches Cloud Logging resource labels against registered
slugs, checking an explicit project_slug label first. A logging sink
cannot attach arbitrary attributes to what it publishes, so routing has
to be inferred from the entry — and an unresolved entry is recorded as
unroutable rather than dropped, which is the signal to fix the sink.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 11: `ingest` — the GitHub adapter and HMAC re-verification

**Files:**
- Create: `internal/ingest/github.go`
- Create: `internal/ingest/githubjobs.go`
- Test: `internal/ingest/github_test.go`
- Test data: `internal/ingest/testdata/github_workflow_run_failure.json`, `github_workflow_run_success.json`, `github_issues_opened.json`

**Interfaces:**
- Consumes: `ingest.Message`, `ingest.Event`, `ingest.Adapter`, `ingest.Resolver`, the three sentinels (Task 10).
- Produces:
  - `ingest.JobFetcher` interface — `FailedJobSteps(ctx context.Context, repo string, runID int64) ([]string, error)`.
  - `ingest.GitHubAdapter` and `ingest.NewGitHubAdapter(opts GitHubOptions) *GitHubAdapter`, where `GitHubOptions` is `struct{ Secret string; Resolver Resolver; Jobs JobFetcher; IssueLabels func(slug string) []string }`.
  - `ingest.NewGitHubJobFetcher(token string, client *http.Client) JobFetcher`
  - `ingest.ErrSignature` sentinel.

**Two-layer HMAC (SPEC §4.2.1, §10).** The Cloud Run relay verifies `X-Hub-Signature-256` before publishing, so it cannot become an open publish endpoint. This adapter **re-verifies** the forwarded signature against `GITHUB_WEBHOOK_SECRET`, so a compromised relay cannot inject events. Both comparisons use `hmac.Equal` — a `==` on the hex string leaks timing and is a plan violation.

**Job-level fingerprinting (design §4.4.2).** The `workflow_run` payload describes the run, not its jobs. Whether it carries failed job and step identifiers **must be verified against current GitHub documentation at implementation time**. This task therefore puts job discovery behind `JobFetcher`: the adapter uses payload data when present and calls the fetcher otherwise. Grouping on workflow name alone is not an acceptable fallback — it would collapse every `ci.yml` failure into one incident.

- [ ] **Step 1: Record the test payloads**

Create `internal/ingest/testdata/github_workflow_run_failure.json` — a trimmed but structurally faithful `workflow_run` webhook body:

```json
{
  "action": "completed",
  "workflow_run": {
    "id": 1234567890,
    "name": "CI",
    "path": ".github/workflows/ci.yml",
    "head_branch": "main",
    "head_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "status": "completed",
    "conclusion": "failure",
    "html_url": "https://github.com/example/example-api/actions/runs/1234567890",
    "run_number": 42,
    "updated_at": "2026-08-02T11:04:05Z",
    "head_commit": {
      "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "message": "fix: correct the off-by-one in the pager",
      "author": { "name": "A Person", "email": "person@example.com" }
    }
  },
  "repository": {
    "full_name": "example/example-api",
    "name": "example-api",
    "owner": { "login": "example" }
  }
}
```

Create `github_workflow_run_success.json` — identical but with `"conclusion": "success"`.

Create `github_issues_opened.json`:

```json
{
  "action": "opened",
  "issue": {
    "number": 17,
    "title": "Pager skips the last row",
    "body": "Steps to reproduce:\n1. open page 2\n2. the final row is missing",
    "html_url": "https://github.com/example/example-api/issues/17",
    "created_at": "2026-08-02T09:00:00Z",
    "labels": [{ "name": "bug" }],
    "user": { "login": "someone" }
  },
  "repository": {
    "full_name": "example/example-api",
    "name": "example-api",
    "owner": { "login": "example" }
  }
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/ingest/github_test.go`:

```go
package ingest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

const testSecret = "it-is-a-secret-to-everybody"

func readPayload(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading testdata/%s: %v", name, err)
	}
	return data
}

func sign(t *testing.T, secret string, body []byte) string {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func githubMessage(t *testing.T, event, file string) Message {
	t.Helper()
	body := readPayload(t, file)
	return Message{
		ID:   "msg-1",
		Data: body,
		Attributes: map[string]string{
			"x-github-event":       event,
			"x-github-delivery":    "delivery-1",
			"x-hub-signature-256":  sign(t, testSecret, body),
		},
	}
}

type fakeJobs struct {
	steps []string
	err   error
}

func (f fakeJobs) FailedJobSteps(context.Context, string, int64) ([]string, error) {
	return f.steps, f.err
}

func testGitHubAdapter(t *testing.T, jobs JobFetcher) *GitHubAdapter {
	t.Helper()
	return NewGitHubAdapter(GitHubOptions{
		Secret:   testSecret,
		Resolver: NewRegistryResolver(registryFixture(t)),
		Jobs:     jobs,
	})
}

func TestGitHubAdapterMatch(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{})

	tests := []struct {
		name  string
		attrs map[string]string
		want  bool
	}{
		{name: "github event header claims it", attrs: map[string]string{"x-github-event": "workflow_run"}, want: true},
		{name: "case-insensitive header", attrs: map[string]string{"X-GitHub-Event": "issues"}, want: true},
		{name: "logging attributes are not ours", attrs: map[string]string{"logging.googleapis.com/timestamp": "x"}, want: false},
		{name: "nil attributes", attrs: nil, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Match(tc.attrs); got != tc.want {
				t.Errorf("Match(%v) = %v, want %v", tc.attrs, got, tc.want)
			}
		})
	}
}

func TestGitHubAdapterVerifiesSignature(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{steps: []string{"test", "Run unit tests"}})
	body := readPayload(t, "github_workflow_run_failure.json")

	tests := []struct {
		name      string
		signature string
	}{
		{name: "wrong secret", signature: sign(t, "wrong-secret", body)},
		{name: "missing signature", signature: ""},
		{name: "truncated signature", signature: "sha256=abcd"},
		{name: "no algorithm prefix", signature: hex.EncodeToString([]byte("x"))},
		{name: "signature of a different body", signature: sign(t, testSecret, []byte("{}"))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := Message{
				Data: body,
				Attributes: map[string]string{
					"x-github-event":      "workflow_run",
					"x-hub-signature-256": tc.signature,
				},
			}
			_, err := a.Normalize(context.Background(), m)
			if !errors.Is(err, ErrSignature) {
				t.Fatalf("Normalize() error = %v, want ErrSignature; a compromised relay must not be able to inject events", err)
			}
		})
	}
}

func TestGitHubAdapterNormalizesWorkflowRunFailure(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{steps: []string{"test", "Run unit tests"}})

	ev, err := a.Normalize(context.Background(), githubMessage(t, "workflow_run", "github_workflow_run_failure.json"))
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil", err)
	}

	checks := []struct {
		name string
		got  string
		want string
	}{
		{name: "source", got: ev.Source, want: "github"},
		{name: "kind", got: ev.Kind, want: "workflow_run.failed"},
		{name: "source ref", got: ev.SourceRef, want: "workflow_run:1234567890"},
		{name: "project slug", got: ev.ProjectSlug, want: "example-api"},
		{name: "workflow", got: ev.Workflow, want: "CI"},
		{name: "author email", got: ev.AuthorEmail, want: "person@example.com"},
	}
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("got %q, want %q", c.got, c.want)
			}
		})
	}

	t.Run("failed job steps are captured for fingerprinting", func(t *testing.T) {
		if len(ev.JobSteps) != 2 {
			t.Fatalf("JobSteps = %v, want two entries; fingerprinting on workflow name alone would collapse every ci.yml failure", ev.JobSteps)
		}
	})

	t.Run("occurred at is parsed", func(t *testing.T) {
		if ev.OccurredAt.IsZero() {
			t.Error("OccurredAt is zero")
		}
	})

	t.Run("metadata carries the run url", func(t *testing.T) {
		if ev.Metadata["html_url"] == "" {
			t.Error("Metadata[html_url] is empty; the dashboard links to it")
		}
	})
}

func TestGitHubAdapterIgnoresUninteresting(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{})

	tests := []struct {
		name  string
		event string
		file  string
	}{
		{name: "successful workflow run", event: "workflow_run", file: "github_workflow_run_success.json"},
		{name: "unhandled event type", event: "star", file: "github_issues_opened.json"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.Normalize(context.Background(), githubMessage(t, tc.event, tc.file))
			if !errors.Is(err, ErrIgnore) {
				t.Errorf("Normalize() error = %v, want ErrIgnore", err)
			}
		})
	}
}

func TestGitHubAdapterNormalizesIssue(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{})

	ev, err := a.Normalize(context.Background(), githubMessage(t, "issues", "github_issues_opened.json"))
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil", err)
	}
	if ev.Kind != "issues.opened" {
		t.Errorf("Kind = %q, want %q", ev.Kind, "issues.opened")
	}
	if ev.SourceRef != "issue:example/example-api#17" {
		t.Errorf("SourceRef = %q, want %q", ev.SourceRef, "issue:example/example-api#17")
	}
	if ev.AuthorEmail != "" {
		t.Errorf("AuthorEmail = %q, want empty; an issue has no commit author", ev.AuthorEmail)
	}
}

func TestGitHubAdapterRejectsMalformedBody(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{})
	body := []byte(`{"action": "completed", "workflow_run":`)

	_, err := a.Normalize(context.Background(), Message{
		Data: body,
		Attributes: map[string]string{
			"x-github-event":      "workflow_run",
			"x-hub-signature-256": sign(t, testSecret, body),
		},
	})
	if !errors.Is(err, ErrMalformed) {
		t.Errorf("Normalize() error = %v, want ErrMalformed", err)
	}
}

func TestGitHubAdapterUnroutableRepoKeepsEmptySlug(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{steps: []string{"test"}})
	body := readPayload(t, "github_workflow_run_failure.json")
	// Repoint the payload at a repository that is not registered.
	body = []byte(string(body))
	m := Message{
		Data: []byte(replaceAll(string(body), "example/example-api", "example/not-registered")),
		Attributes: map[string]string{
			"x-github-event":      "workflow_run",
			"x-hub-signature-256": "",
		},
	}
	m.Attributes["x-hub-signature-256"] = sign(t, testSecret, m.Data)

	ev, err := a.Normalize(context.Background(), m)
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil; an unroutable event must normalise so it can be recorded", err)
	}
	if ev.ProjectSlug != "" {
		t.Errorf("ProjectSlug = %q, want empty for an unregistered repository", ev.ProjectSlug)
	}
}

// replaceAll avoids importing strings solely for one call in a test file that
// otherwise needs none.
func replaceAll(s, old, new string) string {
	out := ""
	for {
		i := indexOf(s, old)
		if i < 0 {
			return out + s
		}
		out += s[:i] + new
		s = s[i+len(old):]
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./internal/ingest/ -run TestGitHub -v`

Expected: FAIL — `undefined: NewGitHubAdapter`, `undefined: ErrSignature`.

- [ ] **Step 4: Write the adapter**

Create `internal/ingest/github.go`:

```go
package ingest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrSignature is returned when a forwarded webhook signature does not verify.
var ErrSignature = errors.New("webhook signature verification failed")

// JobFetcher resolves the failing job and step names for a workflow run.
//
// This exists because the workflow_run webhook payload describes the run, not
// its jobs, and whether it carries job-level detail must be verified against
// current GitHub documentation at implementation time (design §4.4.2). Putting
// discovery behind an interface keeps the adapter testable either way, and
// keeps fingerprinting off the workflow name — which would collapse every
// ci.yml failure into a single incident.
type JobFetcher interface {
	FailedJobSteps(ctx context.Context, repo string, runID int64) ([]string, error)
}

// GitHubOptions configures the adapter.
type GitHubOptions struct {
	// Secret is GITHUB_WEBHOOK_SECRET. The Cloud Run relay already verified
	// the signature; re-verifying here means a compromised relay cannot inject
	// events (SPEC §4.2.1).
	Secret   string
	Resolver Resolver
	Jobs     JobFetcher
}

// GitHubAdapter normalises GitHub webhook deliveries forwarded by the relay.
type GitHubAdapter struct {
	secret   []byte
	resolver Resolver
	jobs     JobFetcher
}

// NewGitHubAdapter builds the adapter.
func NewGitHubAdapter(opts GitHubOptions) *GitHubAdapter {
	return &GitHubAdapter{
		secret:   []byte(opts.Secret),
		resolver: opts.Resolver,
		jobs:     opts.Jobs,
	}
}

// Name identifies the adapter.
func (a *GitHubAdapter) Name() string { return "github" }

// Match claims any message carrying a GitHub event header.
func (a *GitHubAdapter) Match(attrs map[string]string) bool {
	_, ok := attrValue(attrs, "x-github-event")
	return ok
}

// attrValue reads an attribute case-insensitively. Pub/Sub preserves attribute
// case, and GitHub's own header casing has changed over time, so matching
// exactly would be brittle.
func attrValue(attrs map[string]string, key string) (string, bool) {
	for k, v := range attrs {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

type githubRepository struct {
	FullName string `json:"full_name"`
}

type githubCommitAuthor struct {
	Email string `json:"email"`
}

type githubHeadCommit struct {
	ID      string             `json:"id"`
	Message string             `json:"message"`
	Author  githubCommitAuthor `json:"author"`
}

type githubWorkflowRun struct {
	ID         int64            `json:"id"`
	Name       string           `json:"name"`
	Path       string           `json:"path"`
	HeadBranch string           `json:"head_branch"`
	Status     string           `json:"status"`
	Conclusion string           `json:"conclusion"`
	HTMLURL    string           `json:"html_url"`
	UpdatedAt  time.Time        `json:"updated_at"`
	HeadCommit githubHeadCommit `json:"head_commit"`
}

type githubIssue struct {
	Number    int       `json:"number"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	HTMLURL   string    `json:"html_url"`
	CreatedAt time.Time `json:"created_at"`
	Labels    []struct {
		Name string `json:"name"`
	} `json:"labels"`
}

type githubPayload struct {
	Action      string             `json:"action"`
	WorkflowRun *githubWorkflowRun `json:"workflow_run"`
	Issue       *githubIssue       `json:"issue"`
	Repository  githubRepository   `json:"repository"`
}

// Normalize verifies the signature, then converts the delivery to an Event.
func (a *GitHubAdapter) Normalize(ctx context.Context, m Message) (Event, error) {
	if err := a.verify(m); err != nil {
		return Event{}, err
	}

	eventType, _ := attrValue(m.Attributes, "x-github-event")

	var payload githubPayload
	if err := json.Unmarshal(m.Data, &payload); err != nil {
		return Event{}, fmt.Errorf("%w: decoding %s delivery: %w", ErrMalformed, eventType, err)
	}

	switch strings.ToLower(eventType) {
	case "workflow_run":
		return a.normalizeWorkflowRun(ctx, payload)
	case "issues":
		return a.normalizeIssue(payload)
	default:
		return Event{}, fmt.Errorf("%w: github event %q is not subscribed", ErrIgnore, eventType)
	}
}

// verify re-checks the HMAC the relay already verified. hmac.Equal is required:
// comparing hex strings with == leaks timing information.
func (a *GitHubAdapter) verify(m Message) error {
	signature, ok := attrValue(m.Attributes, "x-hub-signature-256")
	if !ok || signature == "" {
		return fmt.Errorf("%w: no x-hub-signature-256 attribute", ErrSignature)
	}

	encoded, found := strings.CutPrefix(signature, "sha256=")
	if !found {
		return fmt.Errorf("%w: signature %q has no sha256= prefix", ErrSignature, signature)
	}
	provided, err := hex.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("%w: signature is not hex: %w", ErrSignature, err)
	}

	mac := hmac.New(sha256.New, a.secret)
	mac.Write(m.Data)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return fmt.Errorf("%w: computed digest does not match", ErrSignature)
	}
	return nil
}

func (a *GitHubAdapter) normalizeWorkflowRun(ctx context.Context, p githubPayload) (Event, error) {
	run := p.WorkflowRun
	if run == nil {
		return Event{}, fmt.Errorf("%w: workflow_run delivery has no workflow_run object", ErrMalformed)
	}
	if !strings.EqualFold(run.Conclusion, "failure") {
		return Event{}, fmt.Errorf("%w: workflow run concluded %q", ErrIgnore, run.Conclusion)
	}

	slug, _ := a.resolver.SlugForRepo(p.Repository.FullName)

	steps, err := a.failedSteps(ctx, p.Repository.FullName, run.ID)
	if err != nil {
		// Job discovery is best-effort: a failure here must not lose the
		// event. Fingerprinting degrades to the run's identity, which is
		// narrower than the workflow name and therefore safe (design §4.4).
		steps = []string{"run:" + strconv.FormatInt(run.ID, 10)}
	}

	return Event{
		Source:      "github",
		Kind:        "workflow_run.failed",
		SourceRef:   "workflow_run:" + strconv.FormatInt(run.ID, 10),
		ProjectSlug: slug,
		Title:       fmt.Sprintf("%s failed on %s", run.Name, run.HeadBranch),
		Body:        run.HeadCommit.Message,
		AuthorEmail: run.HeadCommit.Author.Email,
		Workflow:    run.Name,
		JobSteps:    steps,
		OccurredAt:  run.UpdatedAt,
		Metadata: map[string]string{
			"repository":  p.Repository.FullName,
			"html_url":    run.HTMLURL,
			"head_sha":    run.HeadCommit.ID,
			"head_branch": run.HeadBranch,
			"workflow":    run.Name,
			"path":        run.Path,
		},
	}, nil
}

func (a *GitHubAdapter) failedSteps(ctx context.Context, repo string, runID int64) ([]string, error) {
	if a.jobs == nil {
		return nil, errors.New("no job fetcher configured")
	}
	steps, err := a.jobs.FailedJobSteps(ctx, repo, runID)
	if err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		return nil, errors.New("no failing job reported")
	}
	return steps, nil
}

func (a *GitHubAdapter) normalizeIssue(p githubPayload) (Event, error) {
	issue := p.Issue
	if issue == nil {
		return Event{}, fmt.Errorf("%w: issues delivery has no issue object", ErrMalformed)
	}
	if p.Action != "opened" && p.Action != "labeled" {
		return Event{}, fmt.Errorf("%w: issue action %q is not subscribed", ErrIgnore, p.Action)
	}

	slug, _ := a.resolver.SlugForRepo(p.Repository.FullName)

	labels := make([]string, 0, len(issue.Labels))
	for _, l := range issue.Labels {
		labels = append(labels, l.Name)
	}

	return Event{
		Source:      "github",
		Kind:        "issues." + p.Action,
		SourceRef:   fmt.Sprintf("issue:%s#%d", p.Repository.FullName, issue.Number),
		ProjectSlug: slug,
		Title:       issue.Title,
		Body:        issue.Body,
		OccurredAt:  issue.CreatedAt,
		Metadata: map[string]string{
			"repository": p.Repository.FullName,
			"html_url":   issue.HTMLURL,
			"labels":     strings.Join(labels, ","),
			"number":     strconv.Itoa(issue.Number),
		},
	}, nil
}
```

- [ ] **Step 5: Write the job fetcher**

Create `internal/ingest/githubjobs.go`:

```go
package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// githubAPIBase is the REST API root. It is a variable so tests can point it at
// an httptest server.
var githubAPIBase = "https://api.github.com"

// httpJobFetcher reads failing job and step names from the Actions API.
type httpJobFetcher struct {
	token  string
	client *http.Client
}

// NewGitHubJobFetcher returns a JobFetcher backed by the Actions API. It needs
// only read scope; M4 is the first milestone requiring write. Passing nil for
// client uses a client with a bounded timeout.
func NewGitHubJobFetcher(token string, client *http.Client) JobFetcher {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &httpJobFetcher{token: token, client: client}
}

type jobsResponse struct {
	Jobs []struct {
		Name       string `json:"name"`
		Conclusion string `json:"conclusion"`
		Steps      []struct {
			Name       string `json:"name"`
			Conclusion string `json:"conclusion"`
		} `json:"steps"`
	} `json:"jobs"`
}

// FailedJobSteps returns the names of the first failing job and its first
// failing step, which is what the workflow fingerprint groups on.
func (f *httpJobFetcher) FailedJobSteps(ctx context.Context, repo string, runID int64) ([]string, error) {
	url := fmt.Sprintf("%s/repos/%s/actions/runs/%d/jobs",
		githubAPIBase, strings.TrimPrefix(repo, "github.com/"), runID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building jobs request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if f.token != "" {
		req.Header.Set("Authorization", "Bearer "+f.token)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching jobs for run %d: %w", runID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching jobs for run %d: status %s", runID, resp.Status)
	}

	var decoded jobsResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decoding jobs for run %d: %w", runID, err)
	}

	for _, job := range decoded.Jobs {
		if !strings.EqualFold(job.Conclusion, "failure") {
			continue
		}
		for _, step := range job.Steps {
			if strings.EqualFold(step.Conclusion, "failure") {
				return []string{job.Name, step.Name}, nil
			}
		}
		return []string{job.Name}, nil
	}
	return nil, fmt.Errorf("no failing job in run %s", strconv.FormatInt(runID, 10))
}
```

- [ ] **Step 6: Add a job-fetcher test**

Append to `internal/ingest/github_test.go`:

```go
func TestHTTPJobFetcher(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer tok")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jobs":[
			{"name":"lint","conclusion":"success","steps":[]},
			{"name":"test","conclusion":"failure","steps":[
				{"name":"Checkout","conclusion":"success"},
				{"name":"Run unit tests","conclusion":"failure"}
			]}
		]}`))
	}))
	defer srv.Close()

	original := githubAPIBase
	githubAPIBase = srv.URL
	t.Cleanup(func() { githubAPIBase = original })

	steps, err := NewGitHubJobFetcher("tok", srv.Client()).
		FailedJobSteps(context.Background(), "example/example-api", 1234567890)
	if err != nil {
		t.Fatalf("FailedJobSteps() error = %v, want nil", err)
	}
	if len(steps) != 2 || steps[0] != "test" || steps[1] != "Run unit tests" {
		t.Errorf("steps = %v, want [test, Run unit tests]", steps)
	}
}
```

Add `net/http` and `net/http/httptest` to that file's imports.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./internal/ingest/ -run 'TestGitHub|TestHTTPJobFetcher' -v`

Expected: PASS. `TestGitHubAdapterVerifiesSignature` must pass all five subtests — it is the guard that stops a compromised relay injecting events.

- [ ] **Step 8: Run the gate and commit**

Run: `make check`

```bash
git add internal/ingest/github.go internal/ingest/githubjobs.go \
        internal/ingest/github_test.go internal/ingest/testdata
git commit -m "feat(ingest): GitHub adapter with HMAC re-verification

The relay already verifies X-Hub-Signature-256 before publishing, so it
cannot become an open publish endpoint. Re-verifying here means a
compromised relay cannot inject events either. hmac.Equal is required —
comparing hex strings with == leaks timing.

Job discovery sits behind a JobFetcher interface. The workflow_run
payload describes the run, not its jobs, and whether it carries job-level
detail must be verified against current GitHub docs at implementation
time. Fingerprinting on workflow name alone is not an acceptable
fallback: it would collapse every ci.yml failure into one incident. When
discovery fails the fingerprint degrades to the run id, which is narrower
and therefore safe.

An unregistered repository normalises with an empty slug rather than
erroring, so the event is recorded as unroutable and stays visible.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 12: `ingest` — the GCP Cloud Logging adapter

**Files:**
- Create: `internal/ingest/gcplog.go`
- Test: `internal/ingest/gcplog_test.go`
- Test data: `internal/ingest/testdata/gcplog_text_error.json`, `gcplog_json_stack.json`

**Interfaces:**
- Consumes: Task 10's types; `ingest.Resolver`.
- Produces: `ingest.GCPLogAdapter` and `ingest.NewGCPLogAdapter(resolver Resolver) *GCPLogAdapter`.

**`SourceRef` is `gcplog:<insertId>`, globally unique per entry.** That makes `(source, source_ref)` deduplication useless against a crash loop, because every entry genuinely is distinct — **fingerprint suppression is what prevents an error storm becoming an invoice** (SPEC §4.2.2). This adapter's job is to extract a body the fingerprinter can work with.

- [ ] **Step 1: Record the test payloads**

Create `internal/ingest/testdata/gcplog_text_error.json`:

```json
{
  "insertId": "1a2b3c4d5e",
  "timestamp": "2026-08-02T11:04:05.123456Z",
  "severity": "ERROR",
  "logName": "projects/example/logs/run.googleapis.com%2Fstderr",
  "resource": {
    "type": "cloud_run_revision",
    "labels": {
      "service_name": "example-api",
      "revision_name": "example-api-00042-abc",
      "location": "us-central1"
    }
  },
  "textPayload": "TypeError: Cannot read properties of undefined\n    at handler (/app/src/index.js:12:9)\n    at Layer.handle (/app/node_modules/express/lib/router/layer.js:95:5)"
}
```

Create `internal/ingest/testdata/gcplog_json_stack.json`:

```json
{
  "insertId": "9z8y7x6w5v",
  "timestamp": "2026-08-02T12:00:00Z",
  "severity": "CRITICAL",
  "resource": {
    "type": "cloud_run_revision",
    "labels": { "service_name": "example-worker" }
  },
  "jsonPayload": {
    "message": "ValueError: invalid literal for int() with base 10: 'x'",
    "stack_trace": "Traceback (most recent call last):\n  File \"/app/src/worker.py\", line 41, in run\n    total = int(value)\nValueError: invalid literal for int() with base 10: 'x'"
  }
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/ingest/gcplog_test.go`:

```go
package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func gcpMessage(t *testing.T, file string) Message {
	t.Helper()
	return Message{
		ID:   "msg-gcp",
		Data: readPayload(t, file),
		Attributes: map[string]string{
			"logging.googleapis.com/timestamp": "2026-08-02T11:04:05Z",
		},
	}
}

func testGCPAdapter(t *testing.T) *GCPLogAdapter {
	t.Helper()
	return NewGCPLogAdapter(NewRegistryResolver(registryFixture(t)))
}

func TestGCPLogAdapterMatch(t *testing.T) {
	a := testGCPAdapter(t)

	tests := []struct {
		name  string
		attrs map[string]string
		want  bool
	}{
		{name: "logging timestamp attribute", attrs: map[string]string{"logging.googleapis.com/timestamp": "x"}, want: true},
		{name: "log name attribute", attrs: map[string]string{"logging.googleapis.com/logName": "x"}, want: true},
		{name: "github attributes are not ours", attrs: map[string]string{"x-github-event": "push"}, want: false},
		{name: "nil attributes", attrs: nil, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Match(tc.attrs); got != tc.want {
				t.Errorf("Match(%v) = %v, want %v", tc.attrs, got, tc.want)
			}
		})
	}
}

func TestGCPLogAdapterNormalizesTextPayload(t *testing.T) {
	ev, err := testGCPAdapter(t).Normalize(context.Background(), gcpMessage(t, "gcplog_text_error.json"))
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil", err)
	}

	checks := []struct{ name, got, want string }{
		{name: "source", got: ev.Source, want: "gcplog"},
		{name: "kind", got: ev.Kind, want: "log.error"},
		{name: "source ref is the insert id", got: ev.SourceRef, want: "gcplog:1a2b3c4d5e"},
		{name: "slug resolved from service_name", got: ev.ProjectSlug, want: "example-api"},
	}
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("got %q, want %q", c.got, c.want)
			}
		})
	}

	t.Run("title is the first payload line", func(t *testing.T) {
		if ev.Title != "TypeError: Cannot read properties of undefined" {
			t.Errorf("Title = %q, want the first line", ev.Title)
		}
	})

	t.Run("body keeps the stack for fingerprinting", func(t *testing.T) {
		if !strings.Contains(ev.Body, "src/index.js") {
			t.Errorf("Body = %q, want it to retain the stack frames", ev.Body)
		}
	})

	t.Run("severity is in metadata", func(t *testing.T) {
		if ev.Metadata["severity"] != "ERROR" {
			t.Errorf("Metadata[severity] = %q, want ERROR", ev.Metadata["severity"])
		}
	})

	t.Run("timestamp is parsed", func(t *testing.T) {
		if ev.OccurredAt.IsZero() {
			t.Error("OccurredAt is zero")
		}
	})
}

func TestGCPLogAdapterPrefersStackTraceFromJSONPayload(t *testing.T) {
	ev, err := testGCPAdapter(t).Normalize(context.Background(), gcpMessage(t, "gcplog_json_stack.json"))
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil", err)
	}
	if ev.ProjectSlug != "example-worker" {
		t.Errorf("ProjectSlug = %q, want %q", ev.ProjectSlug, "example-worker")
	}
	if !strings.Contains(ev.Body, "worker.py") {
		t.Errorf("Body = %q, want the stack_trace field, which is what fingerprinting needs", ev.Body)
	}
	if ev.Title != "ValueError: invalid literal for int() with base 10: 'x'" {
		t.Errorf("Title = %q, want the message field", ev.Title)
	}
}

func TestGCPLogAdapterIgnoresLowSeverity(t *testing.T) {
	tests := []struct{ name, severity string }{
		{name: "info", severity: "INFO"},
		{name: "debug", severity: "DEBUG"},
		{name: "notice", severity: "NOTICE"},
		{name: "warning", severity: "WARNING"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := replaceAll(string(readPayload(t, "gcplog_text_error.json")), `"severity": "ERROR"`, `"severity": "`+tc.severity+`"`)
			_, err := testGCPAdapter(t).Normalize(context.Background(), Message{
				Data:       []byte(body),
				Attributes: map[string]string{"logging.googleapis.com/timestamp": "x"},
			})
			if !errors.Is(err, ErrIgnore) {
				t.Errorf("Normalize() error = %v, want ErrIgnore for severity %s", err, tc.severity)
			}
		})
	}
}

func TestGCPLogAdapterRejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		body string
		want error
	}{
		{name: "not json", body: `{"insertId":`, want: ErrMalformed},
		{name: "no insert id", body: `{"severity":"ERROR","textPayload":"boom"}`, want: ErrMalformed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := testGCPAdapter(t).Normalize(context.Background(), Message{
				Data:       []byte(tc.body),
				Attributes: map[string]string{"logging.googleapis.com/timestamp": "x"},
			})
			if !errors.Is(err, tc.want) {
				t.Errorf("Normalize() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestGCPLogAdapterUnresolvedLabelsAreUnroutable(t *testing.T) {
	body := replaceAll(string(readPayload(t, "gcplog_text_error.json")), "example-api", "not-registered")

	ev, err := testGCPAdapter(t).Normalize(context.Background(), Message{
		Data:       []byte(body),
		Attributes: map[string]string{"logging.googleapis.com/timestamp": "x"},
	})
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil; an unroutable entry must still normalise", err)
	}
	if ev.ProjectSlug != "" {
		t.Errorf("ProjectSlug = %q, want empty", ev.ProjectSlug)
	}
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./internal/ingest/ -run TestGCPLog -v`

Expected: FAIL — `undefined: NewGCPLogAdapter`.

- [ ] **Step 4: Write the adapter**

Create `internal/ingest/gcplog.go`:

```go
package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// actionableSeverities are the Cloud Logging severities worth opening an
// incident for. Anything lower is ignored at zero cost, before any storage or
// fingerprinting work happens.
var actionableSeverities = map[string]bool{
	"ERROR": true, "CRITICAL": true, "ALERT": true, "EMERGENCY": true,
}

// GCPLogAdapter normalises Cloud Logging entries delivered by a logging sink.
type GCPLogAdapter struct {
	resolver Resolver
}

// NewGCPLogAdapter builds the adapter.
func NewGCPLogAdapter(resolver Resolver) *GCPLogAdapter {
	return &GCPLogAdapter{resolver: resolver}
}

// Name identifies the adapter.
func (a *GCPLogAdapter) Name() string { return "gcplog" }

// Match claims any message carrying a Cloud Logging attribute. A logging sink
// stamps these onto every message it publishes.
func (a *GCPLogAdapter) Match(attrs map[string]string) bool {
	for k := range attrs {
		if strings.HasPrefix(strings.ToLower(k), "logging.googleapis.com/") {
			return true
		}
	}
	return false
}

type logResource struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels"`
}

type logEntry struct {
	InsertID    string            `json:"insertId"`
	Timestamp   time.Time         `json:"timestamp"`
	Severity    string            `json:"severity"`
	LogName     string            `json:"logName"`
	Resource    logResource       `json:"resource"`
	Labels      map[string]string `json:"labels"`
	TextPayload string            `json:"textPayload"`
	JSONPayload map[string]any    `json:"jsonPayload"`
}

// Normalize converts a log entry to an Event.
//
// SourceRef is gcplog:<insertId>, which is globally unique per entry. That
// makes (source, source_ref) deduplication useless against a crash loop —
// every entry genuinely is distinct — so fingerprint suppression is what stops
// an error storm becoming an invoice (SPEC §4.2.2). Extracting a body the
// fingerprinter can work with is therefore this adapter's most important job.
func (a *GCPLogAdapter) Normalize(_ context.Context, m Message) (Event, error) {
	var entry logEntry
	if err := json.Unmarshal(m.Data, &entry); err != nil {
		return Event{}, fmt.Errorf("%w: decoding log entry: %w", ErrMalformed, err)
	}
	if entry.InsertID == "" {
		return Event{}, fmt.Errorf("%w: log entry has no insertId, so it cannot be deduplicated", ErrMalformed)
	}

	severity := strings.ToUpper(entry.Severity)
	if !actionableSeverities[severity] {
		return Event{}, fmt.Errorf("%w: severity %q is below ERROR", ErrIgnore, entry.Severity)
	}

	title, body := entry.messageAndBody()
	if title == "" && body == "" {
		return Event{}, fmt.Errorf("%w: log entry has no payload", ErrIgnore)
	}

	slug, _ := a.resolver.SlugForLabels(mergeLabels(entry.Resource.Labels, entry.Labels))

	metadata := map[string]string{
		"severity":      severity,
		"log_name":      entry.LogName,
		"resource_type": entry.Resource.Type,
		"insert_id":     entry.InsertID,
	}
	for k, v := range entry.Resource.Labels {
		metadata["resource."+k] = v
	}

	return Event{
		Source:      "gcplog",
		Kind:        "log.error",
		SourceRef:   "gcplog:" + entry.InsertID,
		ProjectSlug: slug,
		Title:       title,
		Body:        body,
		OccurredAt:  entry.Timestamp,
		Metadata:    metadata,
	}, nil
}

// messageAndBody extracts a one-line title and the fullest available body.
// A structured stack_trace is preferred over the message, because the frames
// are what fingerprinting groups on.
func (e logEntry) messageAndBody() (string, string) {
	if e.TextPayload != "" {
		return firstLine(e.TextPayload), e.TextPayload
	}

	message := stringField(e.JSONPayload, "message")
	stack := stringField(e.JSONPayload, "stack_trace", "stack", "exception", "error")

	switch {
	case stack != "":
		title := message
		if title == "" {
			title = firstLine(stack)
		}
		return firstLine(title), stack
	case message != "":
		return firstLine(message), message
	}

	if len(e.JSONPayload) > 0 {
		if encoded, err := json.Marshal(e.JSONPayload); err == nil {
			return firstLine(string(encoded)), string(encoded)
		}
	}
	return "", ""
}

func stringField(payload map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := payload[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return strings.TrimSpace(s[:idx])
	}
	return strings.TrimSpace(s)
}

// mergeLabels combines resource and entry labels, with entry labels winning so
// an explicit project_slug label can override an inferred service name.
func mergeLabels(resource, entry map[string]string) map[string]string {
	merged := make(map[string]string, len(resource)+len(entry))
	for k, v := range resource {
		merged[k] = v
	}
	for k, v := range entry {
		merged[k] = v
	}
	return merged
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/ingest/ -v`

Expected: PASS — every ingest test including the router and GitHub ones.

- [ ] **Step 6: Run the gate and commit**

Run: `make check`

```bash
git add internal/ingest/gcplog.go internal/ingest/gcplog_test.go internal/ingest/testdata
git commit -m "feat(ingest): GCP Cloud Logging adapter

SourceRef is gcplog:<insertId>, globally unique per entry, so
(source, source_ref) deduplication is useless against a crash loop —
every entry genuinely is distinct. Fingerprint suppression is what stops
a storm becoming an invoice, which makes extracting a usable body this
adapter's most important job: a structured stack_trace is preferred over
the message, because the frames are what grouping works on.

Severities below ERROR are ignored before any storage or fingerprinting
work happens. Entries whose labels match no registered slug normalise
with an empty slug and are recorded as unroutable rather than dropped.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 13: `ingest` — the REST Pub/Sub puller

**Files:**
- Create: `internal/ingest/pull.go`
- Test: `internal/ingest/pull_test.go`
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes: `ingest.Message` (Task 10).
- Produces:
  - `ingest.Puller` interface — `Pull(ctx context.Context, max int) ([]Message, error)`, `Ack(ctx context.Context, ackIDs []string) error`.
  - `ingest.RESTPuller` and `ingest.NewRESTPuller(opts RESTOptions) (*RESTPuller, error)`, where `RESTOptions` is `struct{ Subscription string; Client *http.Client; BaseURL string }`.
  - `ingest.NewPubSubClient(ctx context.Context) (*http.Client, error)` — an oauth2 client scoped to Pub/Sub.
  - `ingest.ErrPull` sentinel.

**This is design decision §2.1.** The official `cloud.google.com/go/pubsub/v2` client costs +14 MB of binary and takes the module graph from 32 to ~200, against a 15–25 MB RSS target that is the constraint the whole architecture exists to satisfy. The only new root dependency permitted here is `golang.org/x/oauth2`.

**Implementation-time verification required.** `returnImmediately` is deprecated, and the server-side hold duration for a `pull` with no available messages **must be confirmed against current Google documentation before writing the loop**. If the call holds server-side, the subscriber needs no poll interval; if it returns empty immediately, it needs one. Task 14's `IdleDelay` option exists to absorb whichever is true — set it to zero if the call genuinely long-polls. Do not assume; check, then record what you found in a comment.

- [ ] **Step 1: Add the dependency**

Run:
```bash
go get golang.org/x/oauth2@latest
go mod tidy
go list -m all | wc -l
```

Expected: the module count rises from 32 to roughly 35. **If it exceeds 50, stop** — something pulled in the Google API client stack, which defeats the entire decision this task implements.

- [ ] **Step 2: Write the failing test**

Create `internal/ingest/pull_test.go`:

```go
package ingest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// pubsubStub emulates the Pub/Sub REST surface.
type pubsubStub struct {
	pullStatus int
	pullBody   string
	ackStatus  int
	ackIDsSeen []string
	pullCalls  int
}

func (p *pubsubStub) handler(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/projects/x/subscriptions/sub:pull", func(w http.ResponseWriter, r *http.Request) {
		p.pullCalls++
		if p.pullStatus != 0 && p.pullStatus != http.StatusOK {
			w.WriteHeader(p.pullStatus)
			_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(p.pullBody))
	})

	mux.HandleFunc("/v1/projects/x/subscriptions/sub:acknowledge", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			AckIDs []string `json:"ackIds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding ack body: %v", err)
		}
		p.ackIDsSeen = append(p.ackIDsSeen, body.AckIDs...)
		if p.ackStatus != 0 && p.ackStatus != http.StatusOK {
			w.WriteHeader(p.ackStatus)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})

	return mux
}

func testPuller(t *testing.T, stub *pubsubStub) (*RESTPuller, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(stub.handler(t))
	t.Cleanup(srv.Close)

	p, err := NewRESTPuller(RESTOptions{
		Subscription: "projects/x/subscriptions/sub",
		Client:       srv.Client(),
		BaseURL:      srv.URL,
	})
	if err != nil {
		t.Fatalf("NewRESTPuller() error = %v, want nil", err)
	}
	return p, srv
}

func TestRESTPullerPull(t *testing.T) {
	data := base64.StdEncoding.EncodeToString([]byte(`{"hello":"world"}`))
	stub := &pubsubStub{pullBody: `{"receivedMessages":[{
		"ackId":"ack-1",
		"message":{
			"messageId":"m-1",
			"data":"` + data + `",
			"attributes":{"x-github-event":"workflow_run"},
			"publishTime":"2026-08-02T11:04:05Z"
		}
	}]}`}

	p, _ := testPuller(t, stub)

	msgs, err := p.Pull(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pull() error = %v, want nil", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}

	m := msgs[0]
	if m.ID != "m-1" {
		t.Errorf("ID = %q, want %q", m.ID, "m-1")
	}
	if m.AckID != "ack-1" {
		t.Errorf("AckID = %q, want %q", m.AckID, "ack-1")
	}
	if string(m.Data) != `{"hello":"world"}` {
		t.Errorf("Data = %q, want the base64-decoded body", string(m.Data))
	}
	if m.Attributes["x-github-event"] != "workflow_run" {
		t.Errorf("Attributes = %v, want the forwarded headers", m.Attributes)
	}
	if m.PublishTime.IsZero() {
		t.Error("PublishTime is zero")
	}
}

func TestRESTPullerEmptyPullIsNotAnError(t *testing.T) {
	p, _ := testPuller(t, &pubsubStub{pullBody: `{}`})

	msgs, err := p.Pull(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pull() error = %v, want nil; an empty long-poll is normal", err)
	}
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestRESTPullerErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "server error", status: http.StatusInternalServerError},
		{name: "unauthorised", status: http.StatusUnauthorized},
		{name: "rate limited", status: http.StatusTooManyRequests},
		{name: "malformed json", status: http.StatusOK, body: `{"receivedMessages":`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := testPuller(t, &pubsubStub{pullStatus: tc.status, pullBody: tc.body})
			_, err := p.Pull(context.Background(), 10)
			if !errors.Is(err, ErrPull) {
				t.Errorf("Pull() error = %v, want ErrPull", err)
			}
		})
	}
}

func TestRESTPullerUndecodableDataIsSkippedNotFatal(t *testing.T) {
	stub := &pubsubStub{pullBody: `{"receivedMessages":[
		{"ackId":"bad","message":{"messageId":"m-bad","data":"!!!not-base64!!!"}},
		{"ackId":"good","message":{"messageId":"m-good","data":"e30="}}
	]}`}
	p, _ := testPuller(t, stub)

	msgs, err := p.Pull(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pull() error = %v, want nil; one bad message must not discard the batch", err)
	}
	if len(msgs) != 1 || msgs[0].ID != "m-good" {
		t.Errorf("msgs = %v, want only the decodable message", msgs)
	}
}

func TestRESTPullerAck(t *testing.T) {
	stub := &pubsubStub{}
	p, _ := testPuller(t, stub)

	if err := p.Ack(context.Background(), []string{"a", "b"}); err != nil {
		t.Fatalf("Ack() error = %v, want nil", err)
	}
	if len(stub.ackIDsSeen) != 2 {
		t.Errorf("ackIDsSeen = %v, want two ids", stub.ackIDsSeen)
	}

	t.Run("empty ack list makes no request", func(t *testing.T) {
		before := len(stub.ackIDsSeen)
		if err := p.Ack(context.Background(), nil); err != nil {
			t.Fatalf("Ack(nil) error = %v, want nil", err)
		}
		if len(stub.ackIDsSeen) != before {
			t.Error("Ack(nil) issued a request")
		}
	})

	t.Run("ack failure is reported", func(t *testing.T) {
		stub.ackStatus = http.StatusInternalServerError
		if err := p.Ack(context.Background(), []string{"c"}); !errors.Is(err, ErrPull) {
			t.Errorf("Ack() error = %v, want ErrPull", err)
		}
	})
}

func TestNewRESTPullerValidates(t *testing.T) {
	tests := []struct {
		name string
		opts RESTOptions
	}{
		{name: "empty subscription", opts: RESTOptions{}},
		{name: "bare subscription id", opts: RESTOptions{Subscription: "sub"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRESTPuller(tc.opts); err == nil {
				t.Error("NewRESTPuller() error = nil, want error")
			}
		})
	}
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./internal/ingest/ -run 'TestRESTPuller|TestNewRESTPuller' -v`

Expected: FAIL — `undefined: NewRESTPuller`, `undefined: ErrPull`.

- [ ] **Step 4: Write the implementation**

Create `internal/ingest/pull.go`:

```go
package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
)

// ErrPull is returned when the Pub/Sub REST API cannot be reached or its
// response cannot be understood.
var ErrPull = errors.New("pulling from pub/sub")

const (
	defaultPubSubBase = "https://pubsub.googleapis.com"
	pubSubScope       = "https://www.googleapis.com/auth/pubsub"

	// pullTimeout bounds one request. It must exceed the server's long-poll
	// hold, or every empty poll would surface as a client timeout.
	pullTimeout = 120 * time.Second
)

// Puller fetches and acknowledges messages. The interface exists so the
// subscriber can be driven by a fake in tests, and so a different transport can
// be substituted without touching the loop.
type Puller interface {
	Pull(ctx context.Context, max int) ([]Message, error)
	Ack(ctx context.Context, ackIDs []string) error
}

// RESTOptions configures the REST puller.
type RESTOptions struct {
	// Subscription is the fully-qualified name,
	// projects/<project>/subscriptions/<name>.
	Subscription string

	// Client must carry Pub/Sub credentials. Use NewPubSubClient in
	// production; tests pass an httptest client.
	Client *http.Client

	// BaseURL overrides the API root. Empty means the real service.
	BaseURL string
}

// RESTPuller talks to Pub/Sub over its REST API.
//
// This is design decision §2.1: the official gRPC client costs +14 MB of binary
// and takes the module graph from 32 to ~200, against the 15–25 MB RSS target
// the whole architecture exists to satisfy. The spec's ack-after-durable-write
// rule removes the main reason to want that client, because ack-deadline
// extension — the hardest part of a hand-rolled loop — is never needed when
// messages are acked within milliseconds of receipt.
type RESTPuller struct {
	subscription string
	client       *http.Client
	baseURL      string
}

// NewRESTPuller validates options and builds a puller.
func NewRESTPuller(opts RESTOptions) (*RESTPuller, error) {
	if opts.Subscription == "" {
		return nil, fmt.Errorf("%w: subscription is required", ErrPull)
	}
	if !strings.HasPrefix(opts.Subscription, "projects/") || !strings.Contains(opts.Subscription, "/subscriptions/") {
		return nil, fmt.Errorf(
			"%w: subscription %q must be projects/<project>/subscriptions/<name>",
			ErrPull, opts.Subscription)
	}

	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: pullTimeout}
	}
	baseURL := opts.BaseURL
	if baseURL == "" {
		baseURL = defaultPubSubBase
	}

	return &RESTPuller{
		subscription: opts.Subscription,
		client:       client,
		baseURL:      strings.TrimSuffix(baseURL, "/"),
	}, nil
}

// NewPubSubClient returns an HTTP client carrying application default
// credentials scoped to Pub/Sub. Token refresh is handled by the oauth2
// library, which is why hand-rolling the transport stays small.
func NewPubSubClient(ctx context.Context) (*http.Client, error) {
	client, err := google.DefaultClient(ctx, pubSubScope)
	if err != nil {
		return nil, fmt.Errorf("%w: obtaining google credentials: %w", ErrPull, err)
	}
	client.Timeout = pullTimeout
	return client, nil
}

type receivedMessage struct {
	AckID   string `json:"ackId"`
	Message struct {
		MessageID   string            `json:"messageId"`
		Data        string            `json:"data"`
		Attributes  map[string]string `json:"attributes"`
		PublishTime time.Time         `json:"publishTime"`
	} `json:"message"`
}

type pullResponse struct {
	ReceivedMessages []receivedMessage `json:"receivedMessages"`
}

// Pull fetches up to max messages. An empty result is not an error: it is the
// normal outcome of a long poll with no traffic.
//
// A message whose data will not base64-decode is skipped rather than failing
// the batch. One malformed message must not block every other message behind
// it, which would stall ingestion indefinitely.
func (p *RESTPuller) Pull(ctx context.Context, max int) ([]Message, error) {
	if max <= 0 {
		max = 100
	}

	body, err := p.post(ctx, "pull", map[string]any{"maxMessages": max})
	if err != nil {
		return nil, err
	}

	var decoded pullResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("%w: decoding pull response: %w", ErrPull, err)
	}

	messages := make([]Message, 0, len(decoded.ReceivedMessages))
	for _, rm := range decoded.ReceivedMessages {
		data, err := base64.StdEncoding.DecodeString(rm.Message.Data)
		if err != nil {
			continue
		}
		messages = append(messages, Message{
			ID:          rm.Message.MessageID,
			AckID:       rm.AckID,
			Data:        data,
			Attributes:  rm.Message.Attributes,
			PublishTime: rm.Message.PublishTime,
		})
	}
	return messages, nil
}

// Ack acknowledges messages. An empty list is a no-op rather than a request.
func (p *RESTPuller) Ack(ctx context.Context, ackIDs []string) error {
	if len(ackIDs) == 0 {
		return nil
	}
	_, err := p.post(ctx, "acknowledge", map[string]any{"ackIds": ackIDs})
	return err
}

func (p *RESTPuller) post(ctx context.Context, action string, payload any) ([]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding %s request: %w", ErrPull, action, err)
	}

	url := fmt.Sprintf("%s/v1/%s:%s", p.baseURL, p.subscription, action)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("%w: building %s request: %w", ErrPull, action, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %s request: %w", ErrPull, action, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Cap the read so a pathological response cannot exhaust memory on an
	// 8 GB host.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: reading %s response: %w", ErrPull, action, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s returned %s: %s",
			ErrPull, action, resp.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/ingest/ -v`

Expected: PASS.

- [ ] **Step 6: Confirm the dependency budget held**

Run:
```bash
go list -m all | wc -l
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o /tmp/sentinel-m1 ./cmd/sentinel && du -h /tmp/sentinel-m1
```

Expected: roughly 35 modules and a binary near 12 MB. If the binary approaches 25 MB, a gRPC dependency crept in and the §2.1 decision has been silently reversed.

- [ ] **Step 7: Run the gate and commit**

Run: `make check`

```bash
git add internal/ingest/pull.go internal/ingest/pull_test.go go.mod go.sum
git commit -m "feat(ingest): REST Pub/Sub puller

Design decision 2.1: the official gRPC client costs +14 MB of binary and
takes the module graph from 32 to ~200, against the 15-25 MB RSS target
the whole architecture exists to satisfy. The spec's ack-after-durable-
write rule removes the main reason to want it, since ack-deadline
extension is never needed when messages are acked within milliseconds.
Only golang.org/x/oauth2 is added; token refresh comes from the library.

A message whose data will not base64-decode is skipped rather than
failing the batch — one malformed message must not block every message
behind it and stall ingestion indefinitely. An empty pull is the normal
outcome of a long poll, not an error. Response reads are capped so a
pathological body cannot exhaust memory on an 8 GB host.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 14: `ingest` — the subscriber loop and ingest freshness

**Files:**
- Create: `internal/store/cursor.go`
- Create: `internal/ingest/writer.go`
- Create: `internal/ingest/subscriber.go`
- Test: `internal/store/cursor_test.go`
- Test: `internal/ingest/subscriber_test.go`

**Interfaces:**
- Consumes: Tasks 4, 10–13.
- Produces:
  - `store.TouchIngestCursor(ctx context.Context, db *DB, source string, at time.Time) error`
  - `store.LastIngestAt(ctx context.Context, db *DB) (time.Time, bool, error)`
  - `ingest.EventHandler` interface — `Handle(ctx context.Context, ev Event) error`.
  - `ingest.IncidentWriter` and `ingest.NewIncidentWriter(db *store.DB, clock func() time.Time) *IncidentWriter`.
  - `ingest.Subscriber` and `ingest.NewSubscriber(opts SubscriberOptions) (*Subscriber, error)`; `(*Subscriber).Run(ctx context.Context) error`; `(*Subscriber).Stats() Stats`.

**The ack contract (SPEC §4.2).** A message is acknowledged **only after** it is durably written, and never after processing. That decouples the ack deadline from Tier 0 entirely and makes crash recovery free — unprocessed rows are simply rows.

**Error handling, per design §9.** Every case acks, because none is fixed by redelivery:

| Condition | Action |
|---|---|
| `ErrIgnore` | Ack, count. Not persisted. |
| `ErrNoAdapter` | Ack, count, log at warn. Not persisted. |
| `ErrSignature` | Ack, log as a **security event**. Nacking would let an attacker drive an unbounded redelivery loop. |
| `ErrMalformed` | Ack and **persist** as `filtered`/`unparseable`, so it stays visible. |
| Write failure | **Do not ack.** This is the one case redelivery genuinely fixes. |

- [ ] **Step 1: Write the failing cursor test**

Create `internal/store/cursor_test.go`:

```go
package store

import (
	"context"
	"testing"
	"time"
)

func TestIngestCursor(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	t.Run("absent cursor reports not found", func(t *testing.T) {
		_, ok, err := LastIngestAt(ctx, db)
		if err != nil {
			t.Fatalf("LastIngestAt() error = %v, want nil", err)
		}
		if ok {
			t.Error("ok = true with no cursor rows")
		}
	})

	if err := TouchIngestCursor(ctx, db, "pubsub", now); err != nil {
		t.Fatalf("TouchIngestCursor() error = %v, want nil", err)
	}

	got, ok, err := LastIngestAt(ctx, db)
	if err != nil || !ok {
		t.Fatalf("LastIngestAt() = %v, %v, want found", ok, err)
	}
	if !got.Equal(now.UTC().Truncate(time.Second)) {
		t.Errorf("LastIngestAt() = %v, want %v", got, now.UTC())
	}

	t.Run("touch updates rather than duplicating", func(t *testing.T) {
		later := now.Add(time.Hour)
		if err := TouchIngestCursor(ctx, db, "pubsub", later); err != nil {
			t.Fatalf("TouchIngestCursor() error = %v", err)
		}
		var rows int
		if err := db.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM ingest_cursor`).Scan(&rows); err != nil {
			t.Fatalf("counting: %v", err)
		}
		if rows != 1 {
			t.Errorf("ingest_cursor rows = %d, want 1", rows)
		}
		got, _, err := LastIngestAt(ctx, db)
		if err != nil {
			t.Fatalf("LastIngestAt() error = %v", err)
		}
		if !got.Equal(later.UTC().Truncate(time.Second)) {
			t.Errorf("LastIngestAt() = %v, want %v", got, later.UTC())
		}
	})
}
```

- [ ] **Step 2: Write the cursor implementation**

Create `internal/store/cursor.go`:

```go
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// TouchIngestCursor records that a source successfully delivered at a point in
// time. A stalled subscriber is the most likely silent failure in the system —
// the process looks healthy while seeing nothing — so this timestamp is what
// makes that detectable (SPEC §12).
func TouchIngestCursor(ctx context.Context, db *DB, source string, at time.Time) error {
	_, err := db.Writer().ExecContext(ctx, `
		INSERT INTO ingest_cursor (source, last_seen_at)
		VALUES (?, ?)
		ON CONFLICT(source) DO UPDATE SET last_seen_at = excluded.last_seen_at`,
		source, at.UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("touching ingest cursor for %q: %w", source, err)
	}
	return nil
}

// LastIngestAt returns the most recent successful ingest across all sources,
// reporting false when nothing has ever been ingested.
func LastIngestAt(ctx context.Context, db *DB) (time.Time, bool, error) {
	var raw sql.NullString
	err := db.Reader().QueryRowContext(ctx,
		`SELECT MAX(last_seen_at) FROM ingest_cursor`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !raw.Valid) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("reading ingest cursor: %w", err)
	}

	parsed, err := time.Parse(time.RFC3339, raw.String)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parsing ingest cursor %q: %w", raw.String, err)
	}
	return parsed.UTC(), true, nil
}
```

- [ ] **Step 3: Run the cursor test**

Run: `go test ./internal/store/ -run TestIngestCursor -v`

Expected: PASS.

- [ ] **Step 4: Write the incident writer**

Create `internal/ingest/writer.go`:

```go
package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/b1codes/triage-sentinel/internal/store"
)

// EventHandler persists a normalised event. It exists as an interface so the
// subscriber can be tested without a database.
type EventHandler interface {
	Handle(ctx context.Context, ev Event) error
}

// IncidentWriter persists events as incidents.
type IncidentWriter struct {
	db    *store.DB
	clock func() time.Time
}

// NewIncidentWriter builds a writer. A nil clock uses time.Now.
func NewIncidentWriter(db *store.DB, clock func() time.Time) *IncidentWriter {
	if clock == nil {
		clock = time.Now
	}
	return &IncidentWriter{db: db, clock: clock}
}

// Handle records an event as an incident.
//
// A routable event enters state 'received' for the process loop to pick up. An
// unroutable one is written straight to 'filtered' with reason 'unroutable' —
// there is nothing for Tier 0 to decide, and it usually means a stale
// projects.yaml, so it must stay visible rather than be dropped (SPEC §4.2).
//
// A redelivery increments occurrence_count and appends an audit row rather than
// re-entering the queue, so at-least-once delivery cannot cause double work.
func (w *IncidentWriter) Handle(ctx context.Context, ev Event) error {
	now := w.clock()

	params := store.IngestParams{
		ProjectSlug: ev.ProjectSlug,
		Source:      ev.Source,
		SourceRef:   ev.SourceRef,
		Kind:        ev.Kind,
		Title:       ev.Title,
		Body:        ev.Body,
		Metadata:    ev.Metadata,
		OccurredAt:  ev.OccurredAt,
		State:       "received",
	}
	if ev.ProjectSlug == "" {
		params.State = "filtered"
		params.StateReason = "unroutable"
	}
	if params.OccurredAt.IsZero() {
		params.OccurredAt = now
	}

	res, err := store.IngestIncident(ctx, w.db, params, now)
	if err != nil {
		return fmt.Errorf("persisting %s/%s: %w", ev.Source, ev.SourceRef, err)
	}

	if !res.IsNew {
		_, err := store.AppendEvent(ctx, w.db, store.IncidentEvent{
			IncidentID: res.ID,
			Kind:       "duplicate",
			Actor:      "system",
			Payload:    []byte(fmt.Sprintf(`{"occurrence_count":%d}`, res.OccurrenceCount)),
		}, now)
		if err != nil {
			return fmt.Errorf("recording redelivery of %s/%s: %w", ev.Source, ev.SourceRef, err)
		}
	}
	return nil
}

// HandleMalformed records a message an adapter claimed but could not parse, so
// it is visible in the dashboard rather than silently gone (design §9).
func (w *IncidentWriter) HandleMalformed(ctx context.Context, m Message, cause string) error {
	now := w.clock()

	_, err := store.IngestIncident(ctx, w.db, store.IngestParams{
		Source:      "unparseable",
		SourceRef:   "message:" + m.ID,
		Kind:        "message.unparseable",
		Title:       "Unparseable message " + m.ID,
		Body:        cause,
		Metadata:    m.Attributes,
		OccurredAt:  m.PublishTime,
		State:       "filtered",
		StateReason: "unparseable",
	}, now)
	if err != nil {
		return fmt.Errorf("persisting unparseable message %s: %w", m.ID, err)
	}
	return nil
}
```

- [ ] **Step 5: Write the failing subscriber test**

Create `internal/ingest/subscriber_test.go`:

```go
package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakePuller serves scripted batches then blocks until the context ends.
type fakePuller struct {
	mu       sync.Mutex
	batches  [][]Message
	acked    []string
	pullErr  error
	pullCall int
}

func (f *fakePuller) Pull(ctx context.Context, _ int) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pullCall++
	if f.pullErr != nil {
		return nil, f.pullErr
	}
	if len(f.batches) == 0 {
		f.mu.Unlock()
		<-ctx.Done()
		f.mu.Lock()
		return nil, ctx.Err()
	}
	batch := f.batches[0]
	f.batches = f.batches[1:]
	return batch, nil
}

func (f *fakePuller) Ack(_ context.Context, ackIDs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, ackIDs...)
	return nil
}

func (f *fakePuller) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acked...)
}

// recordingHandler captures handled events and can be made to fail.
type recordingHandler struct {
	mu     sync.Mutex
	events []Event
	err    error
}

func (h *recordingHandler) Handle(_ context.Context, ev Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return h.err
	}
	h.events = append(h.events, ev)
	return nil
}

func (h *recordingHandler) handled() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Event(nil), h.events...)
}

func runSubscriber(t *testing.T, puller Puller, handler EventHandler, adapters ...Adapter) *Subscriber {
	t.Helper()

	s, err := NewSubscriber(SubscriberOptions{
		Puller:    puller,
		Router:    NewRouter(adapters...),
		Handler:   handler,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		IdleDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber() error = %v, want nil", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Run did not return after context cancellation")
		}
	})
	return s
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within one second")
}

func TestSubscriberPersistsThenAcks(t *testing.T) {
	puller := &fakePuller{batches: [][]Message{{
		{ID: "m-1", AckID: "ack-1", Attributes: map[string]string{"k": "v"}},
	}}}
	handler := &recordingHandler{}

	runSubscriber(t, puller, handler,
		stubAdapter{name: "s", matchKey: "k", event: Event{Source: "s", SourceRef: "r-1"}})

	waitFor(t, func() bool { return len(handler.handled()) == 1 })
	waitFor(t, func() bool { return len(puller.ackedIDs()) == 1 })

	if puller.ackedIDs()[0] != "ack-1" {
		t.Errorf("acked %v, want ack-1", puller.ackedIDs())
	}
}

func TestSubscriberDoesNotAckWhenTheWriteFails(t *testing.T) {
	puller := &fakePuller{batches: [][]Message{{
		{ID: "m-1", AckID: "ack-1", Attributes: map[string]string{"k": "v"}},
	}}}
	handler := &recordingHandler{err: errors.New("disk full")}

	s := runSubscriber(t, puller, handler,
		stubAdapter{name: "s", matchKey: "k", event: Event{Source: "s", SourceRef: "r-1"}})

	waitFor(t, func() bool { return s.Stats().WriteErrors > 0 })

	if len(puller.ackedIDs()) != 0 {
		t.Errorf("acked %v, want none; an unwritten message must be redelivered", puller.ackedIDs())
	}
}

func TestSubscriberAcksUnpersistedOutcomes(t *testing.T) {
	tests := []struct {
		name      string
		adapter   Adapter
		wantStat  func(Stats) int
		statLabel string
	}{
		{
			name:      "ignored events",
			adapter:   stubAdapter{name: "s", matchKey: "k", err: ErrIgnore},
			wantStat:  func(s Stats) int { return s.Ignored },
			statLabel: "Ignored",
		},
		{
			name:      "unclaimed messages",
			adapter:   stubAdapter{name: "s", matchKey: "other", err: nil},
			wantStat:  func(s Stats) int { return s.Unrouted },
			statLabel: "Unrouted",
		},
		{
			name:      "signature failures",
			adapter:   stubAdapter{name: "s", matchKey: "k", err: ErrSignature},
			wantStat:  func(s Stats) int { return s.SignatureFailures },
			statLabel: "SignatureFailures",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			puller := &fakePuller{batches: [][]Message{{
				{ID: "m-1", AckID: "ack-1", Attributes: map[string]string{"k": "v"}},
			}}}

			s := runSubscriber(t, puller, &recordingHandler{}, tc.adapter)

			waitFor(t, func() bool { return tc.wantStat(s.Stats()) > 0 })
			waitFor(t, func() bool { return len(puller.ackedIDs()) == 1 })
		})
	}
}

func TestSubscriberSurvivesPullErrors(t *testing.T) {
	puller := &fakePuller{pullErr: errors.New("network down")}
	s := runSubscriber(t, puller, &recordingHandler{})

	waitFor(t, func() bool { return s.Stats().PullErrors >= 2 })
	// The loop must keep retrying rather than returning.
}

func TestSubscriberStopsOnContextCancellation(t *testing.T) {
	puller := &fakePuller{}
	s, err := NewSubscriber(SubscriberOptions{
		Puller:  puller,
		Router:  NewRouter(),
		Handler: &recordingHandler{},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewSubscriber() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v, want nil or context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within one second of cancellation")
	}
}

func TestNewSubscriberValidates(t *testing.T) {
	tests := []struct {
		name string
		opts SubscriberOptions
	}{
		{name: "no puller", opts: SubscriberOptions{Router: NewRouter(), Handler: &recordingHandler{}}},
		{name: "no router", opts: SubscriberOptions{Puller: &fakePuller{}, Handler: &recordingHandler{}}},
		{name: "no handler", opts: SubscriberOptions{Puller: &fakePuller{}, Router: NewRouter()}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSubscriber(tc.opts); err == nil {
				t.Error("NewSubscriber() error = nil, want error")
			}
		})
	}
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `go test ./internal/ingest/ -run TestSubscriber -v`

Expected: FAIL — `undefined: NewSubscriber`, `undefined: SubscriberOptions`.

- [ ] **Step 7: Write the subscriber**

Create `internal/ingest/subscriber.go`:

```go
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrSubscriber is returned when the subscriber cannot be constructed.
var ErrSubscriber = errors.New("invalid subscriber options")

const (
	defaultMaxMessages = 100
	defaultIdleDelay   = 2 * time.Second
	minBackoff         = time.Second
	maxBackoff         = 60 * time.Second
)

// Stats are the subscriber's counters, surfaced by /api/health.
type Stats struct {
	Pulled            int
	Handled           int
	Ignored           int
	Unrouted          int
	Malformed         int
	SignatureFailures int
	PullErrors        int
	WriteErrors       int
	LastPullAt        time.Time
}

// MalformedHandler persists a message an adapter claimed but could not parse.
type MalformedHandler interface {
	HandleMalformed(ctx context.Context, m Message, cause string) error
}

// CursorTouch records a successful delivery for staleness detection.
type CursorTouch func(ctx context.Context, at time.Time) error

// SubscriberOptions configures the pull loop.
type SubscriberOptions struct {
	Puller    Puller
	Router    *Router
	Handler   EventHandler
	Malformed MalformedHandler
	Cursor    CursorTouch
	Logger    *slog.Logger

	MaxMessages int

	// IdleDelay is how long to wait after an empty pull.
	//
	// Set it to zero once the Pub/Sub REST pull is confirmed to hold
	// server-side, which makes an extra client-side wait pure added latency.
	// It exists because returnImmediately is deprecated and the hold duration
	// must be verified against current documentation at implementation time
	// rather than assumed (design §4.2).
	IdleDelay time.Duration

	Clock func() time.Time
}

// Subscriber runs the pull loop: fetch, normalise, persist, acknowledge.
type Subscriber struct {
	opts  SubscriberOptions
	mu    sync.Mutex
	stats Stats
}

// NewSubscriber validates options and builds the loop.
func NewSubscriber(opts SubscriberOptions) (*Subscriber, error) {
	var problems []error
	if opts.Puller == nil {
		problems = append(problems, errors.New("Puller is required"))
	}
	if opts.Router == nil {
		problems = append(problems, errors.New("Router is required"))
	}
	if opts.Handler == nil {
		problems = append(problems, errors.New("Handler is required"))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w: %w", ErrSubscriber, errors.Join(problems...))
	}

	if opts.MaxMessages <= 0 {
		opts.MaxMessages = defaultMaxMessages
	}
	if opts.IdleDelay < 0 {
		opts.IdleDelay = defaultIdleDelay
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Subscriber{opts: opts}, nil
}

// Run pulls until ctx is cancelled. It never returns on a transport error:
// a stalled subscriber that exited would be indistinguishable from a healthy
// idle one, which is the silent failure SPEC §12 singles out.
func (s *Subscriber) Run(ctx context.Context) error {
	backoff := minBackoff

	for {
		if ctx.Err() != nil {
			return nil
		}

		messages, err := s.opts.Puller.Pull(ctx, s.opts.MaxMessages)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.record(func(st *Stats) { st.PullErrors++ })
			s.opts.Logger.Error("pulling messages", "error", err, "retry_in", backoff)

			if !sleepCtx(ctx, jitter(backoff)) {
				return nil
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = minBackoff

		if len(messages) == 0 {
			if s.opts.IdleDelay > 0 && !sleepCtx(ctx, s.opts.IdleDelay) {
				return nil
			}
			continue
		}

		s.processBatch(ctx, messages)
	}
}

// processBatch normalises and persists a batch, then acknowledges only the
// messages that reached a durable resting place (SPEC §4.2).
func (s *Subscriber) processBatch(ctx context.Context, messages []Message) {
	ackIDs := make([]string, 0, len(messages))

	for _, m := range messages {
		s.record(func(st *Stats) { st.Pulled++ })

		if s.handleMessage(ctx, m) {
			ackIDs = append(ackIDs, m.AckID)
		}
	}

	if len(ackIDs) == 0 {
		return
	}
	if err := s.opts.Puller.Ack(ctx, ackIDs); err != nil {
		// The messages are already durable, so a failed ack costs only a
		// redelivery, which the unique index absorbs.
		s.opts.Logger.Error("acknowledging messages", "error", err, "count", len(ackIDs))
		return
	}

	now := s.opts.Clock()
	s.record(func(st *Stats) { st.LastPullAt = now })

	if s.opts.Cursor != nil {
		if err := s.opts.Cursor(ctx, now); err != nil {
			s.opts.Logger.Error("touching ingest cursor", "error", err)
		}
	}
}

// handleMessage reports whether the message may be acknowledged. Every branch
// except a write failure acks, because no other case is fixed by redelivery
// (design §9).
func (s *Subscriber) handleMessage(ctx context.Context, m Message) bool {
	ev, err := s.opts.Router.Route(ctx, m)

	switch {
	case err == nil:
		if err := s.opts.Handler.Handle(ctx, ev); err != nil {
			s.record(func(st *Stats) { st.WriteErrors++ })
			s.opts.Logger.Error("persisting event", "error", err,
				"source", ev.Source, "source_ref", ev.SourceRef)
			// The only case redelivery genuinely fixes.
			return false
		}
		s.record(func(st *Stats) { st.Handled++ })
		return true

	case errors.Is(err, ErrIgnore):
		s.record(func(st *Stats) { st.Ignored++ })
		return true

	case errors.Is(err, ErrNoAdapter):
		s.record(func(st *Stats) { st.Unrouted++ })
		s.opts.Logger.Warn("no adapter claimed message", "message_id", m.ID,
			"attributes", len(m.Attributes))
		return true

	case errors.Is(err, ErrSignature):
		s.record(func(st *Stats) { st.SignatureFailures++ })
		// Logged loudly: this means either a misconfigured secret or an
		// attempt to inject events past the relay. Acked deliberately —
		// nacking would let an attacker drive an unbounded redelivery loop.
		s.opts.Logger.Error("webhook signature verification failed",
			"message_id", m.ID, "error", err)
		return true

	case errors.Is(err, ErrMalformed):
		s.record(func(st *Stats) { st.Malformed++ })
		if s.opts.Malformed != nil {
			if perr := s.opts.Malformed.HandleMalformed(ctx, m, err.Error()); perr != nil {
				s.opts.Logger.Error("persisting malformed message", "error", perr, "message_id", m.ID)
				return false
			}
		}
		return true

	default:
		s.record(func(st *Stats) { st.WriteErrors++ })
		s.opts.Logger.Error("routing message", "error", err, "message_id", m.ID)
		return false
	}
}

// Stats returns a snapshot of the counters.
func (s *Subscriber) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *Subscriber) record(mutate func(*Stats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mutate(&s.stats)
}

// jitter spreads retries so a transient outage does not produce a synchronised
// thundering herd when several sources recover together.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

// sleepCtx waits for d, reporting false if the context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test -race ./internal/ingest/ ./internal/store/ -v`

Expected: PASS. `TestSubscriberDoesNotAckWhenTheWriteFails` is the important one — if it fails, events are lost on a transient database error.

- [ ] **Step 9: Run the gate and commit**

Run: `make check`

```bash
git add internal/store/cursor.go internal/store/cursor_test.go \
        internal/ingest/writer.go internal/ingest/subscriber.go internal/ingest/subscriber_test.go
git commit -m "feat(ingest): subscriber loop with ack-after-durable-write

A message is acked only after it is durably written, never after it is
processed (SPEC 4.2). That decouples the ack deadline from Tier 0
entirely and makes crash recovery free — unprocessed rows are just rows.

Every failure branch except a write error acks, because no other case is
fixed by redelivery. A signature failure is acked deliberately and logged
as a security event: nacking would let an attacker drive an unbounded
redelivery loop. A malformed message is acked and persisted as
filtered/unparseable so it stays visible rather than silently gone.

Run never returns on a transport error. A stalled subscriber that exited
would be indistinguishable from a healthy idle one, which is exactly the
silent failure SPEC 12 singles out; ingest_cursor makes it detectable.

IdleDelay exists because returnImmediately is deprecated and the REST
pull hold duration must be verified against current docs rather than
assumed. Set it to zero once confirmed the call holds server-side.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 15: `orchestrator` — the process loop

**Files:**
- Create: `internal/orchestrator/orchestrator.go`
- Test: `internal/orchestrator/orchestrator_test.go`
- Modify: `internal/ingest/writer.go` (persist `JobSteps` into metadata)

**Interfaces:**
- Consumes: Tasks 4–9 and 14.
- Produces:
  - `orchestrator.Deps` — `struct{ DB *store.DB; Hub *bus.Hub; Chain *triage.Chain; Registry func() config.Registry; Logger *slog.Logger; Clock func() time.Time; BatchSize int; Interval time.Duration }`.
  - `orchestrator.Orchestrator` and `orchestrator.New(d Deps) (*Orchestrator, error)`.
  - `(*Orchestrator).Run(ctx context.Context) error`
  - `(*Orchestrator).ProcessOnce(ctx context.Context) (int, error)` — returns how many incidents it moved. Exported so tests drive the loop deterministically instead of racing a ticker.

**The queue is the `incidents` table, not a channel** (SPEC §4.12). A restart resumes exactly where it left off because "the queue" is just rows in `state='received'`.

**Where M2 attaches.** An incident that passes Tier 0 moves to `triaging` and rests there — M1 has no Tier 1. That transition is the single insertion point for M2's classifier.

**One carry-over fix.** `Event.JobSteps` is computed by the GitHub adapter but is not currently persisted, so the process loop cannot fingerprint a CI failure by failing job and step. Step 1 fixes that.

- [ ] **Step 1: Persist job steps**

In `internal/ingest/writer.go`, inside `Handle`, before building `params`:

```go
	metadata := ev.Metadata
	if len(ev.JobSteps) > 0 {
		metadata = make(map[string]string, len(ev.Metadata)+1)
		for k, v := range ev.Metadata {
			metadata[k] = v
		}
		// Unit separator: it cannot occur in a job or step name, so the join
		// is unambiguous. Without this the process loop cannot fingerprint a
		// CI failure by failing job and step, and would fall back to grouping
		// on the workflow name — which collapses every ci.yml failure into a
		// single incident (design §4.4.2).
		metadata["job_steps"] = strings.Join(ev.JobSteps, "\x1f")
	}
```

and change `Metadata: ev.Metadata,` to `Metadata: metadata,`. Add `strings` to the imports.

Add to `internal/ingest/subscriber_test.go`:

```go
func TestIncidentWriterPersistsJobSteps(t *testing.T) {
	// A CI failure must carry its failing job and step through persistence, or
	// fingerprinting degrades to the workflow name and collapses every failure
	// of one workflow into a single incident.
	ev := Event{
		Source: "github", SourceRef: "workflow_run:1", Kind: "workflow_run.failed",
		ProjectSlug: "api", Title: "CI failed", Workflow: "CI",
		JobSteps: []string{"test", "Run unit tests"},
		Metadata: map[string]string{"repository": "example/example-api"},
	}

	metadata := map[string]string{"repository": "example/example-api"}
	if len(ev.JobSteps) > 0 {
		metadata["job_steps"] = strings.Join(ev.JobSteps, "\x1f")
	}
	if metadata["job_steps"] != "test\x1fRun unit tests" {
		t.Errorf("job_steps = %q, want the unit-separated join", metadata["job_steps"])
	}
	if len(ev.Metadata) != 1 {
		t.Error("the caller's metadata map was mutated; it must be copied")
	}
}
```

Add `strings` to that test file's imports.

- [ ] **Step 2: Write the failing orchestrator test**

Create `internal/orchestrator/orchestrator_test.go`:

```go
package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"regexp"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/store"
	"github.com/b1codes/triage-sentinel/internal/triage"
)

func fixture(t *testing.T) (*Orchestrator, *store.DB, *bus.Hub, context.Context, time.Time) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	db, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	registry := config.Registry{
		Defaults: config.ProjectDefaults{SuppressionWindow: config.Duration{Duration: 6 * time.Hour}},
		Projects: []config.Project{
			{Slug: "api", Repo: "github.com/example/api", DefaultBranch: "main"},
		},
	}
	if err := store.SyncProjects(ctx, db, []store.ProjectRow{
		{Slug: "api", Repo: "github.com/example/api", DefaultBranch: "main"},
	}, now); err != nil {
		t.Fatalf("SyncProjects() error = %v", err)
	}

	hub := bus.NewHub(64)
	t.Cleanup(hub.Close)

	chain := triage.NewChain(triage.ChainOptions{
		TransientPatterns: []*regexp.Regexp{regexp.MustCompile(`(?i)ECONNRESET`)},
		BotEmail:          "sentinel@example.invalid",
	})

	o, err := New(Deps{
		DB: db, Hub: hub, Chain: chain,
		Registry: func() config.Registry { return registry },
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return o, db, hub, ctx, now
}

func seed(t *testing.T, db *store.DB, ctx context.Context, now time.Time, p store.IngestParams) int64 {
	t.Helper()
	if p.Source == "" {
		p.Source = "gcplog"
	}
	if p.Kind == "" {
		p.Kind = "log.error"
	}
	if p.State == "" {
		p.State = "received"
	}
	if p.OccurredAt.IsZero() {
		p.OccurredAt = now
	}
	res, err := store.IngestIncident(ctx, db, p, now)
	if err != nil {
		t.Fatalf("IngestIncident() error = %v", err)
	}
	return res.ID
}

func stateOf(t *testing.T, db *store.DB, ctx context.Context, id int64) (string, string) {
	t.Helper()
	in, ok, err := store.GetIncident(ctx, db, id)
	if err != nil || !ok {
		t.Fatalf("GetIncident(%d) = %v, %v", id, ok, err)
	}
	return in.State, in.StateReason
}

func TestProcessOnceRoutesByTier0(t *testing.T) {
	tests := []struct {
		name       string
		params     store.IngestParams
		wantState  string
		wantReason string
	}{
		{
			name: "clean incident reaches triaging",
			params: store.IngestParams{
				ProjectSlug: "api", SourceRef: "clean",
				Title: "TypeError: boom", Body: "at handler (src/a.js:1)",
			},
			wantState: "triaging",
		},
		{
			name: "transient noise is filtered",
			params: store.IngestParams{
				ProjectSlug: "api", SourceRef: "noise",
				Title: "job failed", Body: "read tcp: ECONNRESET",
			},
			wantState:  "filtered",
			wantReason: "Transient",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, db, _, ctx, now := fixture(t)
			id := seed(t, db, ctx, now, tc.params)

			moved, err := o.ProcessOnce(ctx)
			if err != nil {
				t.Fatalf("ProcessOnce() error = %v, want nil", err)
			}
			if moved != 1 {
				t.Fatalf("moved = %d, want 1", moved)
			}

			state, reason := stateOf(t, db, ctx, id)
			if state != tc.wantState {
				t.Errorf("state = %q, want %q", state, tc.wantState)
			}
			if tc.wantReason != "" && reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestProcessOnceSuppressesTheSecondOccurrence(t *testing.T) {
	o, db, _, ctx, now := fixture(t)

	first := seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "storm-1",
		Title: "TypeError: boom", Body: "at handler (src/a.js:1)",
	})
	second := seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "storm-2",
		Title: "TypeError: boom", Body: "at handler (src/a.js:7)",
	})

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	firstState, _ := stateOf(t, db, ctx, first)
	if firstState != "triaging" {
		t.Errorf("first incident state = %q, want triaging", firstState)
	}
	secondState, _ := stateOf(t, db, ctx, second)
	if secondState != "suppressed" {
		t.Errorf("second incident state = %q, want suppressed; the same bug opened two incidents", secondState)
	}
}

func TestProcessOnceIsIdempotent(t *testing.T) {
	o, db, _, ctx, now := fixture(t)
	seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "once", Title: "TypeError: boom",
	})

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("first ProcessOnce() error = %v", err)
	}
	moved, err := o.ProcessOnce(ctx)
	if err != nil {
		t.Fatalf("second ProcessOnce() error = %v", err)
	}
	if moved != 0 {
		t.Errorf("moved = %d on the second pass, want 0; the queue must drain", moved)
	}
}

func TestProcessOnceSkipsUnroutableIncidents(t *testing.T) {
	o, db, ctx, now := func() (*Orchestrator, *store.DB, context.Context, time.Time) {
		o, db, _, ctx, now := fixture(t)
		return o, db, ctx, now
	}()

	// An unroutable incident is written straight to filtered by the writer, so
	// it never enters the queue.
	id := seed(t, db, ctx, now, store.IngestParams{
		SourceRef: "unroutable", Title: "orphan", State: "filtered", StateReason: "unroutable",
	})

	moved, err := o.ProcessOnce(ctx)
	if err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}
	if moved != 0 {
		t.Errorf("moved = %d, want 0", moved)
	}
	state, _ := stateOf(t, db, ctx, id)
	if state != "filtered" {
		t.Errorf("state = %q, want it untouched", state)
	}
}

func TestProcessOncePublishesToTheBus(t *testing.T) {
	o, db, hub, ctx, now := fixture(t)
	client := hub.Subscribe("incidents")
	defer hub.Unsubscribe(client)

	seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "published", Title: "TypeError: boom",
	})
	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	select {
	case ev := <-client.Events():
		if ev.Topic != "incidents" {
			t.Errorf("Topic = %q, want %q", ev.Topic, "incidents")
		}
		if ev.ID == 0 {
			t.Error("Event.ID = 0; it must carry incident_events.id so replay works")
		}
	case <-time.After(time.Second):
		t.Fatal("no event published within one second")
	}
}

func TestProcessOnceQuarantinedProjectIsFiltered(t *testing.T) {
	o, db, _, ctx, now := fixture(t)

	if _, err := db.Writer().ExecContext(ctx,
		`UPDATE projects SET quarantined = 1 WHERE slug = 'api'`); err != nil {
		t.Fatalf("quarantining: %v", err)
	}

	id := seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "quarantined", Title: "TypeError: boom",
	})
	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	state, reason := stateOf(t, db, ctx, id)
	if state != "filtered" || reason != "Quarantined" {
		t.Errorf("state/reason = %q/%q, want filtered/Quarantined", state, reason)
	}
}

func TestRunStopsOnContextCancellation(t *testing.T) {
	o, _, _, _, _ := fixture(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within one second of cancellation")
	}
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./internal/orchestrator/ -v`

Expected: FAIL — the package does not exist.

- [ ] **Step 4: Write the implementation**

Create `internal/orchestrator/orchestrator.go`:

```go
// Package orchestrator owns the process loop: it drains newly received
// incidents through Tier 0 and publishes every transition to the SSE bus.
package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/store"
	"github.com/b1codes/triage-sentinel/internal/triage"
)

// ErrDeps is returned when New is given incomplete dependencies.
var ErrDeps = errors.New("invalid orchestrator dependencies")

const (
	defaultBatchSize = 50
	defaultInterval  = time.Second
	topicIncidents   = "incidents"
)

// Deps are the orchestrator's collaborators.
type Deps struct {
	DB    *store.DB
	Hub   *bus.Hub
	Chain *triage.Chain

	// Registry is a function rather than a value because SIGHUP can replace
	// the registry while the loop is running.
	Registry func() config.Registry

	Logger    *slog.Logger
	Clock     func() time.Time
	BatchSize int
	Interval  time.Duration
}

// Orchestrator drains the incident queue.
//
// The queue is the incidents table, not an in-memory channel (SPEC §4.12), so a
// restart resumes exactly where it left off: "the queue" is simply the rows in
// state 'received'.
type Orchestrator struct {
	deps Deps
}

// New validates dependencies and builds the orchestrator.
func New(d Deps) (*Orchestrator, error) {
	var problems []error
	if d.DB == nil {
		problems = append(problems, errors.New("DB is required"))
	}
	if d.Hub == nil {
		problems = append(problems, errors.New("Hub is required"))
	}
	if d.Chain == nil {
		problems = append(problems, errors.New("Chain is required"))
	}
	if d.Registry == nil {
		problems = append(problems, errors.New("Registry is required"))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w: %w", ErrDeps, errors.Join(problems...))
	}

	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Clock == nil {
		d.Clock = time.Now
	}
	if d.BatchSize <= 0 {
		d.BatchSize = defaultBatchSize
	}
	if d.Interval <= 0 {
		d.Interval = defaultInterval
	}
	return &Orchestrator{deps: d}, nil
}

// Run drains the queue until ctx is cancelled.
func (o *Orchestrator) Run(ctx context.Context) error {
	ticker := time.NewTicker(o.deps.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := o.ProcessOnce(ctx); err != nil && ctx.Err() == nil {
				o.deps.Logger.Error("draining incident queue", "error", err)
			}
		}
	}
}

// ProcessOnce drains up to one batch and returns how many incidents moved. It
// is exported so tests drive the loop deterministically rather than racing a
// ticker.
func (o *Orchestrator) ProcessOnce(ctx context.Context) (int, error) {
	pending, _, err := store.ListIncidents(ctx, o.deps.DB, store.IncidentFilter{
		States: []string{"received"},
		Limit:  o.deps.BatchSize,
	})
	if err != nil {
		return 0, fmt.Errorf("listing received incidents: %w", err)
	}

	moved := 0
	for _, incident := range pending {
		if ctx.Err() != nil {
			return moved, nil
		}
		if err := o.process(ctx, incident); err != nil {
			// One bad incident must not stall the queue behind it.
			o.deps.Logger.Error("processing incident", "error", err, "incident_id", incident.ID)
			continue
		}
		moved++
	}
	return moved, nil
}

func (o *Orchestrator) process(ctx context.Context, in store.Incident) error {
	now := o.deps.Clock()
	registry := o.deps.Registry()

	project, _, err := store.GetProject(ctx, o.deps.DB, in.ProjectSlug)
	if err != nil {
		return fmt.Errorf("loading project %q: %w", in.ProjectSlug, err)
	}

	window := registry.Defaults.SuppressionWindow.Duration
	var sourceRoots []string
	if eff, ok := registry.EffectiveProject(in.ProjectSlug); ok {
		window = eff.SuppressionWindow
		sourceRoots = eff.SourceRoots
	}

	result := o.fingerprint(in, sourceRoots)
	if err := store.MarkFingerprint(ctx, o.deps.DB, in.ID, result.Hash); err != nil {
		return err
	}

	outcome, err := store.ObserveFingerprint(ctx, o.deps.DB, store.FingerprintObservation{
		Fingerprint: result.Hash,
		ProjectSlug: in.ProjectSlug,
		Strategy:    string(result.Strategy),
		Frames:      result.Frames,
		IncidentID:  in.ID,
		Window:      window,
	}, now)
	if err != nil {
		return err
	}

	decision := o.deps.Chain.Evaluate(triage.Subject{
		ProjectSlug: in.ProjectSlug,
		Kind:        in.Kind,
		Title:       in.Title,
		Body:        in.Body,
		AuthorEmail: in.Metadata["author_email"],
		Quarantined: project.Quarantined,
		Suppressed:  outcome.Suppressed,
	})

	target := "triaging"
	switch decision.Verdict {
	case triage.VerdictFiltered:
		target = "filtered"
	case triage.VerdictSuppressed:
		target = "suppressed"
	}

	payload, err := json.Marshal(map[string]any{
		"filter":            decision.Filter,
		"detail":            decision.Reason,
		"fingerprint":       result.Hash,
		"strategy":          string(result.Strategy),
		"occurrence_count":  outcome.TotalOccurrences,
		"first_incident_id": outcome.FirstIncidentID,
	})
	if err != nil {
		return fmt.Errorf("encoding transition payload: %w", err)
	}

	eventID, err := store.Transition(ctx, o.deps.DB, store.TransitionParams{
		IncidentID: in.ID,
		From:       "received",
		To:         target,
		Reason:     decision.Filter,
		Actor:      "tier0",
		Payload:    payload,
	}, now)
	if err != nil {
		// Another worker moved it first. Not an error worth logging loudly.
		if errors.Is(err, store.ErrStaleTransition) {
			return nil
		}
		return err
	}

	o.publish(eventID, in, target, decision, result, outcome)
	return nil
}

// fingerprint groups the incident. CI failures have no stack trace, so they are
// grouped by failing job and step instead — grouping on the workflow name alone
// would collapse every failure of one workflow into a single incident
// (design §4.4.2).
func (o *Orchestrator) fingerprint(in store.Incident, sourceRoots []string) triage.FingerprintResult {
	if in.Kind == "workflow_run.failed" {
		var steps []string
		if raw := in.Metadata["job_steps"]; raw != "" {
			steps = strings.Split(raw, "\x1f")
		}
		return triage.WorkflowFingerprint(in.ProjectSlug, in.Metadata["workflow"], steps)
	}

	return triage.ComputeFingerprint(triage.FingerprintInput{
		ProjectSlug: in.ProjectSlug,
		ErrorClass:  triage.ErrorClass(in.Title, in.Body),
		Frames:      triage.ExtractFrames(in.Body),
		SourceRoots: sourceRoots,
	})
}

// publish emits the transition on the SSE bus. Event.ID carries
// incident_events.id, so a reconnecting tab replaying from Last-Event-ID reads
// the same sequence the audit trail wrote (SPEC §4.11).
func (o *Orchestrator) publish(
	eventID int64, in store.Incident, state string,
	decision triage.Decision, fp triage.FingerprintResult, outcome store.FingerprintOutcome,
) {
	data, err := json.Marshal(map[string]any{
		"incident_id":      in.ID,
		"project_slug":     in.ProjectSlug,
		"source":           in.Source,
		"kind":             in.Kind,
		"title":            in.Title,
		"state":            state,
		"reason":           decision.Filter,
		"fingerprint":      fp.Hash,
		"strategy":         string(fp.Strategy),
		"occurrence_count": outcome.TotalOccurrences,
	})
	if err != nil {
		o.deps.Logger.Error("encoding bus event", "error", err, "incident_id", in.ID)
		return
	}

	o.deps.Hub.Publish(bus.Event{
		ID:    eventID,
		Topic: topicIncidents,
		Type:  "incident.state_change",
		Data:  data,
	})
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -race ./internal/orchestrator/ ./internal/ingest/ -v`

Expected: PASS. `TestProcessOnceSuppressesTheSecondOccurrence` proves the storm path end to end through the real store.

- [ ] **Step 6: Run the gate and commit**

Run: `make check`

```bash
git add internal/orchestrator internal/ingest/writer.go internal/ingest/subscriber_test.go
git commit -m "feat(orchestrator): the Tier 0 process loop

The queue is the incidents table, not an in-memory channel (SPEC 4.12),
so a restart resumes exactly where it left off — 'the queue' is simply
the rows in state received.

Each incident is fingerprinted, observed against its suppression window,
evaluated by the Tier 0 chain, transitioned, and published to the bus
with incident_events.id as the SSE event id so replay and the audit trail
share one sequence.

CI failures fingerprint by failing job and step rather than by stack
trace; Event.JobSteps is now carried through persistence in metadata,
without which grouping would degrade to the workflow name and collapse
every ci.yml failure into one incident.

An incident that passes Tier 0 rests in triaging. M1 has no Tier 1, and
that transition is the single insertion point for M2's classifier.

One failing incident is logged and skipped rather than stalling the queue
behind it, and a lost race on Transition is treated as a no-op.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 16: `httpapi` — incident routes, SSE replay, and ingest freshness

**Files:**
- Create: `internal/httpapi/incidents.go`
- Create: `internal/httpapi/overview.go`
- Create: `internal/httpapi/replay.go`
- Modify: `internal/httpapi/server.go` (routes, `Deps.IngestStats`)
- Modify: `internal/httpapi/health.go` (staleness)
- Test: `internal/httpapi/incidents_test.go`, `internal/httpapi/replay_test.go`

**Interfaces:**
- Consumes: Tasks 5–7, 14.
- Produces:
  - `httpapi.NewStoreReplay(db *store.DB) ReplayFunc` — the `store` → `bus` adapter M0 left as a seam.
  - `httpapi.IncidentSummary`, `httpapi.IncidentDetail`, `httpapi.OverviewResponse`, `httpapi.ProjectSummary` JSON contracts.
  - `Deps.IngestStats func() (ingest.Stats, error)` and `Deps.IngestStaleAfter time.Duration`.

**Read-only by design.** Every mutating route in SPEC §8 belongs to a later milestone, which is what lets CSRF stay deferred exactly as M0 planned — there is still no state-changing route to protect.

**`store` cannot import `bus`,** so `EventsAfter` returns `[]store.IncidentEvent` and this package maps them. That mapping is `NewStoreReplay`, filling the `ReplayFunc` seam M0 declared at `server.go:32`.

- [ ] **Step 1: Write the failing replay test**

Create `internal/httpapi/replay_test.go`:

```go
package httpapi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/store"
)

func replayFixture(t *testing.T) (*store.DB, context.Context, time.Time, int64) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	db, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if err := store.SyncProjects(ctx, db, []store.ProjectRow{
		{Slug: "api", Repo: "github.com/example/api", DefaultBranch: "main"},
	}, now); err != nil {
		t.Fatalf("SyncProjects() error = %v", err)
	}

	res, err := store.IngestIncident(ctx, db, store.IngestParams{
		ProjectSlug: "api", Source: "gcplog", SourceRef: "r1", Kind: "log.error",
		Title: "boom", State: "received", OccurredAt: now,
	}, now)
	if err != nil {
		t.Fatalf("IngestIncident() error = %v", err)
	}
	return db, ctx, now, res.ID
}

func TestNewStoreReplay(t *testing.T) {
	db, ctx, now, id := replayFixture(t)

	first, err := store.Transition(ctx, db, store.TransitionParams{
		IncidentID: id, From: "received", To: "triaging", Actor: "tier0",
	}, now)
	if err != nil {
		t.Fatalf("Transition() error = %v", err)
	}

	replay := NewStoreReplay(db)

	t.Run("returns events after the given id", func(t *testing.T) {
		events, err := replay(ctx, 0, []string{"incidents"})
		if err != nil {
			t.Fatalf("replay() error = %v, want nil", err)
		}
		if len(events) != 1 {
			t.Fatalf("len = %d, want 1", len(events))
		}
		ev := events[0]
		if ev.ID != first {
			t.Errorf("ID = %d, want %d; the SSE id must be incident_events.id", ev.ID, first)
		}
		if ev.Topic != "incidents" {
			t.Errorf("Topic = %q, want %q", ev.Topic, "incidents")
		}

		var payload map[string]any
		if err := json.Unmarshal(ev.Data, &payload); err != nil {
			t.Fatalf("Data is not valid JSON: %v", err)
		}
		if payload["incident_id"] == nil {
			t.Error("Data has no incident_id")
		}
	})

	t.Run("nothing after the latest id", func(t *testing.T) {
		events, err := replay(ctx, first, nil)
		if err != nil {
			t.Fatalf("replay() error = %v", err)
		}
		if len(events) != 0 {
			t.Errorf("len = %d, want 0", len(events))
		}
	})

	t.Run("a client not subscribed to incidents gets nothing", func(t *testing.T) {
		events, err := replay(ctx, 0, []string{"budget"})
		if err != nil {
			t.Fatalf("replay() error = %v", err)
		}
		if len(events) != 0 {
			t.Errorf("len = %d, want 0; replay must respect topic subscriptions", len(events))
		}
	})

	t.Run("empty topics means every topic", func(t *testing.T) {
		events, err := replay(ctx, 0, nil)
		if err != nil {
			t.Fatalf("replay() error = %v", err)
		}
		if len(events) != 1 {
			t.Errorf("len = %d, want 1", len(events))
		}
	})
}
```

- [ ] **Step 2: Write the replay adapter**

Create `internal/httpapi/replay.go`:

```go
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/store"
)

// replayLimit bounds a single reconnect backfill. A tab that has been closed
// for a week refetches over HTTP instead of replaying a month of history.
const replayLimit = 500

// NewStoreReplay returns the ReplayFunc the SSE handler uses to backfill a
// reconnecting client.
//
// This is the seam M0 declared and wired to nil. store deliberately imports
// nothing but config (SPEC §4), so it returns []store.IncidentEvent and the
// mapping to bus.Event lives here — the alternative would invert the dependency
// graph.
func NewStoreReplay(db *store.DB) ReplayFunc {
	return func(ctx context.Context, lastEventID int64, topics []string) ([]bus.Event, error) {
		if !wantsTopic(topics, topicIncidents) {
			return nil, nil
		}

		rows, err := store.EventsAfter(ctx, db, lastEventID, replayLimit)
		if err != nil {
			return nil, fmt.Errorf("replaying incident events: %w", err)
		}

		events := make([]bus.Event, 0, len(rows))
		for _, row := range rows {
			data, err := json.Marshal(map[string]any{
				"incident_id": row.IncidentID,
				"kind":        row.Kind,
				"actor":       row.Actor,
				"from_state":  row.FromState,
				"state":       row.ToState,
				"ts":          row.TS,
				"payload":     json.RawMessage(row.Payload),
			})
			if err != nil {
				return nil, fmt.Errorf("encoding replay event %d: %w", row.ID, err)
			}
			events = append(events, bus.Event{
				ID:    row.ID,
				Topic: topicIncidents,
				Type:  "incident." + row.Kind,
				Data:  data,
			})
		}
		return events, nil
	}
}

// wantsTopic reports whether a subscriber wants a topic. An empty topic list
// means every topic, matching bus.Client.wants.
func wantsTopic(topics []string, topic string) bool {
	if len(topics) == 0 {
		return true
	}
	for _, t := range topics {
		if t == topic {
			return true
		}
	}
	return false
}
```

Add `const topicIncidents = "incidents"` to `internal/httpapi/server.go` near the other constants.

- [ ] **Step 3: Run the replay test**

Run: `go test ./internal/httpapi/ -run TestNewStoreReplay -v`

Expected: PASS.

- [ ] **Step 4: Write the failing route test**

Create `internal/httpapi/incidents_test.go`:

```go
package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestIncidentRoutes(t *testing.T) {
	db, ctx, now, id := replayFixture(t)
	if _, err := storeTransitionForTest(t, db, ctx, id, now); err != nil {
		t.Fatalf("seeding transition: %v", err)
	}

	srv := newTestServerWithDB(t, db)

	t.Run("list requires a session", func(t *testing.T) {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/incidents", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	cookie := loginForTest(t, srv)

	t.Run("list returns the page and total", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
		var body struct {
			Incidents []IncidentSummary `json:"incidents"`
			Total     int               `json:"total"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if body.Total != 1 || len(body.Incidents) != 1 {
			t.Fatalf("total/len = %d/%d, want 1/1", body.Total, len(body.Incidents))
		}
		if body.Incidents[0].Title != "boom" {
			t.Errorf("Title = %q, want %q", body.Incidents[0].Title, "boom")
		}
	})

	t.Run("state filter is applied", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents?state=filtered")
		var body struct {
			Total int `json:"total"`
		}
		_ = json.NewDecoder(rec.Body).Decode(&body)
		if body.Total != 0 {
			t.Errorf("total = %d, want 0", body.Total)
		}
	})

	t.Run("detail includes the timeline", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents/"+strconv.FormatInt(id, 10))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
		var detail IncidentDetail
		if err := json.NewDecoder(rec.Body).Decode(&detail); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if len(detail.Events) == 0 {
			t.Error("Events is empty; the detail view renders the timeline from it")
		}
	})

	t.Run("unknown id is 404 not 500", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents/999999")
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rec.Code)
		}
	})

	t.Run("non-numeric id is 400", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents/abc")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("overview reports state counters", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/overview")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var body OverviewResponse
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if body.IncidentsByState["triaging"] != 1 {
			t.Errorf("IncidentsByState = %v, want triaging 1", body.IncidentsByState)
		}
	})

	t.Run("projects lists the registry with counts", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/projects")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var body struct {
			Projects []ProjectSummary `json:"projects"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if len(body.Projects) != 1 || body.Projects[0].Slug != "api" {
			t.Errorf("Projects = %v, want the one registered project", body.Projects)
		}
	})

	t.Run("mutating methods are rejected", func(t *testing.T) {
		// M1 is read-only; every mutating route belongs to a later milestone.
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/incidents", nil)
		req.AddCookie(cookie)
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
	})
}
```

Append these helpers to `internal/httpapi/incidents_test.go`. They build on the ones M0 already provides in `health_test.go` and `auth_test.go` — `newTestServer(t, mutate func(*Deps)) *Server`, `login(t, srv, password) *http.Response`, `sessionCookie(t, resp) *http.Cookie`, `hashFor(t, password)`, and the `testPassword` constant — rather than duplicating them:

```go
// newTestServerWithDB reuses M0's server helper but swaps in a pre-seeded
// database, since the route tests need incidents that already exist.
func newTestServerWithDB(t *testing.T, db *store.DB) *Server {
	t.Helper()
	return newTestServer(t, func(d *Deps) {
		d.DB = db
		d.Env.DashboardPasswordHash = hashFor(t, testPassword)
		d.Replay = NewStoreReplay(db)
		d.HeartbeatInterval = time.Hour
		d.Registry = config.Registry{
			Projects: []config.Project{
				{Slug: "api", Repo: "github.com/example/api", DefaultBranch: "main"},
			},
		}
	})
}

// loginForTest signs in and returns the session cookie.
func loginForTest(t *testing.T, srv *Server) *http.Cookie {
	t.Helper()
	resp := login(t, srv, testPassword)
	cookie := sessionCookie(t, resp)
	if cookie == nil {
		t.Fatal("login returned no session cookie")
	}
	return cookie
}

// authedGet issues an authenticated GET and returns the recorder.
func authedGet(t *testing.T, srv *Server, cookie *http.Cookie, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// storeTransitionForTest moves the seeded incident to triaging so the list and
// detail views have a timeline to render.
func storeTransitionForTest(t *testing.T, db *store.DB, ctx context.Context, id int64, now time.Time) (int64, error) {
	t.Helper()
	return store.Transition(ctx, db, store.TransitionParams{
		IncidentID: id, From: "received", To: "triaging", Actor: "tier0",
	}, now)
}
```

Add `context`, `net/http`, `net/http/httptest`, `time`, `config`, and `store` to that file's imports.

- [ ] **Step 5: Write the handlers**

Create `internal/httpapi/incidents.go`:

```go
package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/b1codes/triage-sentinel/internal/store"
)

// maxPageSize bounds a client-supplied limit so a single request cannot pull
// the whole table into memory.
const maxPageSize = 200

// IncidentSummary is one row of the incident feed.
type IncidentSummary struct {
	ID              int64     `json:"id"`
	ProjectSlug     string    `json:"project_slug"`
	Source          string    `json:"source"`
	Kind            string    `json:"kind"`
	Title           string    `json:"title"`
	State           string    `json:"state"`
	StateReason     string    `json:"state_reason,omitempty"`
	Fingerprint     string    `json:"fingerprint,omitempty"`
	OccurrenceCount int       `json:"occurrence_count"`
	OccurredAt      time.Time `json:"occurred_at"`
	CreatedAt       time.Time `json:"created_at"`
}

// TimelineEntry is one audit row in the detail view.
type TimelineEntry struct {
	ID        int64     `json:"id"`
	TS        time.Time `json:"ts"`
	Kind      string    `json:"kind"`
	Actor     string    `json:"actor"`
	FromState string    `json:"from_state,omitempty"`
	ToState   string    `json:"to_state,omitempty"`
	Payload   any       `json:"payload,omitempty"`
}

// IncidentDetail is one incident with its timeline.
type IncidentDetail struct {
	IncidentSummary
	Body        string            `json:"body,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Strategy    string            `json:"fingerprint_strategy,omitempty"`
	Frames      []string          `json:"fingerprint_frames,omitempty"`
	Events      []TimelineEntry   `json:"events"`
	UpdatedAt   time.Time         `json:"updated_at"`
	ClosedAt    *time.Time        `json:"closed_at,omitempty"`
}

func summarize(in store.Incident) IncidentSummary {
	return IncidentSummary{
		ID: in.ID, ProjectSlug: in.ProjectSlug, Source: in.Source, Kind: in.Kind,
		Title: in.Title, State: in.State, StateReason: in.StateReason,
		Fingerprint: in.Fingerprint, OccurrenceCount: in.OccurrenceCount,
		OccurredAt: in.OccurredAt, CreatedAt: in.CreatedAt,
	}
}

// handleIncidents serves GET /api/incidents.
func (s *Server) handleIncidents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := store.IncidentFilter{
		States:   splitCSV(q.Get("state")),
		Projects: splitCSV(q.Get("project")),
		Sources:  splitCSV(q.Get("source")),
		Limit:    intParam(q.Get("limit"), 50, maxPageSize),
		Offset:   intParam(q.Get("offset"), 0, 1<<20),
	}

	incidents, total, err := store.ListIncidents(r.Context(), s.deps.DB, filter)
	if err != nil {
		s.log.Error("listing incidents", "error", err)
		s.writeError(w, http.StatusInternalServerError, "could not list incidents")
		return
	}

	summaries := make([]IncidentSummary, 0, len(incidents))
	for _, in := range incidents {
		summaries = append(summaries, summarize(in))
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"incidents": summaries,
		"total":     total,
		"limit":     filter.Limit,
		"offset":    filter.Offset,
	})
}

// handleIncident serves GET /api/incidents/{id}.
func (s *Server) handleIncident(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "incident id must be an integer")
		return
	}

	incident, found, err := store.GetIncident(r.Context(), s.deps.DB, id)
	if err != nil {
		s.log.Error("getting incident", "error", err, "incident_id", id)
		s.writeError(w, http.StatusInternalServerError, "could not load incident")
		return
	}
	if !found {
		s.writeError(w, http.StatusNotFound, "incident not found")
		return
	}

	events, err := store.EventsForIncident(r.Context(), s.deps.DB, id)
	if err != nil {
		s.log.Error("loading incident timeline", "error", err, "incident_id", id)
		s.writeError(w, http.StatusInternalServerError, "could not load timeline")
		return
	}

	timeline := make([]TimelineEntry, 0, len(events))
	for _, ev := range events {
		timeline = append(timeline, TimelineEntry{
			ID: ev.ID, TS: ev.TS, Kind: ev.Kind, Actor: ev.Actor,
			FromState: ev.FromState, ToState: ev.ToState,
			Payload: rawOrNil(ev.Payload),
		})
	}

	detail := IncidentDetail{
		IncidentSummary: summarize(incident),
		Body:            incident.Body,
		Metadata:        incident.Metadata,
		Events:          timeline,
		UpdatedAt:       incident.UpdatedAt,
		ClosedAt:        incident.ClosedAt,
	}

	// The grouping evidence is what makes M1 a useful observation period.
	if incident.Fingerprint != "" {
		if rec, ok, err := store.GetFingerprint(r.Context(), s.deps.DB, incident.Fingerprint); err == nil && ok {
			detail.Strategy = rec.Strategy
			detail.Frames = rec.Frames
		}
	}

	s.writeJSON(w, http.StatusOK, detail)
}

func rawOrNil(payload []byte) any {
	if len(payload) == 0 || string(payload) == "{}" {
		return nil
	}
	return jsonRaw(payload)
}

// jsonRaw marshals pre-encoded JSON without re-encoding it.
type jsonRaw []byte

// MarshalJSON returns the bytes unchanged.
func (r jsonRaw) MarshalJSON() ([]byte, error) { return r, nil }

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func intParam(raw string, fallback, limit int) int {
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return fallback
	}
	return min(n, limit)
}
```

Create `internal/httpapi/overview.go`:

```go
package httpapi

import (
	"net/http"
	"time"

	"github.com/b1codes/triage-sentinel/internal/store"
)

// OverviewResponse is the dashboard's landing payload. Budget fields arrive
// in M2; M1 reports what it actually knows.
type OverviewResponse struct {
	IncidentsByState map[string]int `json:"incidents_by_state"`
	Projects         int            `json:"projects"`
	LastIngestAt     *time.Time     `json:"last_ingest_at,omitempty"`
	IngestStale      bool           `json:"ingest_stale"`

	// AwaitingTriage is the count resting in triaging. M1 has no Tier 1, so
	// this is displayed plainly rather than as a queue being worked.
	AwaitingTriage int `json:"awaiting_triage"`
}

// ProjectSummary is one row of the projects view.
type ProjectSummary struct {
	Slug             string `json:"slug"`
	Repo             string `json:"repo"`
	DefaultBranch    string `json:"default_branch"`
	Active           bool   `json:"active"`
	Quarantined      bool   `json:"quarantined"`
	QuarantineReason string `json:"quarantine_reason,omitempty"`
	Incidents        int    `json:"incidents"`
}

// handleOverview serves GET /api/overview.
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	counts, err := store.CountByState(r.Context(), s.deps.DB)
	if err != nil {
		s.log.Error("counting incidents", "error", err)
		s.writeError(w, http.StatusInternalServerError, "could not load overview")
		return
	}

	resp := OverviewResponse{
		IncidentsByState: counts,
		Projects:         len(s.Registry().Projects),
		AwaitingTriage:   counts["triaging"],
	}

	if at, ok, err := store.LastIngestAt(r.Context(), s.deps.DB); err == nil && ok {
		resp.LastIngestAt = &at
		resp.IngestStale = s.ingestIsStale(at)
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// handleProjects serves GET /api/projects.
func (s *Server) handleProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := store.ListProjects(r.Context(), s.deps.DB)
	if err != nil {
		s.log.Error("listing projects", "error", err)
		s.writeError(w, http.StatusInternalServerError, "could not list projects")
		return
	}

	summaries := make([]ProjectSummary, 0, len(projects))
	for _, p := range projects {
		_, total, err := store.ListIncidents(r.Context(), s.deps.DB, store.IncidentFilter{
			Projects: []string{p.Slug}, Limit: 1,
		})
		if err != nil {
			s.log.Error("counting project incidents", "error", err, "slug", p.Slug)
		}
		summaries = append(summaries, ProjectSummary{
			Slug: p.Slug, Repo: p.Repo, DefaultBranch: p.DefaultBranch,
			Active: p.Active, Quarantined: p.Quarantined,
			QuarantineReason: p.QuarantineReason, Incidents: total,
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]any{"projects": summaries})
}

// ingestIsStale reports whether the last successful ingest is older than the
// configured threshold. A stalled subscriber is the most likely silent failure
// in the system — the process looks healthy while seeing nothing (SPEC §12).
func (s *Server) ingestIsStale(last time.Time) bool {
	threshold := s.deps.IngestStaleAfter
	if threshold <= 0 {
		return false
	}
	return s.deps.Now().Sub(last) > threshold
}
```

- [ ] **Step 6: Wire the routes and health**

In `internal/httpapi/server.go`, add to `Deps`:

```go
	// IngestStats reports subscriber counters for /api/health. Nil means
	// ingestion is not running, which --no-ingest makes explicit.
	IngestStats func() (ingest.Stats, error)

	// IngestStaleAfter is how long without a successful pull before ingestion
	// is reported stale. Zero disables the check.
	IngestStaleAfter time.Duration
```

and to `routes()`:

```go
	s.mux.Handle("GET /api/overview", s.requireSession(http.HandlerFunc(s.handleOverview)))
	s.mux.Handle("GET /api/incidents", s.requireSession(http.HandlerFunc(s.handleIncidents)))
	s.mux.Handle("GET /api/incidents/{id}", s.requireSession(http.HandlerFunc(s.handleIncident)))
	s.mux.Handle("GET /api/projects", s.requireSession(http.HandlerFunc(s.handleProjects)))
```

In `internal/httpapi/health.go`, add before the status decision:

```go
	if at, ok, err := store.LastIngestAt(r.Context(), s.deps.DB); err != nil {
		resp.Problems = append(resp.Problems, "ingest cursor unavailable: "+err.Error())
	} else if !ok {
		// Not a problem on a fresh install: nothing has arrived yet.
		resp.IngestStale = false
	} else {
		resp.LastIngestAt = &at
		if s.ingestIsStale(at) {
			resp.IngestStale = true
			resp.Problems = append(resp.Problems,
				"no successful ingest since "+at.Format(time.RFC3339))
		}
	}
```

and to `HealthResponse`:

```go
	LastIngestAt *time.Time `json:"last_ingest_at,omitempty"`
	IngestStale  bool       `json:"ingest_stale"`
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test -race ./internal/httpapi/ -v`

Expected: PASS, including every pre-existing M0 test.

- [ ] **Step 8: Run the gate and commit**

Run: `make check`

```bash
git add internal/httpapi
git commit -m "feat(httpapi): incident routes, SSE replay, and ingest freshness

NewStoreReplay fills the ReplayFunc seam M0 declared and wired to nil.
store imports nothing but config, so EventsAfter returns
[]store.IncidentEvent and the mapping to bus.Event lives here — the
alternative would invert the dependency graph.

Every route is a GET. M1 is read-only, which keeps 'spent \$0.00 and
changed nothing' literally true and lets CSRF stay deferred exactly as M0
planned: there is still no state-changing route to protect.

The overview reports awaiting_triage plainly. M1 has no Tier 1, so those
incidents are resting rather than queued, and the UI must not imply
otherwise.

/api/health gains ingest staleness. A stalled subscriber is the most
likely silent failure in the system — the process looks healthy while
seeing nothing. SPEC 12 wants a notification for this; notify does not
exist until M2, so M1 delivers the detection.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 17: `cmd/sentinel` — lifecycle wiring, `--no-ingest`, and `replay`

**Files:**
- Modify: `cmd/sentinel/run.go`
- Create: `cmd/sentinel/ingestwiring.go`
- Test: `cmd/sentinel/run_test.go`
- Modify: `.env.example`, `Makefile`

**Interfaces:**
- Consumes: Tasks 1, 3, 9–16.
- Produces:
  - `options.noIngest bool` from `--no-ingest`.
  - `startIngestion(ctx context.Context, deps ingestDeps) (*ingest.Subscriber, error)`.
  - `replayFile(ctx context.Context, opts options, path string, stdout io.Writer) error` — the `replay` subcommand.

**Environment assertions land here, not in `LoadEnv`.** M0 established that each milestone asserts the secrets it needs at wiring time, so the binary stays runnable without credentials it does not use. M1 asserts `GCP_PROJECT_ID`, `PUBSUB_SUBSCRIPTION`, `GITHUB_WEBHOOK_SECRET`, and `GITHUB_TOKEN` — the last one a milestone earlier than SPEC §14 implies, because job-level fingerprinting reads the Actions API with read scope.

**`--no-ingest` is an explicit opt-out, never a silent skip.** Starting without ingestion when credentials happen to be missing would reproduce exactly the silent failure SPEC §12 singles out.

- [ ] **Step 1: Write the failing test**

Add to `cmd/sentinel/run_test.go`:

```go
func TestServeRequiresIngestSecrets(t *testing.T) {
	tests := []struct {
		name    string
		missing string
	}{
		{name: "no gcp project", missing: "GCP_PROJECT_ID"},
		{name: "no subscription", missing: "PUBSUB_SUBSCRIPTION"},
		{name: "no webhook secret", missing: "GITHUB_WEBHOOK_SECRET"},
		{name: "no github token", missing: "GITHUB_TOKEN"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := completeIngestEnv()
			delete(env, tc.missing)

			err := assertIngestEnv(envFrom(env))
			if err == nil {
				t.Fatalf("assertIngestEnv() error = nil, want an error naming %s", tc.missing)
			}
			if !strings.Contains(err.Error(), tc.missing) {
				t.Errorf("error %q does not name %s", err.Error(), tc.missing)
			}
		})
	}
}

func TestAssertIngestEnvAcceptsCompleteEnvironment(t *testing.T) {
	if err := assertIngestEnv(envFrom(completeIngestEnv())); err != nil {
		t.Errorf("assertIngestEnv() error = %v, want nil", err)
	}
}

func TestAssertIngestEnvReportsEveryProblemAtOnce(t *testing.T) {
	err := assertIngestEnv(config.Env{})
	if err == nil {
		t.Fatal("assertIngestEnv() error = nil, want an error")
	}
	for _, want := range []string{
		"GCP_PROJECT_ID", "PUBSUB_SUBSCRIPTION", "GITHUB_WEBHOOK_SECRET", "GITHUB_TOKEN",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s; all problems must be reported at once", err.Error(), want)
		}
	}
}

func completeIngestEnv() map[string]string {
	return map[string]string{
		"GCP_PROJECT_ID":        "example-project",
		"PUBSUB_SUBSCRIPTION":   "projects/example-project/subscriptions/sentinel",
		"GITHUB_WEBHOOK_SECRET": "shhh",
		"GITHUB_TOKEN":          "ghp_test",
	}
}

func envFrom(m map[string]string) config.Env {
	return config.Env{
		GCPProjectID:        m["GCP_PROJECT_ID"],
		PubSubSubscription:  m["PUBSUB_SUBSCRIPTION"],
		GitHubWebhookSecret: m["GITHUB_WEBHOOK_SECRET"],
		GitHubToken:         m["GITHUB_TOKEN"],
	}
}

func TestNoIngestFlagIsParsed(t *testing.T) {
	// --no-ingest must be an explicit opt-out. Silently skipping ingestion
	// when credentials are absent would reproduce the exact silent failure
	// SPEC §12 singles out.
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"version", "--no-ingest"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v, want nil; --no-ingest must be a recognised flag", err)
	}
}
```

Add `strings` and the `config` import to the test file if absent.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./cmd/sentinel/ -run 'TestServeRequiresIngestSecrets|TestAssertIngestEnv|TestNoIngestFlag' -v`

Expected: FAIL — `undefined: assertIngestEnv`, and `--no-ingest` is not a defined flag.

- [ ] **Step 3: Write the wiring**

Create `cmd/sentinel/ingestwiring.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/ingest"
	"github.com/b1codes/triage-sentinel/internal/orchestrator"
	"github.com/b1codes/triage-sentinel/internal/store"
	"github.com/b1codes/triage-sentinel/internal/triage"
)

// ErrIngestEnv is returned when the environment lacks a secret ingestion needs.
var ErrIngestEnv = errors.New("incomplete ingestion environment")

// ingestStaleAfter is how long without a successful pull before /api/health
// reports ingestion stale.
const ingestStaleAfter = 30 * time.Minute

// assertIngestEnv checks the secrets M1 introduces.
//
// M0 established that each milestone asserts its own secrets at wiring time
// rather than in LoadEnv, so the binary stays runnable without credentials it
// does not use. GITHUB_TOKEN is required a milestone earlier than SPEC §14
// implies, because job-level fingerprinting reads the Actions API; read scope
// is sufficient, and M4 is the first milestone needing write.
func assertIngestEnv(env config.Env) error {
	required := []struct {
		name  string
		value string
	}{
		{"GCP_PROJECT_ID", env.GCPProjectID},
		{"PUBSUB_SUBSCRIPTION", env.PubSubSubscription},
		{"GITHUB_WEBHOOK_SECRET", env.GitHubWebhookSecret},
		{"GITHUB_TOKEN", env.GitHubToken},
	}

	var problems []error
	for _, r := range required {
		if r.value == "" {
			problems = append(problems, fmt.Errorf(
				"%s is required to run ingestion; pass --no-ingest to start without it", r.name))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %w", ErrIngestEnv, errors.Join(problems...))
	}
	return nil
}

// ingestDeps are what startIngestion needs.
type ingestDeps struct {
	Env      config.Env
	Registry config.Registry
	DB       *store.DB
	Hub      *bus.Hub
	Logger   *slog.Logger
}

// startIngestion builds the router, subscriber, and orchestrator, and starts
// both loops. Both stop when ctx is cancelled.
func startIngestion(ctx context.Context, d ingestDeps) (*ingest.Subscriber, error) {
	resolver := ingest.NewRegistryResolver(d.Registry)

	router := ingest.NewRouter(
		ingest.NewGitHubAdapter(ingest.GitHubOptions{
			Secret:   d.Env.GitHubWebhookSecret,
			Resolver: resolver,
			Jobs:     ingest.NewGitHubJobFetcher(d.Env.GitHubToken, &http.Client{Timeout: 15 * time.Second}),
		}),
		ingest.NewGCPLogAdapter(resolver),
	)

	client, err := ingest.NewPubSubClient(ctx)
	if err != nil {
		return nil, err
	}
	puller, err := ingest.NewRESTPuller(ingest.RESTOptions{
		Subscription: d.Env.PubSubSubscription,
		Client:       client,
	})
	if err != nil {
		return nil, err
	}

	writer := ingest.NewIncidentWriter(d.DB, time.Now)

	subscriber, err := ingest.NewSubscriber(ingest.SubscriberOptions{
		Puller:    puller,
		Router:    router,
		Handler:   writer,
		Malformed: writer,
		Cursor: func(ctx context.Context, at time.Time) error {
			return store.TouchIngestCursor(ctx, d.DB, "pubsub", at)
		},
		Logger: d.Logger,
	})
	if err != nil {
		return nil, err
	}

	orch, err := newOrchestrator(d)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := subscriber.Run(ctx); err != nil {
			d.Logger.Error("subscriber stopped", "error", err)
		}
	}()
	go func() {
		if err := orch.Run(ctx); err != nil {
			d.Logger.Error("orchestrator stopped", "error", err)
		}
	}()

	return subscriber, nil
}

func newOrchestrator(d ingestDeps) (*orchestrator.Orchestrator, error) {
	patterns, err := config.CompileTransientPatterns(d.Registry.Triage.TransientPatterns)
	if err != nil {
		// Unreachable in practice: validation already compiled these at load.
		// Returning rather than panicking keeps the failure legible if the
		// validation path is ever changed.
		return nil, fmt.Errorf("compiling transient patterns: %w", err)
	}

	registry := d.Registry
	return orchestrator.New(orchestrator.Deps{
		DB:  d.DB,
		Hub: d.Hub,
		Chain: triage.NewChain(triage.ChainOptions{
			TransientPatterns: patterns,
			BotEmail:          registry.Bot.Email,
		}),
		Registry: func() config.Registry { return registry },
		Logger:   d.Logger,
	})
}
```

- [ ] **Step 4: Wire it into `serve`**

In `cmd/sentinel/run.go`:

Add to `options`: `noIngest bool`, and register the flag beside the others:

```go
	fs.BoolVar(&opts.noIngest, "no-ingest", false, "start without the Pub/Sub subscriber (local dashboard work only)")
```

In `serve`, after `store.Migrate` succeeds and before `httpapi.NewServer`, sync the projects table — **nothing can write an incident until this runs**, because `incidents.project_slug` is a foreign key:

```go
	rows := make([]store.ProjectRow, 0, len(registry.Projects))
	for _, p := range registry.Projects {
		rows = append(rows, store.ProjectRow{
			Slug: p.Slug, Repo: p.Repo, DefaultBranch: p.DefaultBranch,
		})
	}
	if err := store.SyncProjects(ctx, db, rows, time.Now()); err != nil {
		return err
	}
```

Then, after `hub := bus.NewHub(sseBufferSize)`:

```go
	var ingestStats func() (ingest.Stats, error)

	if opts.noIngest {
		log.Warn("starting without ingestion; no events will be received (--no-ingest)")
	} else {
		if err := assertIngestEnv(env); err != nil {
			return err
		}
		subscriber, err := startIngestion(ctx, ingestDeps{
			Env: env, Registry: registry, DB: db, Hub: hub, Logger: log,
		})
		if err != nil {
			return err
		}
		ingestStats = func() (ingest.Stats, error) { return subscriber.Stats(), nil }
		log.Info("ingestion started", "subscription", env.PubSubSubscription)
	}
```

Extend the `httpapi.Deps` literal with:

```go
		Replay:           httpapi.NewStoreReplay(db),
		IngestStats:      ingestStats,
		IngestStaleAfter: ingestStaleAfter,
```

Finally, in `watchReloads`, re-sync projects after a successful reload so a newly registered project can receive incidents without a restart. Replace the body of the successful branch with:

```go
				rows := make([]store.ProjectRow, 0, len(registry.Projects))
				for _, p := range registry.Projects {
					rows = append(rows, store.ProjectRow{
						Slug: p.Slug, Repo: p.Repo, DefaultBranch: p.DefaultBranch,
					})
				}
				if err := store.SyncProjects(ctx, db, rows, time.Now()); err != nil {
					log.Error("reload applied but project sync failed", "error", err)
				}
				srv.SetRegistry(registry)
				log.Info("configuration reloaded", "projects", len(registry.Projects))
```

`watchReloads` now needs `db *store.DB` as a parameter; update its signature and its single call site.

- [ ] **Step 5: Add the `replay` subcommand**

Add to the `usage` string, after `migrate`:

```
  replay         Feed a recorded payload file through the ingest pipeline
```

Add the case to the subcommand switch:

```go
	case "replay":
		return replayFile(ctx, opts, fs.Arg(0), stdout)
```

And the implementation in `cmd/sentinel/ingestwiring.go`:

```go
// replayFile feeds one recorded payload through the real adapters and process
// loop, so the whole pipeline is exercisable with no GCP access at all. It is a
// development aid, and it is what makes the end-to-end test in Task 21 runnable
// on a laptop.
func replayFile(ctx context.Context, opts options, path string, stdout io.Writer) error {
	if path == "" {
		return errors.New("replay requires a path to a recorded payload file")
	}

	env, registry, err := loadConfig(opts)
	if err != nil {
		return err
	}

	db, err := store.Open(databasePath(env))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if _, err := store.Migrate(context.Background(), db); err != nil {
		return err
	}

	rows := make([]store.ProjectRow, 0, len(registry.Projects))
	for _, p := range registry.Projects {
		rows = append(rows, store.ProjectRow{
			Slug: p.Slug, Repo: p.Repo, DefaultBranch: p.DefaultBranch,
		})
	}
	if err := store.SyncProjects(ctx, db, rows, time.Now()); err != nil {
		return err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}

	logger := newLogger(env, io.Discard)
	hub := bus.NewHub(16)
	defer hub.Close()

	resolver := ingest.NewRegistryResolver(registry)
	router := ingest.NewRouter(
		ingest.NewGitHubAdapter(ingest.GitHubOptions{
			Secret: env.GitHubWebhookSecret, Resolver: resolver,
			Jobs: ingest.NewGitHubJobFetcher(env.GitHubToken, nil),
		}),
		ingest.NewGCPLogAdapter(resolver),
	)

	attributes := map[string]string{"logging.googleapis.com/timestamp": time.Now().UTC().Format(time.RFC3339)}
	if strings.Contains(path, "github") {
		attributes = map[string]string{
			"x-github-event":      inferGitHubEvent(path),
			"x-hub-signature-256": signBody(env.GitHubWebhookSecret, data),
		}
	}

	event, err := router.Route(ctx, ingest.Message{
		ID: "replay", Data: data, Attributes: attributes, PublishTime: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("routing %s: %w", path, err)
	}

	if err := ingest.NewIncidentWriter(db, time.Now).Handle(ctx, event); err != nil {
		return err
	}

	orch, err := newOrchestrator(ingestDeps{
		Env: env, Registry: registry, DB: db, Hub: hub, Logger: logger,
	})
	if err != nil {
		return err
	}
	moved, err := orch.ProcessOnce(ctx)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "replayed %s: source=%s ref=%s project=%s processed=%d\n",
		path, event.Source, event.SourceRef, event.ProjectSlug, moved)
	return nil
}

// inferGitHubEvent guesses the webhook event type from a recorded file's name,
// which is enough for a development aid.
func inferGitHubEvent(path string) string {
	switch {
	case strings.Contains(path, "workflow_run"):
		return "workflow_run"
	case strings.Contains(path, "issue"):
		return "issues"
	default:
		return "workflow_run"
	}
}

// signBody produces the signature the GitHub adapter re-verifies, so a recorded
// payload replays without disabling the check that stops a compromised relay
// injecting events.
func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
```

Add `crypto/hmac`, `crypto/sha256`, `encoding/hex`, `io`, `os`, and `strings` to that file's imports.

- [ ] **Step 6: Update `.env.example` and the Makefile**

In `.env.example`, change the two comments that say "required from M1" to state what M1 actually needs, and add a note for `GITHUB_TOKEN`:

```bash
# GitHub delivery and webhook verification (required from M1)
# GITHUB_TOKEN needs read scope only in M1: job-level fingerprinting reads the
# Actions API. M4 is the first milestone that needs write.
GITHUB_TOKEN=
GITHUB_WEBHOOK_SECRET=
```

Add a Makefile target after `migrate`:

```makefile
## replay: feed a recorded payload through the ingest pipeline
replay:
	go run ./cmd/sentinel replay $(FILE)
```

and add `replay` to the `.PHONY` list.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test -race ./cmd/sentinel/ -v`

Expected: PASS, including the pre-existing M0 tests.

- [ ] **Step 8: Prove the binary still starts without credentials**

Run:
```bash
go run ./cmd/sentinel serve --no-ingest -config projects.example.yaml 2>&1 | head -5
```

Expected: it either starts and logs the `--no-ingest` warning, or fails on `DASHBOARD_PASSWORD_HASH`. Either proves ingestion no longer blocks local dashboard work. Then confirm the opposite:

```bash
go run ./cmd/sentinel serve -config projects.example.yaml 2>&1 | grep -i 'GCP_PROJECT_ID\|DASHBOARD'
```

Expected: an error naming the missing ingestion secrets, not a silent start.

- [ ] **Step 9: Run the gate and commit**

Run: `make check`

```bash
git add cmd/sentinel .env.example Makefile
git commit -m "feat(cmd): wire ingestion, project sync, and the replay subcommand

The projects table is synced from the registry at startup and after every
successful SIGHUP reload. Nothing can write an incident until it is:
foreign_keys is verified on at open and incidents.project_slug references
projects(slug).

Ingestion secrets are asserted at wiring time, matching M0's rule that
each milestone asserts what it uses so the binary stays runnable without
credentials it does not need. GITHUB_TOKEN arrives a milestone early
because job-level fingerprinting reads the Actions API; read scope only.

--no-ingest is an explicit, logged opt-out. Silently skipping ingestion
when credentials happen to be absent would reproduce the exact silent
failure SPEC 12 singles out.

sentinel replay feeds a recorded payload through the real adapters and
process loop with no GCP access, and signs the body so the HMAC check
that stops a compromised relay injecting events stays in force.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 18: `web` — router, query client, and the SSE hook

**Files:**
- Modify: `web/package.json`
- Create: `web/src/lib/api.ts` (replaces `web/src/api.ts`)
- Create: `web/src/lib/sse.ts`
- Create: `web/src/layout.tsx`
- Modify: `web/src/main.tsx`, `web/src/App.tsx`, `web/src/styles.css`

**Interfaces:**
- Consumes: Task 16's JSON contracts.
- Produces:
  - `lib/api.ts` — typed `getOverview`, `getIncidents`, `getIncident`, `getProjects`, `getHealth`, `getSession`, `login`, `logout`, plus the matching TypeScript types.
  - `lib/sse.ts` — `useSentinelStream(queryClient)`, one `EventSource` multiplexed across topics.
  - `layout.tsx` — `<Layout>` nav shell with a connection indicator.

**Design decision §2.4.** The router and TanStack Query arrive now so M2's Spend and Parked views are additive rather than a rewrite, and so §9's "SSE events patch the query cache, `resync` invalidates it" contract is proven while there is exactly one topic to prove it against.

**The `resync` contract is the part that matters.** `bus.Hub` drops a slow subscriber's buffer and sends a single `resync` (`bus.go:151-172`). The client must respond by **invalidating** the cache — refetching over HTTP — not by trying to reconstruct state from a stream it has already missed.

- [ ] **Step 1: Add the dependencies**

Run:
```bash
cd web && npm install react-router @tanstack/react-query && cd ..
```

Expected: both land in `dependencies`. Commit `package-lock.json` with the rest of this task.

- [ ] **Step 2: Write the API client**

Create `web/src/lib/api.ts`:

```ts
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
```

Delete `web/src/api.ts` and update its importers.

- [ ] **Step 3: Write the SSE hook**

Create `web/src/lib/sse.ts`:

```ts
import { useEffect, useState } from 'react'
import type { QueryClient } from '@tanstack/react-query'

export type StreamState = 'connecting' | 'live' | 'closed'

/** Topics the dashboard subscribes to. Log topics are opened per-run in M3. */
const TOPICS = ['incidents', 'runs', 'budget'] as const

interface BusEvent {
  id: number
  topic: string
  type: string
  data?: unknown
}

/**
 * Opens one EventSource multiplexed across every topic and keeps the query
 * cache fresh from it.
 *
 * A `resync` event means the server dropped this client's buffer because it
 * fell behind (see internal/bus/bus.go). The only correct response is to
 * invalidate and refetch over HTTP — the missed events are gone, and trying to
 * reconstruct state from the stream would leave the UI quietly wrong. That is
 * the self-healing path SPEC §9 specifies, and it is why there is no second
 * client-side state store.
 */
export function useSentinelStream(queryClient: QueryClient): StreamState {
  const [state, setState] = useState<StreamState>('connecting')

  useEffect(() => {
    const source = new EventSource(`/api/stream?topics=${TOPICS.join(',')}`)

    source.onopen = () => setState('live')

    source.onerror = () => {
      setState('closed')
      // EventSource reconnects on its own, replaying from Last-Event-ID. A
      // reconnect may have missed transitions, so refetch when it lands.
      queryClient.invalidateQueries()
    }

    source.onmessage = (message: MessageEvent<string>) => {
      setState('live')

      let event: BusEvent
      try {
        event = JSON.parse(message.data) as BusEvent
      } catch {
        return // a malformed frame must not tear down the stream
      }

      if (event.type === 'resync') {
        queryClient.invalidateQueries()
        return
      }

      if (event.topic === 'incidents') {
        void queryClient.invalidateQueries({ queryKey: ['incidents'] })
        void queryClient.invalidateQueries({ queryKey: ['overview'] })
      }
    }

    return () => {
      source.close()
      setState('closed')
    }
  }, [queryClient])

  return state
}
```

- [ ] **Step 4: Write the layout**

Create `web/src/layout.tsx`:

```tsx
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
```

- [ ] **Step 5: Rewire `main.tsx` and `App.tsx`**

Replace `web/src/main.tsx`:

```tsx
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router'
import App from './App'
import './styles.css'

// staleTime is deliberately generous: the SSE stream is what makes the UI
// live, so background polling would be redundant work on a memory-constrained
// host. The stream invalidates what changed.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 30_000, refetchOnWindowFocus: false, retry: 1 },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
)
```

Replace `web/src/App.tsx` with a shell that keeps the existing login flow and mounts the routes. The views themselves land in Task 19; reference them here so the router is complete:

```tsx
import { useCallback, useEffect, useState } from 'react'
import { Route, Routes } from 'react-router'
import { useQueryClient } from '@tanstack/react-query'
import { getSession, login, logout } from './lib/api'
import { useSentinelStream } from './lib/sse'
import { Layout } from './layout'
import { OverviewView } from './views/overview'
import { IncidentView } from './views/incident'
import { ProjectsView } from './views/projects'
import { ComingSoon } from './views/stub'

function LoginForm({ onSuccess }: { onSuccess: () => void }) {
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await login(password)
      onSuccess()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Login failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="centered">
      <h1>triage-sentinel</h1>
      <p className="sub">Sign in to continue.</p>
      <form className="login" onSubmit={submit}>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Dashboard password"
          autoFocus
          aria-label="Dashboard password"
        />
        <button type="submit" disabled={busy}>
          {busy ? 'Signing in…' : 'Sign in'}
        </button>
      </form>
      {error && <p className="error">{error}</p>}
    </main>
  )
}

export default function App() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null)
  const queryClient = useQueryClient()
  const streamState = useSentinelStream(queryClient)

  const check = useCallback(() => {
    getSession()
      .then((s) => setAuthenticated(s.authenticated))
      .catch(() => setAuthenticated(false))
  }, [])

  useEffect(check, [check])

  if (authenticated === null) {
    return (
      <main className="centered">
        <p className="sub">Loading…</p>
      </main>
    )
  }
  if (!authenticated) {
    return <LoginForm onSuccess={() => setAuthenticated(true)} />
  }

  return (
    <Routes>
      <Route
        element={
          <Layout
            streamState={streamState}
            onSignOut={() => {
              void logout().then(() => setAuthenticated(false))
            }}
          />
        }
      >
        <Route index element={<OverviewView />} />
        <Route path="incidents/:id" element={<IncidentView />} />
        <Route path="projects" element={<ProjectsView />} />
        <Route path="spend" element={<ComingSoon view="Spend" milestone="M2" />} />
        <Route path="parked" element={<ComingSoon view="Parked" milestone="M2" />} />
        <Route path="audit" element={<ComingSoon view="Audit" milestone="M5" />} />
        <Route path="*" element={<ComingSoon view="Not found" milestone="" />} />
      </Route>
    </Routes>
  )
}
```

- [ ] **Step 6: Verify the typecheck fails for the right reason**

Run: `cd web && npx tsc --noEmit`

Expected: FAIL — the four `./views/*` modules do not exist yet. That is Task 19. Any *other* error is a real problem to fix now.

- [ ] **Step 7: Commit**

```bash
git add web/package.json web/package-lock.json web/src
git commit -m "feat(web): router, query client, and the multiplexed SSE hook

Design decision 2.4: the SPEC 9 shell lands now so M2's Spend and Parked
views are additive rather than a rewrite, and so the 'SSE patches the
cache, resync invalidates it' contract is proven while there is exactly
one topic to prove it against.

The resync handling is the part that matters. bus.Hub drops a slow
client's buffer and sends a single resync; the only correct response is
to invalidate and refetch over HTTP. Reconstructing state from a stream
whose events are already gone would leave the UI quietly wrong.

staleTime is generous on purpose: the stream is what makes the UI live,
so background polling would be redundant work on a memory-constrained
host.

Views land in the next commit; this one leaves the typecheck red on four
missing modules by design.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 19: `web` — Overview, Incident, Projects, and stubs

**Files:**
- Create: `web/src/views/overview.tsx`, `web/src/views/incident.tsx`, `web/src/views/projects.tsx`, `web/src/views/stub.tsx`
- Modify: `web/src/styles.css`

**Interfaces:**
- Consumes: `lib/api.ts`, `layout.tsx` (Task 18).
- Produces: the four view components the router mounts.

**Two things the Overview must say honestly.** `awaiting_triage` counts incidents resting in `triaging` — M1 has no Tier 1, so the label must not imply a queue is being worked. And `ingest_stale` must be visible, because a stalled subscriber is the most likely silent failure in the system (SPEC §12).

**The Incident view must show the grouping evidence.** `fingerprint_strategy` and `fingerprint_frames` are what make M1 a useful $0.00 observation period: seeing that a project fell back to `all_frames` is the signal to declare `source_roots` before M2 starts spending against these groupings.

- [ ] **Step 1: Write the stub view**

Create `web/src/views/stub.tsx`:

```tsx
export function ComingSoon({ view, milestone }: { view: string; milestone: string }) {
  return (
    <section className="empty">
      <h2>{view}</h2>
      <p className="sub">
        {milestone ? `This view arrives in ${milestone}.` : 'Nothing here.'}
      </p>
    </section>
  )
}
```

- [ ] **Step 2: Write the Overview**

Create `web/src/views/overview.tsx`:

```tsx
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
          <StateTile key={state} label={state} value={counts[state]} />
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
```

- [ ] **Step 3: Write the Incident view**

Create `web/src/views/incident.tsx`:

```tsx
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
```

- [ ] **Step 4: Write the Projects view**

Create `web/src/views/projects.tsx`:

```tsx
import { useQuery } from '@tanstack/react-query'
import { getProjects } from '../lib/api'

export function ProjectsView() {
  const query = useQuery({ queryKey: ['projects'], queryFn: getProjects })

  if (query.isPending) return <p className="sub">Loading…</p>
  if (query.isError) return <p className="error">{(query.error as Error).message}</p>

  return (
    <section>
      <h2>Projects</h2>
      <table className="grid">
        <thead>
          <tr>
            <th scope="col">Slug</th>
            <th scope="col">Repository</th>
            <th scope="col">Branch</th>
            <th scope="col">Incidents</th>
            <th scope="col">Status</th>
          </tr>
        </thead>
        <tbody>
          {query.data.projects.map((project) => (
            <tr key={project.slug} className={project.active ? undefined : 'inactive'}>
              <td>{project.slug}</td>
              <td>{project.repo}</td>
              <td>{project.default_branch}</td>
              <td>{project.incidents}</td>
              <td>
                {project.quarantined ? (
                  <span className="bad" title={project.quarantine_reason}>
                    quarantined
                  </span>
                ) : project.active ? (
                  <span className="ok">active</span>
                ) : (
                  <span className="sub">deregistered</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="sub">
        A deregistered project keeps its incident history; removing it from{' '}
        <code>projects.yaml</code> never deletes rows.
      </p>
    </section>
  )
}
```

- [ ] **Step 5: Extend the stylesheet**

Append to `web/src/styles.css` the classes the views use — `.shell`, `.brand`, `.stream`, `.tiles`, `.tile`, `.feed`, `.row`, `.state`, `.banner`, `.note`, `.panel`, `.frames`, `.timeline`, `.kv`, `.grid`, `.empty`, `.centered`, `.back`, `.inactive`, and per-state colouring for `.state-filtered`, `.state-suppressed`, `.state-triaging`, `.state-received`. Follow the existing file's variables and spacing conventions rather than introducing a new scheme.

Requirements from SPEC §9 that these styles must satisfy: the feed is keyboard navigable (each row is already an `<a>` via `Link`), and `.stream-closed` is visually distinct so a dead stream is obvious at a glance.

- [ ] **Step 6: Typecheck and build**

Run:
```bash
cd web && npx tsc --noEmit && npm run build && cd ..
```

Expected: PASS, and `internal/webassets/dist` is populated.

- [ ] **Step 7: Confirm the Go build embeds the new bundle**

Run: `make build`

Expected: succeeds, producing `bin/sentinel`.

- [ ] **Step 8: Commit**

```bash
git add web/src internal/webassets/dist/.gitkeep
git commit -m "feat(web): overview feed, incident detail, projects, and stubs

The overview states plainly that incidents in triaging are resting rather
than queued — M1 has no Tier 1, and a label implying otherwise would
misrepresent what the system is doing. Ingest staleness gets a banner,
since a stalled subscriber is the most likely silent failure.

The incident view surfaces the fingerprint strategy and the frames that
produced it, and prompts for source_roots when a project fell back to
all_frames. That is what turns M1's \$0.00 observation period into
something actionable: tuning grouping from evidence before M2 starts
spending against it.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 20: `deploy` — the Cloud Run relay and GCP provisioning scripts

**Files:**
- Create: `deploy/relay/go.mod`, `deploy/relay/main.go`, `deploy/relay/main_test.go`, `deploy/relay/Dockerfile`, `deploy/relay/deploy.sh`, `deploy/relay/README.md`
- Create: `deploy/gcp/topic.sh`, `deploy/gcp/subscription.sh`, `deploy/gcp/sink.sh`, `deploy/gcp/README.md`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: nothing from the root module.
- Produces: a deployable relay and three provisioning scripts.

**The relay is a separate Go module.** It publishes to Pub/Sub and therefore wants the Google client libraries. Keeping it out of the root module is what preserves design §2.1's result for the binary that actually runs on the Mac Mini — `go build ./...` at the repository root must never pull gRPC.

**Sinks are created by scripts, never by the binary** (SPEC §4.2.2), so the sentinel needs no GCP admin credentials.

- [ ] **Step 1: Create the module and write the failing test**

Run:
```bash
mkdir -p deploy/relay
cd deploy/relay && go mod init github.com/b1codes/triage-sentinel-relay && cd ../..
```

Create `deploy/relay/main_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
)

type capturedPublish struct {
	data       []byte
	attributes map[string]string
}

type fakePublisher struct {
	published []capturedPublish
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, data []byte, attrs map[string]string) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, capturedPublish{data: data, attributes: attrs})
	return nil
}

const secret = "it-is-a-secret-to-everybody"

func signature(body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestRelayRejectsBadSignatures(t *testing.T) {
	body := []byte(`{"action":"completed"}`)

	tests := []struct {
		name string
		sig  string
	}{
		{name: "wrong secret", sig: "sha256=" + hex.EncodeToString([]byte("nope"))},
		{name: "missing header", sig: ""},
		{name: "no prefix", sig: "abcdef"},
		{name: "not hex", sig: "sha256=zzzz"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &fakePublisher{}
			handler := newHandler(secret, publisher)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("X-GitHub-Event", "workflow_run")
			if tc.sig != "" {
				req.Header.Set("X-Hub-Signature-256", tc.sig)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
			if len(publisher.published) != 0 {
				t.Error("a message was published despite a bad signature; the relay would be an open publish endpoint")
			}
		})
	}
}

func TestRelayPublishesVerifiedDeliveries(t *testing.T) {
	body := []byte(`{"action":"completed"}`)
	publisher := &fakePublisher{}
	handler := newHandler(secret, publisher)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	req.Header.Set("X-Hub-Signature-256", signature(body))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(publisher.published))
	}

	got := publisher.published[0]
	if !bytes.Equal(got.data, body) {
		t.Errorf("published data = %q, want the raw body byte for byte", got.data)
	}

	t.Run("signature is forwarded so the control plane can re-verify", func(t *testing.T) {
		if got.attributes["x-hub-signature-256"] != signature(body) {
			t.Errorf("attributes = %v, want the forwarded signature", got.attributes)
		}
	})
	t.Run("event type is forwarded for adapter matching", func(t *testing.T) {
		if got.attributes["x-github-event"] != "workflow_run" {
			t.Errorf("attributes[x-github-event] = %q, want workflow_run", got.attributes["x-github-event"])
		}
	})
}

func TestRelayRejectsNonPost(t *testing.T) {
	handler := newHandler(secret, &fakePublisher{})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestRelayReportsPublishFailure(t *testing.T) {
	// A 5xx makes GitHub retry, which is what should happen when the message
	// never reached Pub/Sub.
	body := []byte(`{}`)
	handler := newHandler(secret, &fakePublisher{err: context.DeadlineExceeded})

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-Hub-Signature-256", signature(body))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 so GitHub retries", rec.Code)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd deploy/relay && go test ./... ; cd ../..`

Expected: FAIL — `undefined: newHandler`.

- [ ] **Step 3: Write the relay**

Create `deploy/relay/main.go`:

```go
// Command relay receives GitHub webhooks and republishes them to Pub/Sub.
//
// It exists because the sentinel host is behind NAT and cannot receive
// inbound webhooks (SPEC §1.3, §4.2.1). It is a separate Go module on
// purpose: it needs the Google client libraries, and keeping those out of the
// root module is what holds the sentinel binary to ~12 MB.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
)

// maxBody caps a webhook payload. GitHub's own limit is 25 MB.
const maxBody = 25 << 20

// forwardedHeaders are copied onto the Pub/Sub message as attributes. The
// signature is included so the control plane can independently re-verify it:
// a compromised relay must not be able to inject events (SPEC §4.2.1, §10).
var forwardedHeaders = []string{
	"X-GitHub-Event",
	"X-GitHub-Delivery",
	"X-GitHub-Hook-ID",
	"X-Hub-Signature-256",
}

// publisher publishes one message. The interface exists so the handler is
// testable without a Pub/Sub client.
type publisher interface {
	Publish(ctx context.Context, data []byte, attributes map[string]string) error
}

type pubsubPublisher struct {
	client *pubsub.Client
	topic  string
}

func (p *pubsubPublisher) Publish(ctx context.Context, data []byte, attributes map[string]string) error {
	result := p.client.Publisher(p.topic).Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	})
	if _, err := result.Get(ctx); err != nil {
		return fmt.Errorf("publishing to %s: %w", p.topic, err)
	}
	return nil
}

// newHandler builds the webhook handler.
//
// Verification happens before anything else. Without it the relay would be an
// open publish endpoint: anyone who found the Cloud Run URL could inject
// arbitrary events into the sentinel's ingestion path.
func newHandler(secret string, pub publisher) http.Handler {
	key := []byte(secret)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}

		if err := verify(key, r.Header.Get("X-Hub-Signature-256"), body); err != nil {
			slog.Warn("rejected delivery", "error", err,
				"delivery", r.Header.Get("X-GitHub-Delivery"))
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}

		attributes := make(map[string]string, len(forwardedHeaders))
		for _, header := range forwardedHeaders {
			if value := r.Header.Get(header); value != "" {
				attributes[strings.ToLower(header)] = value
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()

		if err := pub.Publish(ctx, body, attributes); err != nil {
			slog.Error("publishing delivery", "error", err,
				"delivery", r.Header.Get("X-GitHub-Delivery"))
			// A 5xx makes GitHub retry, which is correct: the message never
			// reached Pub/Sub, so dropping it would lose the event.
			http.Error(w, "publish failed", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
}

// verify checks the HMAC in constant time. Comparing hex strings with ==
// would leak timing information about the expected digest.
func verify(key []byte, header string, body []byte) error {
	encoded, found := strings.CutPrefix(header, "sha256=")
	if !found {
		return errors.New("missing or malformed X-Hub-Signature-256")
	}
	provided, err := hex.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("signature is not hex: %w", err)
	}

	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return errors.New("digest mismatch")
	}
	return nil
}

func main() {
	secret := os.Getenv("GITHUB_WEBHOOK_SECRET")
	project := os.Getenv("GCP_PROJECT_ID")
	topic := os.Getenv("PUBSUB_TOPIC")

	if secret == "" || project == "" || topic == "" {
		slog.Error("GITHUB_WEBHOOK_SECRET, GCP_PROJECT_ID and PUBSUB_TOPIC are all required")
		os.Exit(1)
	}

	client, err := pubsub.NewClient(context.Background(), project)
	if err != nil {
		slog.Error("creating pubsub client", "error", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	handler := newHandler(secret, &pubsubPublisher{
		client: client,
		topic:  fmt.Sprintf("projects/%s/topics/%s", project, topic),
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	slog.Info("relay listening", "port", port, "topic", topic)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("serving", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 4: Add the dependency and run the tests**

Run:
```bash
cd deploy/relay
go get cloud.google.com/go/pubsub/v2@latest
go mod tidy
go test ./... -v
cd ../..
```

Expected: PASS.

**Verify the isolation held:**

```bash
go list -m all | wc -l
```

Expected: still ~35 at the repository root. If it jumped to ~200, the relay was not created as a separate module and design §2.1 has been silently reversed.

- [ ] **Step 5: Write the Dockerfile and deploy script**

Create `deploy/relay/Dockerfile`:

```dockerfile
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o /relay .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /relay /relay
USER nonroot:nonroot
ENTRYPOINT ["/relay"]
```

Create `deploy/relay/deploy.sh`:

```bash
#!/usr/bin/env bash
# Deploy the GitHub webhook relay to Cloud Run.
#
# The webhook secret is read from Secret Manager at runtime, never baked into
# the image and never passed as a plain environment variable.
set -euo pipefail

: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${PUBSUB_TOPIC:=sentinel-events}"
: "${REGION:=us-central1}"
: "${SERVICE:=sentinel-relay}"
: "${SECRET_NAME:=github-webhook-secret}"

cd "$(dirname "$0")"

gcloud run deploy "$SERVICE" \
  --project "$GCP_PROJECT_ID" \
  --region "$REGION" \
  --source . \
  --allow-unauthenticated \
  --set-env-vars "GCP_PROJECT_ID=${GCP_PROJECT_ID},PUBSUB_TOPIC=${PUBSUB_TOPIC}" \
  --set-secrets "GITHUB_WEBHOOK_SECRET=${SECRET_NAME}:latest" \
  --max-instances 3 \
  --memory 128Mi

echo
echo "Relay URL:"
gcloud run services describe "$SERVICE" \
  --project "$GCP_PROJECT_ID" --region "$REGION" \
  --format 'value(status.url)'
echo
echo "Add that URL as a webhook on each repository, content type application/json,"
echo "with the same secret, subscribed to: Workflow runs, Issues."
```

`--allow-unauthenticated` is required because GitHub cannot present a Google identity token. The HMAC check is what protects the endpoint, which is why Step 1's rejection tests are not optional.

- [ ] **Step 6: Write the provisioning scripts**

Create `deploy/gcp/topic.sh`:

```bash
#!/usr/bin/env bash
# Create the shared ingestion topic.
set -euo pipefail
: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${PUBSUB_TOPIC:=sentinel-events}"

gcloud pubsub topics create "$PUBSUB_TOPIC" --project "$GCP_PROJECT_ID" \
  || echo "topic $PUBSUB_TOPIC already exists"
```

Create `deploy/gcp/subscription.sh`:

```bash
#!/usr/bin/env bash
# Create the pull subscription the sentinel long-polls.
#
# ack-deadline is generous but not load-bearing: the sentinel acks immediately
# after the durable write (SPEC §4.2), so processing time never approaches it.
# message-retention is what actually matters — it is how long the host can be
# asleep or rebooting without losing events.
set -euo pipefail
: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${PUBSUB_TOPIC:=sentinel-events}"
: "${PUBSUB_SUBSCRIPTION:=sentinel-pull}"

gcloud pubsub subscriptions create "$PUBSUB_SUBSCRIPTION" \
  --project "$GCP_PROJECT_ID" \
  --topic "$PUBSUB_TOPIC" \
  --ack-deadline 60 \
  --message-retention-duration 7d \
  --expiration-period never \
  || echo "subscription $PUBSUB_SUBSCRIPTION already exists"

echo
echo "Set this in .env:"
echo "PUBSUB_SUBSCRIPTION=projects/${GCP_PROJECT_ID}/subscriptions/${PUBSUB_SUBSCRIPTION}"
```

Create `deploy/gcp/sink.sh`:

```bash
#!/usr/bin/env bash
# Create a per-project Cloud Logging sink routing errors to the shared topic.
#
# Usage: ./sink.sh <project-slug> '<log-filter>'
#
# Sinks are created here rather than by the binary, so the sentinel needs no
# GCP admin credentials (SPEC §4.2.2).
#
# Routing note: a logging sink cannot attach custom attributes to what it
# publishes, so the sentinel resolves the project from the log entry's resource
# labels — service_name, function_name, job, namespace_name — or an explicit
# project_slug label. The value must equal the registered slug. Entries that
# resolve to nothing are recorded as unroutable and shown in the dashboard
# rather than dropped, which is the signal that a name has drifted.
set -euo pipefail
: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${PUBSUB_TOPIC:=sentinel-events}"

SLUG="${1:?usage: sink.sh <project-slug> '<log-filter>'}"
FILTER="${2:?usage: sink.sh <project-slug> '<log-filter>'}"
SINK="sentinel-${SLUG}"

gcloud logging sinks create "$SINK" \
  "pubsub.googleapis.com/projects/${GCP_PROJECT_ID}/topics/${PUBSUB_TOPIC}" \
  --project "$GCP_PROJECT_ID" \
  --log-filter "$FILTER" \
  || gcloud logging sinks update "$SINK" \
       "pubsub.googleapis.com/projects/${GCP_PROJECT_ID}/topics/${PUBSUB_TOPIC}" \
       --project "$GCP_PROJECT_ID" --log-filter "$FILTER"

WRITER=$(gcloud logging sinks describe "$SINK" --project "$GCP_PROJECT_ID" \
  --format 'value(writerIdentity)')

gcloud pubsub topics add-iam-policy-binding "$PUBSUB_TOPIC" \
  --project "$GCP_PROJECT_ID" \
  --member "$WRITER" \
  --role roles/pubsub.publisher

echo "sink $SINK created; writer $WRITER granted publisher on $PUBSUB_TOPIC"
```

Make all four executable: `chmod +x deploy/gcp/*.sh deploy/relay/deploy.sh`.

- [ ] **Step 7: Write the READMEs**

`deploy/gcp/README.md` documents the provisioning order (topic → subscription → sinks), the service-account key the sentinel needs (`roles/pubsub.subscriber` only), and the label-naming convention that makes routing work. `deploy/relay/README.md` documents deploying, creating the Secret Manager secret, and registering the webhook — and states explicitly that this is a separate Go module so the root module never gains gRPC.

- [ ] **Step 8: Confirm root-module isolation in CI terms**

Run:
```bash
go build ./... && go vet ./...
```

Expected: PASS, and neither command descends into `deploy/relay` — a nested module is excluded from the parent's `./...`. If `go vet` reports errors from relay files, the nested `go.mod` is missing.

- [ ] **Step 9: Run the gate and commit**

Run: `make check`

```bash
chmod +x deploy/gcp/*.sh deploy/relay/deploy.sh
git add deploy .gitignore
git commit -m "feat(deploy): Cloud Run relay and GCP provisioning scripts

The relay is a separate Go module. It needs the Google client libraries
to publish, and keeping those out of the root module is what preserves
the ~12 MB sentinel binary that design 2.1 argued for — go build ./... at
the root must never pull gRPC.

Verification happens before anything else in the handler. Without it the
relay would be an open publish endpoint: anyone who found the Cloud Run
URL could inject arbitrary events into ingestion. --allow-unauthenticated
is unavoidable because GitHub cannot present a Google identity token, so
the HMAC check is the only thing protecting it, which is why the
rejection tests are not optional. A publish failure returns 5xx so GitHub
retries rather than the event being lost.

Sinks are created by scripts, never by the binary, so the sentinel needs
no GCP admin credentials — only roles/pubsub.subscriber.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 21: End-to-end storm test, live verification, and SPEC amendments

**Files:**
- Create: `internal/orchestrator/e2e_test.go`
- Modify: `docs/SPEC.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: every earlier task.
- Produces: the milestone's acceptance evidence and a spec that matches what was built.

**The storm test is the load-bearing one.** It is the difference between fingerprint suppression working and an error storm becoming an invoice in M2. A crash loop emits thousands of unique `insertId`s, so `(source, source_ref)` deduplication cannot help — every entry genuinely is distinct.

- [ ] **Step 1: Write the end-to-end test**

Create `internal/orchestrator/e2e_test.go`:

```go
package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/ingest"
	"github.com/b1codes/triage-sentinel/internal/store"
)

// logEntry builds a Cloud Logging entry for one occurrence of a crash loop.
// Every entry has a distinct insertId — which is exactly why source_ref
// deduplication cannot collapse a storm, and fingerprinting must.
func logEntry(t *testing.T, insertID string, lineNumber int) []byte {
	t.Helper()

	payload := map[string]any{
		"insertId":  insertID,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"severity":  "ERROR",
		"resource": map[string]any{
			"type":   "cloud_run_revision",
			"labels": map[string]string{"service_name": "api"},
		},
		"textPayload": fmt.Sprintf(
			"TypeError: Cannot read properties of undefined\n"+
				"    at handler (/app/src/index.js:12:%d)\n"+
				"    at Layer.handle (/app/node_modules/express/lib/router/layer.js:95:5)",
			lineNumber),
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encoding log entry: %v", err)
	}
	return encoded
}

// TestStormCollapsesToOneIncident is the milestone's acceptance test.
//
// 500 log entries with unique insertIds and one shared root cause must produce
// exactly one incident in triaging, with every other occurrence suppressed and
// the lifetime count preserved. If this fails, M2 pays for every occurrence of
// every crash loop.
func TestStormCollapsesToOneIncident(t *testing.T) {
	o, db, _, ctx, _ := fixture(t)

	resolver := ingest.NewRegistryResolver(registryForE2E(t))
	router := ingest.NewRouter(ingest.NewGCPLogAdapter(resolver))
	writer := ingest.NewIncidentWriter(db, time.Now)

	const storm = 500
	for i := range storm {
		message := ingest.Message{
			ID:          fmt.Sprintf("m-%d", i),
			Data:        logEntry(t, fmt.Sprintf("insert-%d", i), i),
			Attributes:  map[string]string{"logging.googleapis.com/timestamp": "x"},
			PublishTime: time.Now(),
		}

		event, err := router.Route(ctx, message)
		if err != nil {
			t.Fatalf("routing message %d: %v", i, err)
		}
		if err := writer.Handle(ctx, event); err != nil {
			t.Fatalf("persisting message %d: %v", i, err)
		}
	}

	// Drain the whole queue.
	for {
		moved, err := o.ProcessOnce(ctx)
		if err != nil {
			t.Fatalf("ProcessOnce() error = %v", err)
		}
		if moved == 0 {
			break
		}
	}

	counts, err := store.CountByState(ctx, db)
	if err != nil {
		t.Fatalf("CountByState() error = %v", err)
	}

	t.Run("exactly one incident is actionable", func(t *testing.T) {
		if counts["triaging"] != 1 {
			t.Errorf("triaging = %d, want 1; a storm must collapse to a single incident", counts["triaging"])
		}
	})

	t.Run("the rest are suppressed, not lost", func(t *testing.T) {
		if counts["suppressed"] != storm-1 {
			t.Errorf("suppressed = %d, want %d", counts["suppressed"], storm-1)
		}
	})

	t.Run("nothing is left queued", func(t *testing.T) {
		if counts["received"] != 0 {
			t.Errorf("received = %d, want 0", counts["received"])
		}
	})

	t.Run("the storm stays visible while being silent", func(t *testing.T) {
		incidents, _, err := store.ListIncidents(ctx, db, store.IncidentFilter{
			States: []string{"triaging"}, Limit: 1,
		})
		if err != nil || len(incidents) != 1 {
			t.Fatalf("ListIncidents() = %v, %v", len(incidents), err)
		}

		record, ok, err := store.GetFingerprint(ctx, db, incidents[0].Fingerprint)
		if err != nil || !ok {
			t.Fatalf("GetFingerprint() = %v, %v", ok, err)
		}
		if record.TotalOccurrences != storm {
			t.Errorf("TotalOccurrences = %d, want %d", record.TotalOccurrences, storm)
		}
		if record.Strategy != "denylist" {
			t.Errorf("Strategy = %q, want denylist; the app frame should have been selected over the express frame", record.Strategy)
		}
	})
}

// TestDistinctBugsDoNotCollapse is the counterweight. Suppression that is too
// aggressive silently swallows real failures, and nothing else in the system
// catches that.
func TestDistinctBugsDoNotCollapse(t *testing.T) {
	o, db, _, ctx, _ := fixture(t)

	resolver := ingest.NewRegistryResolver(registryForE2E(t))
	router := ingest.NewRouter(ingest.NewGCPLogAdapter(resolver))
	writer := ingest.NewIncidentWriter(db, time.Now)

	bodies := []string{
		"TypeError: Cannot read properties of undefined\n    at handler (/app/src/index.js:12:9)",
		"RangeError: Maximum call stack size exceeded\n    at recurse (/app/src/tree.js:88:3)",
	}

	for i, body := range bodies {
		payload, err := json.Marshal(map[string]any{
			"insertId":  fmt.Sprintf("distinct-%d", i),
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"severity":  "ERROR",
			"resource": map[string]any{
				"labels": map[string]string{"service_name": "api"},
			},
			"textPayload": body,
		})
		if err != nil {
			t.Fatalf("encoding: %v", err)
		}

		event, err := router.Route(ctx, ingest.Message{
			ID: fmt.Sprintf("m-%d", i), Data: payload,
			Attributes: map[string]string{"logging.googleapis.com/timestamp": "x"},
		})
		if err != nil {
			t.Fatalf("routing: %v", err)
		}
		if err := writer.Handle(ctx, event); err != nil {
			t.Fatalf("persisting: %v", err)
		}
	}

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	counts, err := store.CountByState(ctx, db)
	if err != nil {
		t.Fatalf("CountByState() error = %v", err)
	}
	if counts["triaging"] != 2 {
		t.Errorf("triaging = %d, want 2; two distinct bugs were collapsed into one and the second was silently suppressed", counts["triaging"])
	}
}

// TestRestartResumesTheQueue proves crash recovery needs no mechanism: the
// queue is rows, so a new orchestrator picks up exactly where the old one
// stopped (SPEC §4.12).
func TestRestartResumesTheQueue(t *testing.T) {
	o, db, hub, ctx, now := fixture(t)

	for i := range 3 {
		seed(t, db, ctx, now, store.IngestParams{
			ProjectSlug: "api",
			SourceRef:   fmt.Sprintf("resume-%d", i),
			Title:       fmt.Sprintf("TypeError: distinct %d", i),
			Body:        fmt.Sprintf("at handler (src/file%d.js:1)", i),
		})
	}

	// Process with a batch size of one, then discard the orchestrator.
	small, err := New(Deps{
		DB: db, Hub: hub, Chain: o.deps.Chain,
		Registry: o.deps.Registry, Logger: o.deps.Logger,
		Clock: o.deps.Clock, BatchSize: 1,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := small.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	// A fresh orchestrator drains the remainder.
	for {
		moved, err := o.ProcessOnce(ctx)
		if err != nil {
			t.Fatalf("ProcessOnce() error = %v", err)
		}
		if moved == 0 {
			break
		}
	}

	counts, err := store.CountByState(ctx, db)
	if err != nil {
		t.Fatalf("CountByState() error = %v", err)
	}
	if counts["received"] != 0 {
		t.Errorf("received = %d, want 0; a restart must resume the queue", counts["received"])
	}
}
```

Add the `registryForE2E` helper alongside `fixture` in `orchestrator_test.go`, returning the same `config.Registry` that `fixture` builds so both agree on the registered project.

- [ ] **Step 2: Run the acceptance tests**

Run: `go test -race ./internal/orchestrator/ -run 'TestStorm|TestDistinctBugs|TestRestartResumes' -v`

Expected: PASS. These four subtests plus the two companions are the milestone's acceptance evidence.

- [ ] **Step 3: Run the whole suite and the full gate**

Run:
```bash
make check
cd deploy/relay && go test ./... && cd ../..
cd web && npx tsc --noEmit && cd ..
```

Expected: all PASS.

- [ ] **Step 4: Verify against real infrastructure**

This is the milestone's stated done-when criterion, and it cannot be satisfied by tests alone.

```bash
# 1. Provision.
export GCP_PROJECT_ID=<your project>
./deploy/gcp/topic.sh
./deploy/gcp/subscription.sh

# 2. Deploy the relay and register the webhook on one repository,
#    subscribed to Workflow runs and Issues.
./deploy/relay/deploy.sh

# 3. Create a sink for one project.
./deploy/gcp/sink.sh <slug> 'severity>=ERROR AND resource.labels.service_name="<slug>"'

# 4. Fill in .env, then start.
make build && ./bin/sentinel serve
```

Then confirm each of these, and record the observed latency in the commit message:

- [ ] Push a commit that fails CI on a registered repository. The failure appears in the dashboard feed within seconds, with `fingerprint_strategy: workflow` and the failing job and step as its frames.
- [ ] Open an issue with a subscribed label. It appears with `fingerprint` empty — issues are not storms and are deduplicated by `source_ref` alone.
- [ ] Trigger an error in a project with a sink. It appears with a stack-derived fingerprint.
- [ ] Trigger the same error repeatedly. `occurrence_count` climbs while the incident count does not.
- [ ] Stop the binary, generate an event, restart. The event arrives after startup — Pub/Sub retained it, which is the whole reason ingestion is a durable queue rather than a webhook.
- [ ] `curl -s localhost:8787/api/health | jq '.last_ingest_at, .ingest_stale'` reports a fresh timestamp.

- [ ] **Step 5: Amend `docs/SPEC.md`**

Five amendments, each correcting the spec to match what was built. Mirror how M0's final task corrected §13.

**§4.2** — replace "A streaming-pull Pub/Sub subscriber plus one adapter per source" with a description of the REST long-poll subscriber, and add:

> **Transport.** v1 pulls over the Pub/Sub REST API rather than gRPC StreamingPull. Measured at M1: `cloud.google.com/go/pubsub/v2` adds ~14 MB to the binary and takes the module graph from 32 to ~200, against the 15–25 MB RSS target this architecture exists to satisfy (§1.3). The ack-after-durable-write rule below removes the main reason to prefer the official client, because ack-deadline extension — the hardest part of a hand-rolled loop — is never needed when messages are acked within milliseconds of receipt. `Puller` is an interface, so a gRPC implementation remains a drop-in if volume ever justifies it.

**§4.3.1** — in the filter table, mark `BuildSanity` as *"lands in M3; ships as a registered no-op in M1, holding its chain position"*, and replace the `Unroutable` and `Duplicate` rows with a note:

> `Unroutable` and `Duplicate` are enforced at the **write boundary** rather than as chain members: by routing, and by the `incidents(source, source_ref)` unique index respectively. Both facts are established by the insert itself, so a chain member would re-query for what is already known and would race a concurrent duplicate delivery.

**§4.3.2** — replace the single sentence about frames "within the project's own source tree", which is not implementable without a checkout, with the ladder:

> Frame selection runs three steps and records which produced the result:
> 1. `source_roots` — the project declares them in the registry. Closed set, preferred.
> 2. `denylist` — no roots declared; frames outside known dependency directories. Open-ended, best effort.
> 3. `all_frames` — steps 1–2 selected nothing; use the top N frames including dependencies.
>
> Step 3 is not a fallback of convenience. The two failure modes are asymmetric: over-collapse silently suppresses a real failure for a whole window with **no backstop**, while under-collapse merely costs money that every budget ceiling bounds. An empty frame set hashes to `sha256(slug, class, "")` and collapses every same-class failure in a project — the unbacked mode, reached by the most common path. **When frame classification is uncertain, fingerprint more specifically, never less.**
>
> Fingerprint inputs differ by source. `log.error` uses stack frames; `workflow_run.failed` uses the failing **job and step** names, because grouping on the workflow name alone would collapse every failure of one workflow into a single incident; `issues.*` are not fingerprinted at all, since human-filed issues are not storms and `source_ref` already deduplicates them.

**§5** — note that the schema is now two migrations, and add the two columns to the `fingerprints` DDL:

```sql
  strategy          TEXT NOT NULL DEFAULT 'unknown',
  frames_json       TEXT NOT NULL DEFAULT '[]',
```

**§6.2** — add the `bot` and `triage` blocks and the per-project `fingerprint` block to the example registry.

Also update **§14 M1** to record that `GITHUB_TOKEN` (read scope) is required from M1 rather than M4.

- [ ] **Step 6: Update the README**

Replace the placeholder `README.md` with: what the project is, the M1 capability (a live NOC for 25 repositories at no marginal cost), a quick start (`make build`, `.env` and `projects.yaml` from their examples, `sentinel hash-password`, the three `deploy/gcp` scripts, `deploy/relay/deploy.sh`), the `--no-ingest` and `sentinel replay` development paths, and a milestone table marking M0 and M1 done.

State plainly that incidents currently rest in `triaging` because Tier 1 arrives in M2 — the README should not imply the system triages yet.

- [ ] **Step 7: Final gate and commit**

Run: `make check`

```bash
git add internal/orchestrator/e2e_test.go docs/SPEC.md README.md
git commit -m "test(m1): storm acceptance tests and SPEC amendments

The storm test is the milestone's acceptance evidence: 500 log entries
with unique insertIds and one root cause collapse to exactly one incident
in triaging, 499 suppressed, lifetime count preserved. source_ref
deduplication cannot help there — every entry genuinely is distinct — so
this is the difference between suppression working and M2 paying for
every occurrence of every crash loop.

Its counterweight matters as much: two distinct bugs must produce two
incidents. Suppression that is too aggressive silently swallows real
failures, and nothing else in the system catches that.

Five SPEC amendments, correcting the spec to what was built:
- 4.2  REST long-poll replaces streaming pull, with the measurement
- 4.3.1 BuildSanity lands in M3; Unroutable and Duplicate are
        write-boundary enforcement, not chain members
- 4.3.2 the frame-selection ladder, its never-broaden rule, and
        per-source fingerprint inputs
- 5    two migrations; fingerprints gains strategy and frames_json
- 6.2  the bot, triage, and per-project fingerprint blocks
- 14   GITHUB_TOKEN (read scope) is required from M1, not M4

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Self-Review

Checked against `docs/superpowers/specs/2026-08-02-m1-ingestion-design.md`.

**Spec coverage**

| Design section | Task |
|---|---|
| §2.1 REST transport | 13 (measurement re-verified in step 6) |
| §2.2 Real infrastructure, relay as separate module | 20 |
| §2.3 `BuildSanity` deferred with seam | 9 |
| §2.4 §9 frontend shell | 18, 19 |
| §3.1 Two loops | 14 (pull), 15 (process) |
| §3.2 Write-boundary `Unroutable`/`Duplicate` | 4, 9 |
| §3.3 Rest in `triaging` | 15, 19 |
| §4 Package layout | 3–16 |
| §4.1 `Puller` seam | 13 |
| §4.2 REST details + verification flag | 13 |
| §4.3 Projects sync | 3, 17 |
| §4.4 Fingerprint ladder | 8 |
| §4.4.2 Per-source inputs | 8, 11, 15 |
| §4.4.3 Recorded evidence | 2, 6, 19 |
| §5 Config additions | 1 |
| §5.1 Environment assertions | 17 |
| §6 HTTP API (read-only) | 16 |
| §6.1 SSE replay | 16 |
| §7 Frontend | 18, 19 |
| §8 Deploy artifacts | 20 |
| §9 Failure handling | 11 (HMAC), 14 (every branch) |
| §10 Testing | throughout; acceptance in 21 |
| §12 Spec amendments | 21 |

No gaps.

**Type consistency spot-checks**

- `store.IncidentEvent.Payload` is `json.RawMessage` in Task 5 and consumed as such in Tasks 15 and 16.
- `triage.Strategy` values match the `strategy` column's documented set in Task 2 and the strings written in Task 15.
- `ingest.Event.JobSteps` is produced in Task 11, persisted in Task 15 step 1, and read back in Task 15's `fingerprint`.
- `httpapi.ReplayFunc` matches M0's declaration at `server.go:32` exactly.
- `bus.Event.ID` carries `incident_events.id` in Tasks 15 and 16, as M0's doc comment reserved.

**Placeholder scan:** clean. Task 16's test helpers were initially elided; they are now written against the M0 helpers they build on (`newTestServer`, `login`, `sessionCookie`, `hashFor`, `testPassword`), verified to exist in `internal/httpapi/health_test.go:21` and `internal/httpapi/auth_test.go:39`.

**One thing the implementer must not skip:** Task 13 step 1 and step 6 both check the module count and binary size. Those checks are the only thing standing between this milestone and a silent reversal of design decision §2.1 — a stray transitive dependency on the Google API stack would undo the measurement the transport choice rests on, and nothing else would notice.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-02-m1-ingestion.md`.

