package config

import (
	"errors"
	"strings"
	"testing"
)

// baseRegistry returns a registry that passes validation. Each test mutates
// one thing so a failure names exactly one broken rule.
func baseRegistry() Registry {
	soft7, hard10 := 7.0, 10.0
	hard2 := 2.0
	soft35, hard50 := 35.0, 50.0
	soft110, hard150 := 110.0, 150.0

	return Registry{
		Version: 1,
		Budgets: Budgets{
			PerIncidentUSD: 1.00,
			Daily: DailyBudget{
				Global:     Threshold{Soft: &soft7, Hard: &hard10},
				PerProject: Threshold{Hard: &hard2},
			},
			Weekly:       WindowBudget{Global: Threshold{Soft: &soft35, Hard: &hard50}},
			Monthly:      WindowBudget{Global: Threshold{Soft: &soft110, Hard: &hard150}},
			WeekStartsOn: "monday",
			Efficiency: Efficiency{
				Enabled:                      true,
				Window:                       "week",
				MaxCostPerResolutionMultiple: 3.0,
				MinResolutionsForSignal:      5,
			},
			Forecast: Forecast{Enabled: true, WarnAtFractionOfHard: 0.9},
		},
		SoftMode: SoftMode{ParkTier2: true},
		Defaults: ProjectDefaults{
			Autonomy:                        "pr_only",
			Tier1Model:                      "claude-haiku-4-5",
			Tier2Model:                      "claude-opus-5",
			Tier2Effort:                     "high",
			MaxTurns:                        40,
			RunTimeout:                      Duration{20 * 60 * 1e9},
			SuppressionWindow:               Duration{6 * 60 * 60 * 1e9},
			MaxRepairAttemptsPerFingerprint: 2,
			MaxDiffLines:                    400,
			ProbationIncidents:              3,
			Commands: Commands{
				Test: "make test", Build: "make build", Healthcheck: "make healthcheck",
			},
		},
		Runtime: Runtime{
			MaxConcurrentAgents: 1,
			MinFreeRAMMB:        2048,
			MinFreeDiskMB:       10240,
			Tier2MinConfidence:  0.5,
			MaxInputTokens:      40000,
		},
		Bot: Bot{Name: "triage-sentinel", Email: "sentinel@example.invalid"},
		Projects: []Project{{
			Slug:          "example-api",
			Repo:          "github.com/example/example-api",
			DefaultBranch: "main",
			Triggers:      Triggers{WorkflowRun: true},
		}},
	}
}

func TestValidateAcceptsBaseRegistry(t *testing.T) {
	if err := baseRegistry().Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestValidateRules(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*Registry)
		wantText string
	}{
		{
			name:     "wrong version",
			mutate:   func(r *Registry) { r.Version = 2 },
			wantText: "version",
		},
		{
			name:     "no projects",
			mutate:   func(r *Registry) { r.Projects = nil },
			wantText: "at least one project",
		},
		{
			name:     "slug too short",
			mutate:   func(r *Registry) { r.Projects[0].Slug = "ab" },
			wantText: "slug",
		},
		{
			name:     "slug uppercase",
			mutate:   func(r *Registry) { r.Projects[0].Slug = "Example-API" },
			wantText: "slug",
		},
		{
			name:     "slug trailing hyphen",
			mutate:   func(r *Registry) { r.Projects[0].Slug = "example-" },
			wantText: "slug",
		},
		{
			name: "duplicate slug",
			mutate: func(r *Registry) {
				r.Projects = append(r.Projects, r.Projects[0])
			},
			wantText: "duplicate",
		},
		{
			name:     "repo not a github path",
			mutate:   func(r *Registry) { r.Projects[0].Repo = "gitlab.com/example/x" },
			wantText: "repo",
		},
		{
			name:     "repo missing name",
			mutate:   func(r *Registry) { r.Projects[0].Repo = "github.com/example" },
			wantText: "repo",
		},
		{
			name:     "empty default branch",
			mutate:   func(r *Registry) { r.Projects[0].DefaultBranch = "" },
			wantText: "default_branch",
		},
		{
			name: "no effective test command",
			mutate: func(r *Registry) {
				r.Defaults.Commands.Test = ""
				r.Projects[0].Commands.Test = ""
			},
			wantText: "commands.test",
		},
		{
			name:     "unknown autonomy",
			mutate:   func(r *Registry) { r.Projects[0].Autonomy = "yolo" },
			wantText: "autonomy",
		},
		{
			name: "auto_deploy without deploy command",
			mutate: func(r *Registry) {
				r.Projects[0].Autonomy = "auto_deploy"
			},
			wantText: "commands.deploy",
		},
		{
			name:     "unknown tier1 model",
			mutate:   func(r *Registry) { r.Defaults.Tier1Model = "claude-imaginary-9" },
			wantText: "claude-imaginary-9",
		},
		{
			name:     "unknown tier2 model override",
			mutate:   func(r *Registry) { r.Projects[0].Tier2Model = "gpt-hal-9000" },
			wantText: "gpt-hal-9000",
		},
		{
			name:     "unknown effort",
			mutate:   func(r *Registry) { r.Defaults.Tier2Effort = "maximum overdrive" },
			wantText: "tier2_effort",
		},
		{
			name: "soft not below hard",
			mutate: func(r *Registry) {
				v := 60.0
				r.Budgets.Weekly.Global.Soft = &v
			},
			wantText: "soft",
		},
		{
			name: "negative threshold",
			mutate: func(r *Registry) {
				v := -1.0
				r.Budgets.Monthly.Global.Hard = &v
			},
			wantText: "monthly",
		},
		{
			name:     "zero per-incident budget",
			mutate:   func(r *Registry) { r.Budgets.PerIncidentUSD = 0 },
			wantText: "per_incident_usd",
		},
		{
			name:     "unknown week start",
			mutate:   func(r *Registry) { r.Budgets.WeekStartsOn = "caturday" },
			wantText: "week_starts_on",
		},
		{
			name:     "unknown efficiency window",
			mutate:   func(r *Registry) { r.Budgets.Efficiency.Window = "fortnight" },
			wantText: "efficiency.window",
		},
		{
			name:     "forecast fraction out of range",
			mutate:   func(r *Registry) { r.Budgets.Forecast.WarnAtFractionOfHard = 1.5 },
			wantText: "warn_at_fraction_of_hard",
		},
		{
			name:     "zero concurrency",
			mutate:   func(r *Registry) { r.Runtime.MaxConcurrentAgents = 0 },
			wantText: "max_concurrent_agents",
		},
		{
			name:     "confidence out of range",
			mutate:   func(r *Registry) { r.Runtime.Tier2MinConfidence = 1.4 },
			wantText: "tier2_min_confidence",
		},
		{
			name:     "zero run timeout",
			mutate:   func(r *Registry) { r.Defaults.RunTimeout = Duration{} },
			wantText: "run_timeout",
		},
		{
			name: "soft mode with no action does nothing",
			mutate: func(r *Registry) {
				r.SoftMode = SoftMode{}
			},
			wantText: "soft_mode",
		},
		{
			name:     "weekly hard cap missing",
			mutate:   func(r *Registry) { r.Budgets.Weekly.Global.Hard = nil },
			wantText: "budgets.weekly.global.hard is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := baseRegistry()
			tc.mutate(&reg)

			err := reg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
			if !errors.Is(err, ErrInvalidRegistry) {
				t.Errorf("errors.Is(err, ErrInvalidRegistry) = false, want true")
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.wantText)
			}
		})
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	reg := baseRegistry()
	reg.Version = 9
	reg.Projects[0].Slug = "BAD"
	reg.Runtime.MaxConcurrentAgents = 0

	err := reg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
	for _, want := range []string{"version", "slug", "max_concurrent_agents"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q; all problems must be reported at once", err.Error(), want)
		}
	}
}

func TestLoadRegistryValidatesTestdata(t *testing.T) {
	reg, err := LoadRegistry("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("LoadRegistry() error = %v, want nil", err)
	}
	if len(reg.Projects) != 2 {
		t.Errorf("len(Projects) = %d, want 2", len(reg.Projects))
	}
}

func TestLoadRegistryMissingFile(t *testing.T) {
	_, err := LoadRegistry("testdata/does-not-exist.yaml")
	if err == nil {
		t.Fatal("LoadRegistry() error = nil, want error")
	}
	if !errors.Is(err, ErrInvalidRegistry) {
		t.Errorf("errors.Is(err, ErrInvalidRegistry) = false, want true")
	}
}

func TestLoadRegistryValidatesExampleFile(t *testing.T) {
	// projects.example.yaml is committed and must always be valid, or a new
	// operator's first run fails.
	if _, err := LoadRegistry("../../projects.example.yaml"); err != nil {
		t.Fatalf("projects.example.yaml is invalid: %v", err)
	}
}

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
