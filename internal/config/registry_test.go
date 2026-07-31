package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading testdata/%s: %v", name, err)
	}
	return data
}

func TestParseRegistryValid(t *testing.T) {
	reg, err := ParseRegistry(readTestdata(t, "valid.yaml"))
	if err != nil {
		t.Fatalf("ParseRegistry() error = %v, want nil", err)
	}

	if reg.Version != 1 {
		t.Errorf("Version = %d, want 1", reg.Version)
	}
	if len(reg.Projects) != 2 {
		t.Fatalf("len(Projects) = %d, want 2", len(reg.Projects))
	}

	t.Run("durations decode from strings", func(t *testing.T) {
		if got, want := reg.Defaults.RunTimeout.Duration, 20*time.Minute; got != want {
			t.Errorf("Defaults.RunTimeout = %v, want %v", got, want)
		}
		if got, want := reg.Defaults.SuppressionWindow.Duration, 6*time.Hour; got != want {
			t.Errorf("Defaults.SuppressionWindow = %v, want %v", got, want)
		}
		if reg.Projects[1].SuppressionWindow == nil {
			t.Fatal("Projects[1].SuppressionWindow = nil, want 12h")
		}
		if got, want := reg.Projects[1].SuppressionWindow.Duration, 12*time.Hour; got != want {
			t.Errorf("Projects[1].SuppressionWindow = %v, want %v", got, want)
		}
	})

	t.Run("absent threshold is nil not zero", func(t *testing.T) {
		if reg.Budgets.Daily.PerProject.Soft != nil {
			t.Errorf("Daily.PerProject.Soft = %v, want nil", *reg.Budgets.Daily.PerProject.Soft)
		}
		if reg.Budgets.Daily.PerProject.Hard == nil {
			t.Fatal("Daily.PerProject.Hard = nil, want 2")
		}
		if got := *reg.Budgets.Daily.PerProject.Hard; got != 2 {
			t.Errorf("Daily.PerProject.Hard = %v, want 2", got)
		}
	})

	t.Run("explicit null min_confidence stays nil", func(t *testing.T) {
		if reg.SoftMode.MinConfidence != nil {
			t.Errorf("SoftMode.MinConfidence = %v, want nil", *reg.SoftMode.MinConfidence)
		}
	})

	t.Run("soft mode parks tier 2", func(t *testing.T) {
		if !reg.SoftMode.ParkTier2 {
			t.Error("SoftMode.ParkTier2 = false, want true")
		}
		if reg.SoftMode.ForcePROnly {
			t.Error("SoftMode.ForcePROnly = true, want false")
		}
	})

	t.Run("nested project fields", func(t *testing.T) {
		p := reg.Projects[0]
		if p.Slug != "example-api" {
			t.Errorf("Slug = %q, want %q", p.Slug, "example-api")
		}
		if !p.Triggers.WorkflowRun {
			t.Error("Triggers.WorkflowRun = false, want true")
		}
		if len(p.Triggers.Issues.Labels) != 2 {
			t.Errorf("len(Triggers.Issues.Labels) = %d, want 2", len(p.Triggers.Issues.Labels))
		}
		if !strings.Contains(p.Triggers.GCPLogFilter, "severity>=ERROR") {
			t.Errorf("GCPLogFilter = %q, want it to contain severity>=ERROR", p.Triggers.GCPLogFilter)
		}
		if p.Env["DATABASE_URL"] == "" {
			t.Error("Env[DATABASE_URL] is empty")
		}
		if p.Commands.Deploy != "make deploy" {
			t.Errorf("Commands.Deploy = %q, want %q", p.Commands.Deploy, "make deploy")
		}
	})
}

func TestParseRegistryErrors(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		wantText string
	}{
		{
			name:     "malformed yaml",
			yaml:     "version: 1\n  bad indent: [",
			wantText: "parsing registry",
		},
		{
			name:     "unknown field is rejected",
			yaml:     "version: 1\nbudgetz:\n  per_incident_usd: 1\n",
			wantText: "budgetz",
		},
		{
			name:     "unparseable duration",
			yaml:     "version: 1\ndefaults:\n  run_timeout: twenty minutes\n",
			wantText: "run_timeout",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseRegistry([]byte(tc.yaml))
			if err == nil {
				t.Fatal("ParseRegistry() error = nil, want error")
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

func TestStrictFieldCheckingInCommandsLost(t *testing.T) {
	// This test reveals the problem: typo "buld" in commands should NOT be accepted
	// because strict field checking should propagate through nested structs.
	yaml := `version: 1
defaults:
  commands:
    test: make test
    buld: make build
`
	_, err := ParseRegistry([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for typo 'buld' in commands, got nil - REGRESSION in strict field checking")
	}
	if !strings.Contains(err.Error(), "buld") {
		t.Errorf("error should mention typo 'buld': %v", err)
	}
}

func TestProjectSuppressionWindowError(t *testing.T) {
	// Check if Project.SuppressionWindow error includes field name
	yaml := `version: 1
projects:
  - slug: test
    repo: github.com/test/test
    default_branch: main
    suppression_window: not a duration
`
	_, err := ParseRegistry([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
	if !strings.Contains(err.Error(), "suppression_window") {
		t.Errorf("error should mention field 'suppression_window': %v", err)
	}
}

func TestProjectCommandsTypoRegression(t *testing.T) {
	// Regression test: typo "buld" in projects[].commands should be caught
	// (Issue: Project.UnmarshalYAML was using valNode.Decode without strict field checking)
	yaml := `version: 1
projects:
  - slug: test
    repo: github.com/test/test
    default_branch: main
    commands:
      test: make test
      buld: make build
`
	_, err := ParseRegistry([]byte(yaml))
	if err == nil {
		t.Fatal("REGRESSION: typo 'buld' in projects[].commands was not caught")
	}
	if !strings.Contains(err.Error(), "buld") {
		t.Errorf("error should mention 'buld' typo: %v", err)
	}
}

func TestProjectTriggersTypoRegression(t *testing.T) {
	// Regression test: typo "lables" in projects[].triggers.issues should be caught
	// (Issue: Project.UnmarshalYAML was using valNode.Decode without strict field checking)
	yaml := `version: 1
projects:
  - slug: test
    repo: github.com/test/test
    default_branch: main
    triggers:
      issues:
        lables: [bug]
`
	_, err := ParseRegistry([]byte(yaml))
	if err == nil {
		t.Fatal("REGRESSION: typo 'lables' in projects[].triggers.issues was not caught")
	}
	if !strings.Contains(err.Error(), "lables") {
		t.Errorf("error should mention 'lables' typo: %v", err)
	}
}
