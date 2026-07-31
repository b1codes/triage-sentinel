package config

import (
	"testing"
	"time"
)

func TestEffectiveProjectInheritsDefaults(t *testing.T) {
	reg := baseRegistry()

	got, ok := reg.EffectiveProject("example-api")
	if !ok {
		t.Fatal("EffectiveProject() ok = false, want true")
	}

	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "autonomy", got: got.Autonomy, want: "pr_only"},
		{name: "tier1 model", got: got.Tier1Model, want: "claude-haiku-4-5"},
		{name: "tier2 model", got: got.Tier2Model, want: "claude-opus-5"},
		{name: "tier2 effort", got: got.Tier2Effort, want: "high"},
		{name: "max turns", got: got.MaxTurns, want: 40},
		{name: "max diff lines", got: got.MaxDiffLines, want: 400},
		{name: "run timeout", got: got.RunTimeout, want: 20 * time.Minute},
		{name: "suppression window", got: got.SuppressionWindow, want: 6 * time.Hour},
		{name: "allow test changes", got: got.AllowTestChanges, want: false},
		{name: "test command", got: got.Commands.Test, want: "make test"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}

	if got.DailyBudgetUSD != nil {
		t.Errorf("DailyBudgetUSD = %v, want nil (falls back to the global per-project limit)", *got.DailyBudgetUSD)
	}
}

func TestEffectiveProjectAppliesOverrides(t *testing.T) {
	reg := baseRegistry()
	budget := 0.75
	window := Duration{12 * time.Hour}
	diff := 120
	allow := true

	reg.Projects[0].Autonomy = "auto_merge"
	reg.Projects[0].Tier2Model = "claude-sonnet-5"
	reg.Projects[0].Commands.Test = "npm test"
	reg.Projects[0].DailyBudgetUSD = &budget
	reg.Projects[0].SuppressionWindow = &window
	reg.Projects[0].MaxDiffLines = &diff
	reg.Projects[0].AllowTestChanges = &allow

	got, ok := reg.EffectiveProject("example-api")
	if !ok {
		t.Fatal("EffectiveProject() ok = false, want true")
	}

	if got.Autonomy != "auto_merge" {
		t.Errorf("Autonomy = %q, want %q", got.Autonomy, "auto_merge")
	}
	if got.Tier2Model != "claude-sonnet-5" {
		t.Errorf("Tier2Model = %q, want %q", got.Tier2Model, "claude-sonnet-5")
	}
	if got.Commands.Test != "npm test" {
		t.Errorf("Commands.Test = %q, want %q", got.Commands.Test, "npm test")
	}
	// Build is not overridden, so it still comes from defaults.
	if got.Commands.Build != "make build" {
		t.Errorf("Commands.Build = %q, want %q", got.Commands.Build, "make build")
	}
	if got.DailyBudgetUSD == nil || *got.DailyBudgetUSD != 0.75 {
		t.Errorf("DailyBudgetUSD = %v, want 0.75", got.DailyBudgetUSD)
	}
	if got.SuppressionWindow != 12*time.Hour {
		t.Errorf("SuppressionWindow = %v, want %v", got.SuppressionWindow, 12*time.Hour)
	}
	if got.MaxDiffLines != 120 {
		t.Errorf("MaxDiffLines = %d, want 120", got.MaxDiffLines)
	}
	if !got.AllowTestChanges {
		t.Error("AllowTestChanges = false, want true")
	}
}

func TestEffectiveProjectUnknownSlug(t *testing.T) {
	if _, ok := baseRegistry().EffectiveProject("nope"); ok {
		t.Error("EffectiveProject(\"nope\") ok = true, want false")
	}
}

func TestEffectiveProjectProtectedPathsAreCopied(t *testing.T) {
	reg := baseRegistry()
	reg.Defaults.ProtectedPaths = []string{".github/**"}

	got, _ := reg.EffectiveProject("example-api")
	got.ProtectedPaths[0] = "clobbered"

	again, _ := reg.EffectiveProject("example-api")
	if again.ProtectedPaths[0] == "clobbered" {
		t.Error("ProtectedPaths shares backing storage with the registry; a caller can corrupt it")
	}
}

func TestSlugsIsSorted(t *testing.T) {
	reg := baseRegistry()
	reg.Projects = append(reg.Projects, Project{Slug: "aaa-first"}, Project{Slug: "zzz-last"})

	slugs := reg.Slugs()
	for i := 1; i < len(slugs); i++ {
		if slugs[i-1] >= slugs[i] {
			t.Errorf("Slugs() not sorted: %q >= %q", slugs[i-1], slugs[i])
		}
	}
}
