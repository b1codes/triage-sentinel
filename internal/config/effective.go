package config

import (
	"sort"
	"time"
)

// EffectiveProject is a project with every default resolved, so no consumer
// needs to know which values were inherited and which were overridden.
type EffectiveProject struct {
	Slug          string
	Repo          string
	DefaultBranch string

	Autonomy    string
	Tier1Model  string
	Tier2Model  string
	Tier2Effort string

	MaxTurns                        int
	MaxDiffLines                    int
	ProbationIncidents              int
	MaxRepairAttemptsPerFingerprint int

	RunTimeout        time.Duration
	SuppressionWindow time.Duration

	AllowTestChanges bool

	// DailyBudgetUSD is nil when the project inherits the global per-project
	// daily limit rather than declaring its own.
	DailyBudgetUSD *float64

	ProtectedPaths []string

	// SourceRoots is empty when the project declares none, which selects the
	// denylist frame-selection strategy rather than meaning "no frames".
	SourceRoots []string

	Commands Commands
	Triggers Triggers
	Env      map[string]string
}

// EffectiveProject resolves defaults and overrides for one slug. It reports
// false when the slug is not in the registry.
func (r Registry) EffectiveProject(slug string) (EffectiveProject, bool) {
	for _, p := range r.Projects {
		if p.Slug != slug {
			continue
		}
		d := r.Defaults

		eff := EffectiveProject{
			Slug:                            p.Slug,
			Repo:                            p.Repo,
			DefaultBranch:                   p.DefaultBranch,
			Autonomy:                        firstNonEmpty(p.Autonomy, d.Autonomy),
			Tier1Model:                      d.Tier1Model,
			Tier2Model:                      firstNonEmpty(p.Tier2Model, d.Tier2Model),
			Tier2Effort:                     d.Tier2Effort,
			MaxTurns:                        d.MaxTurns,
			MaxDiffLines:                    d.MaxDiffLines,
			ProbationIncidents:              d.ProbationIncidents,
			MaxRepairAttemptsPerFingerprint: d.MaxRepairAttemptsPerFingerprint,
			RunTimeout:                      d.RunTimeout.Duration,
			SuppressionWindow:               d.SuppressionWindow.Duration,
			AllowTestChanges:                d.AllowTestChanges,
			DailyBudgetUSD:                  p.DailyBudgetUSD,
			ProtectedPaths:                  append([]string(nil), d.ProtectedPaths...),
			Commands:                        mergeCommands(d.Commands, p.Commands),
			Triggers:                        p.Triggers,
			Env:                             copyStringMap(p.Env),
		}

		if p.SuppressionWindow != nil {
			eff.SuppressionWindow = p.SuppressionWindow.Duration
		}
		if p.MaxDiffLines != nil {
			eff.MaxDiffLines = *p.MaxDiffLines
		}
		if p.AllowTestChanges != nil {
			eff.AllowTestChanges = *p.AllowTestChanges
		}
		if p.Fingerprint != nil {
			eff.SourceRoots = append([]string(nil), p.Fingerprint.SourceRoots...)
		}
		return eff, true
	}
	return EffectiveProject{}, false
}

// Slugs returns every registered project slug in sorted order.
func (r Registry) Slugs() []string {
	slugs := make([]string, 0, len(r.Projects))
	for _, p := range r.Projects {
		slugs = append(slugs, p.Slug)
	}
	sort.Strings(slugs)
	return slugs
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
