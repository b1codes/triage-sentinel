package config

import (
	"errors"
	"fmt"
	"net/mail"
	"os"
	"path"
	"regexp"
	"strings"
)

var (
	slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,38}[a-z0-9]$`)
	repoPattern = regexp.MustCompile(`^github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
)

var (
	validAutonomy   = map[string]bool{"pr_only": true, "auto_merge": true, "auto_deploy": true}
	validEffort     = map[string]bool{"low": true, "medium": true, "high": true, "xhigh": true, "max": true}
	validWeekStart  = map[string]bool{"monday": true, "sunday": true}
	validWindowKind = map[string]bool{"day": true, "week": true, "month": true}
)

// LoadRegistry reads, parses, and validates a projects.yaml file. Every error
// wraps ErrInvalidRegistry.
func LoadRegistry(path string) (Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Registry{}, fmt.Errorf("%w: reading %s: %w", ErrInvalidRegistry, path, err)
	}

	reg, err := ParseRegistry(data)
	if err != nil {
		return Registry{}, err
	}
	if err := reg.Validate(); err != nil {
		return Registry{}, err
	}
	return reg, nil
}

// Validate checks every rule in SPEC §4.1. It accumulates all problems and
// returns them together, because an operator fixing a registry should see the
// complete list rather than one problem per restart. The returned error wraps
// ErrInvalidRegistry.
func (r Registry) Validate() error {
	var problems []error

	if r.Version != 1 {
		problems = append(problems, fmt.Errorf("version must be 1, got %d", r.Version))
	}

	problems = append(problems, r.validateBudgets()...)
	problems = append(problems, r.validateSoftMode()...)
	problems = append(problems, r.validateDefaults()...)
	problems = append(problems, r.validateRuntime()...)
	problems = append(problems, r.validateBot()...)
	problems = append(problems, r.validateTriage()...)
	problems = append(problems, r.validateProjects()...)

	if len(problems) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidRegistry, errors.Join(problems...))
	}
	return nil
}

func (r Registry) validateBudgets() []error {
	var problems []error
	b := r.Budgets

	if b.PerIncidentUSD <= 0 {
		problems = append(problems, fmt.Errorf(
			"budgets.per_incident_usd must be > 0, got %v", b.PerIncidentUSD))
	}

	windows := []struct {
		name string
		t    Threshold
	}{
		{"budgets.daily.global", b.Daily.Global},
		{"budgets.daily.per_project", b.Daily.PerProject},
		{"budgets.weekly.global", b.Weekly.Global},
		{"budgets.monthly.global", b.Monthly.Global},
	}
	for _, w := range windows {
		problems = append(problems, validateThreshold(w.name, w.t)...)
	}

	if b.Daily.Global.Hard == nil {
		problems = append(problems, errors.New("budgets.daily.global.hard is required"))
	}
	// Weekly hard cap is required for the same reason as daily and monthly:
	// without it, a weekly soft limit alone would alert but never actually
	// stop spend, leaving the window unbounded (a correction on top of the
	// brief, which checked daily and monthly but not weekly).
	if b.Weekly.Global.Hard == nil {
		problems = append(problems, errors.New("budgets.weekly.global.hard is required"))
	}
	if b.Monthly.Global.Hard == nil {
		problems = append(problems, errors.New("budgets.monthly.global.hard is required"))
	}

	if !validWeekStart[b.WeekStartsOn] {
		problems = append(problems, fmt.Errorf(
			"budgets.week_starts_on must be monday or sunday, got %q", b.WeekStartsOn))
	}

	if b.Efficiency.Enabled {
		if !validWindowKind[b.Efficiency.Window] {
			problems = append(problems, fmt.Errorf(
				"budgets.efficiency.window must be day, week or month, got %q", b.Efficiency.Window))
		}
		if b.Efficiency.MaxCostPerResolutionMultiple <= 1 {
			problems = append(problems, fmt.Errorf(
				"budgets.efficiency.max_cost_per_resolution_multiple must be > 1, got %v",
				b.Efficiency.MaxCostPerResolutionMultiple))
		}
		if b.Efficiency.MinResolutionsForSignal < 1 {
			problems = append(problems, fmt.Errorf(
				"budgets.efficiency.min_resolutions_for_signal must be >= 1, got %d",
				b.Efficiency.MinResolutionsForSignal))
		}
	}

	if b.Forecast.Enabled {
		if f := b.Forecast.WarnAtFractionOfHard; f <= 0 || f > 1 {
			problems = append(problems, fmt.Errorf(
				"budgets.forecast.warn_at_fraction_of_hard must be in (0, 1], got %v", f))
		}
	}

	return problems
}

func validateThreshold(name string, t Threshold) []error {
	var problems []error

	if t.Soft != nil && *t.Soft <= 0 {
		problems = append(problems, fmt.Errorf("%s.soft must be > 0, got %v", name, *t.Soft))
	}
	if t.Hard != nil && *t.Hard <= 0 {
		problems = append(problems, fmt.Errorf("%s.hard must be > 0, got %v", name, *t.Hard))
	}
	if t.Soft != nil && t.Hard != nil && *t.Soft >= *t.Hard {
		problems = append(problems, fmt.Errorf(
			"%s.soft (%v) must be strictly less than %s.hard (%v)", name, *t.Soft, name, *t.Hard))
	}
	return problems
}

func (r Registry) validateSoftMode() []error {
	s := r.SoftMode
	if !s.ParkTier2 && !s.DowngradeModel && !s.ForcePROnly && s.MinConfidence == nil {
		return []error{errors.New(
			"soft_mode has no action enabled; a soft threshold would alert but change nothing")}
	}
	if s.MinConfidence != nil {
		if c := *s.MinConfidence; c < 0 || c > 1 {
			return []error{fmt.Errorf(
				"soft_mode.min_confidence must be in [0, 1], got %v", c)}
		}
	}
	return nil
}

func (r Registry) validateDefaults() []error {
	var problems []error
	d := r.Defaults

	if !validAutonomy[d.Autonomy] {
		problems = append(problems, fmt.Errorf(
			"defaults.autonomy must be pr_only, auto_merge or auto_deploy, got %q", d.Autonomy))
	}
	if !validEffort[d.Tier2Effort] {
		problems = append(problems, fmt.Errorf(
			"defaults.tier2_effort must be low, medium, high, xhigh or max, got %q", d.Tier2Effort))
	}
	problems = append(problems, validateModelField("defaults.tier1_model", d.Tier1Model, true)...)
	problems = append(problems, validateModelField("defaults.tier2_model", d.Tier2Model, true)...)

	if d.MaxTurns < 1 {
		problems = append(problems, fmt.Errorf("defaults.max_turns must be >= 1, got %d", d.MaxTurns))
	}
	if d.RunTimeout.Duration <= 0 {
		problems = append(problems, errors.New("defaults.run_timeout must be > 0"))
	}
	if d.SuppressionWindow.Duration <= 0 {
		problems = append(problems, errors.New("defaults.suppression_window must be > 0"))
	}
	if d.MaxRepairAttemptsPerFingerprint < 1 {
		problems = append(problems, fmt.Errorf(
			"defaults.max_repair_attempts_per_fingerprint must be >= 1, got %d",
			d.MaxRepairAttemptsPerFingerprint))
	}
	if d.MaxDiffLines < 1 {
		problems = append(problems, fmt.Errorf(
			"defaults.max_diff_lines must be >= 1, got %d", d.MaxDiffLines))
	}
	if d.ProbationIncidents < 0 {
		problems = append(problems, fmt.Errorf(
			"defaults.probation_incidents must be >= 0, got %d", d.ProbationIncidents))
	}
	return problems
}

// validateModelField checks a model ID against the price table. The system
// refuses to start rather than discover an unpriceable model at spend time
// (SPEC §7.2).
func validateModelField(field, id string, required bool) []error {
	if id == "" {
		if required {
			return []error{fmt.Errorf("%s is required", field)}
		}
		return nil
	}
	if _, ok := LookupModel(id); !ok {
		return []error{fmt.Errorf(
			"%s %q is not in the price table; known models: %s",
			field, id, strings.Join(KnownModelIDs(), ", "))}
	}
	return nil
}

func (r Registry) validateRuntime() []error {
	var problems []error
	rt := r.Runtime

	if rt.MaxConcurrentAgents < 1 {
		problems = append(problems, fmt.Errorf(
			"runtime.max_concurrent_agents must be >= 1, got %d", rt.MaxConcurrentAgents))
	}
	if rt.MinFreeRAMMB < 0 {
		problems = append(problems, fmt.Errorf(
			"runtime.min_free_ram_mb must be >= 0, got %d", rt.MinFreeRAMMB))
	}
	if rt.MinFreeDiskMB < 0 {
		problems = append(problems, fmt.Errorf(
			"runtime.min_free_disk_mb must be >= 0, got %d", rt.MinFreeDiskMB))
	}
	if c := rt.Tier2MinConfidence; c < 0 || c > 1 {
		problems = append(problems, fmt.Errorf(
			"runtime.tier2_min_confidence must be in [0, 1], got %v", c))
	}
	if rt.MaxInputTokens < 1000 {
		problems = append(problems, fmt.Errorf(
			"runtime.max_input_tokens must be >= 1000, got %d", rt.MaxInputTokens))
	}
	return problems
}

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

func (r Registry) validateProjects() []error {
	var problems []error

	if len(r.Projects) == 0 {
		problems = append(problems, errors.New("projects must contain at least one project"))
	}

	seen := make(map[string]bool, len(r.Projects))
	for i, p := range r.Projects {
		where := fmt.Sprintf("projects[%d]", i)
		if p.Slug != "" {
			where = fmt.Sprintf("projects[%s]", p.Slug)
		}

		if !slugPattern.MatchString(p.Slug) {
			problems = append(problems, fmt.Errorf(
				"%s.slug %q must match %s", where, p.Slug, slugPattern.String()))
		} else if seen[p.Slug] {
			problems = append(problems, fmt.Errorf("duplicate slug %q", p.Slug))
		} else {
			seen[p.Slug] = true
		}

		if !repoPattern.MatchString(p.Repo) {
			problems = append(problems, fmt.Errorf(
				"%s.repo %q must be github.com/<owner>/<name>", where, p.Repo))
		}
		if strings.TrimSpace(p.DefaultBranch) == "" {
			problems = append(problems, fmt.Errorf("%s.default_branch is required", where))
		}

		autonomy := firstNonEmpty(p.Autonomy, r.Defaults.Autonomy)
		if !validAutonomy[autonomy] {
			problems = append(problems, fmt.Errorf(
				"%s.autonomy must be pr_only, auto_merge or auto_deploy, got %q", where, autonomy))
		}

		cmds := mergeCommands(r.Defaults.Commands, p.Commands)
		if strings.TrimSpace(cmds.Test) == "" {
			problems = append(problems, fmt.Errorf(
				"%s has no effective commands.test (set it on the project or in defaults)", where))
		}
		if autonomy == "auto_deploy" && strings.TrimSpace(cmds.Deploy) == "" {
			problems = append(problems, fmt.Errorf(
				"%s uses autonomy auto_deploy and therefore requires commands.deploy", where))
		}

		problems = append(problems, validateModelField(where+".tier2_model", p.Tier2Model, false)...)

		if p.DailyBudgetUSD != nil && *p.DailyBudgetUSD <= 0 {
			problems = append(problems, fmt.Errorf(
				"%s.daily_budget_usd must be > 0, got %v", where, *p.DailyBudgetUSD))
		}
		if p.SuppressionWindow != nil && p.SuppressionWindow.Duration <= 0 {
			problems = append(problems, fmt.Errorf("%s.suppression_window must be > 0", where))
		}
		if p.MaxDiffLines != nil && *p.MaxDiffLines < 1 {
			problems = append(problems, fmt.Errorf(
				"%s.max_diff_lines must be >= 1, got %d", where, *p.MaxDiffLines))
		}
		if !p.Triggers.WorkflowRun &&
			len(p.Triggers.Issues.Labels) == 0 &&
			strings.TrimSpace(p.Triggers.GCPLogFilter) == "" {
			problems = append(problems, fmt.Errorf(
				"%s has no triggers enabled; it would never receive an incident", where))
		}

		problems = append(problems, validateSourceRoots(p.Slug, p.Fingerprint)...)
	}
	return problems
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// mergeCommands overlays a project's commands on the defaults, field by field.
func mergeCommands(defaults, override Commands) Commands {
	return Commands{
		Test:        firstNonEmpty(override.Test, defaults.Test),
		Build:       firstNonEmpty(override.Build, defaults.Build),
		Healthcheck: firstNonEmpty(override.Healthcheck, defaults.Healthcheck),
		Deploy:      firstNonEmpty(override.Deploy, defaults.Deploy),
		Rollback:    firstNonEmpty(override.Rollback, defaults.Rollback),
	}
}
