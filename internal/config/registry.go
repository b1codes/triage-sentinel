package config

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// ErrInvalidRegistry is returned when projects.yaml cannot be parsed or fails
// validation.
var ErrInvalidRegistry = errors.New("invalid project registry")

// Duration wraps time.Duration so YAML string values such as "20m" and "6h"
// decode correctly. gopkg.in/yaml.v3 decodes a bare time.Duration only from an
// integer nanosecond count, which would silently misread every duration in the
// registry.
type Duration struct {
	time.Duration
}

// UnmarshalYAML decodes a Go duration string into d.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string such as \"20m\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("parsing duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

// MarshalYAML renders d back to its string form.
func (d Duration) MarshalYAML() (any, error) {
	return d.Duration.String(), nil
}

// Threshold is a soft/hard budget pair. Both fields are pointers so an absent
// threshold is distinguishable from an explicit zero: the per-project daily
// budget declares a hard limit with no soft limit (SPEC §7.1).
type Threshold struct {
	Soft *float64 `yaml:"soft"`
	Hard *float64 `yaml:"hard"`
}

// WindowBudget holds the global threshold for one budget window.
type WindowBudget struct {
	Global Threshold `yaml:"global"`
}

// DailyBudget holds both the global and the per-project daily thresholds.
type DailyBudget struct {
	Global     Threshold `yaml:"global"`
	PerProject Threshold `yaml:"per_project"`
}

// Efficiency configures the cost-per-resolution signal (SPEC §7.4).
type Efficiency struct {
	Enabled                      bool    `yaml:"enabled"`
	Window                       string  `yaml:"window"`
	MaxCostPerResolutionMultiple float64 `yaml:"max_cost_per_resolution_multiple"`
	MinResolutionsForSignal      int     `yaml:"min_resolutions_for_signal"`
}

// Forecast configures the burn-rate projection signal (SPEC §7.5).
type Forecast struct {
	Enabled              bool    `yaml:"enabled"`
	WarnAtFractionOfHard float64 `yaml:"warn_at_fraction_of_hard"`
}

// Budgets is the full threshold ladder (SPEC §7.1).
type Budgets struct {
	PerIncidentUSD float64      `yaml:"per_incident_usd"`
	Daily          DailyBudget  `yaml:"daily"`
	Weekly         WindowBudget `yaml:"weekly"`
	Monthly        WindowBudget `yaml:"monthly"`
	WeekStartsOn   string       `yaml:"week_starts_on"`
	Efficiency     Efficiency   `yaml:"efficiency"`
	Forecast       Forecast     `yaml:"forecast"`
}

// SoftMode describes what happens when a soft threshold is crossed
// (SPEC §7.3).
type SoftMode struct {
	ParkTier2      bool     `yaml:"park_tier2"`
	DowngradeModel bool     `yaml:"downgrade_model"`
	ForcePROnly    bool     `yaml:"force_pr_only"`
	MinConfidence  *float64 `yaml:"min_confidence"`
}

// Commands is a project's declared entry points (SPEC §6.3).
type Commands struct {
	Test        string `yaml:"test"`
	Build       string `yaml:"build"`
	Healthcheck string `yaml:"healthcheck"`
	Deploy      string `yaml:"deploy"`
	Rollback    string `yaml:"rollback"`
}

// ProjectDefaults supplies values for any project that does not override them.
type ProjectDefaults struct {
	Autonomy                        string   `yaml:"autonomy"`
	Tier1Model                      string   `yaml:"tier1_model"`
	Tier2Model                      string   `yaml:"tier2_model"`
	Tier2Effort                     string   `yaml:"tier2_effort"`
	MaxTurns                        int      `yaml:"max_turns"`
	RunTimeout                      Duration `yaml:"run_timeout"`
	SuppressionWindow               Duration `yaml:"suppression_window"`
	MaxRepairAttemptsPerFingerprint int      `yaml:"max_repair_attempts_per_fingerprint"`
	MaxDiffLines                    int      `yaml:"max_diff_lines"`
	ProbationIncidents              int      `yaml:"probation_incidents"`
	AllowTestChanges                bool     `yaml:"allow_test_changes"`
	ProtectedPaths                  []string `yaml:"protected_paths"`
	Commands                        Commands `yaml:"commands"`
}

// UnmarshalYAML provides field-aware error messages for ProjectDefaults.
// NOTE: This switch must stay in sync with ProjectDefaults struct fields.
func (pd *ProjectDefaults) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping for defaults")
	}

	// Create a default instance
	*pd = ProjectDefaults{}

	// Process each key-value pair
	for i := 0; i < len(value.Content); i += 2 {
		keyNode := value.Content[i]
		valNode := value.Content[i+1]

		key := keyNode.Value

		switch key {
		case "autonomy":
			if err := valNode.Decode(&pd.Autonomy); err != nil {
				return fmt.Errorf("autonomy: %w", err)
			}
		case "tier1_model":
			if err := valNode.Decode(&pd.Tier1Model); err != nil {
				return fmt.Errorf("tier1_model: %w", err)
			}
		case "tier2_model":
			if err := valNode.Decode(&pd.Tier2Model); err != nil {
				return fmt.Errorf("tier2_model: %w", err)
			}
		case "tier2_effort":
			if err := valNode.Decode(&pd.Tier2Effort); err != nil {
				return fmt.Errorf("tier2_effort: %w", err)
			}
		case "max_turns":
			if err := valNode.Decode(&pd.MaxTurns); err != nil {
				return fmt.Errorf("max_turns: %w", err)
			}
		case "run_timeout":
			if err := valNode.Decode(&pd.RunTimeout); err != nil {
				return fmt.Errorf("run_timeout: %w", err)
			}
		case "suppression_window":
			if err := valNode.Decode(&pd.SuppressionWindow); err != nil {
				return fmt.Errorf("suppression_window: %w", err)
			}
		case "max_repair_attempts_per_fingerprint":
			if err := valNode.Decode(&pd.MaxRepairAttemptsPerFingerprint); err != nil {
				return fmt.Errorf("max_repair_attempts_per_fingerprint: %w", err)
			}
		case "max_diff_lines":
			if err := valNode.Decode(&pd.MaxDiffLines); err != nil {
				return fmt.Errorf("max_diff_lines: %w", err)
			}
		case "probation_incidents":
			if err := valNode.Decode(&pd.ProbationIncidents); err != nil {
				return fmt.Errorf("probation_incidents: %w", err)
			}
		case "allow_test_changes":
			if err := valNode.Decode(&pd.AllowTestChanges); err != nil {
				return fmt.Errorf("allow_test_changes: %w", err)
			}
		case "protected_paths":
			if err := valNode.Decode(&pd.ProtectedPaths); err != nil {
				return fmt.Errorf("protected_paths: %w", err)
			}
		case "commands":
			// For nested struct, enforce strict field checking by validating unknown fields
			if err := pd.decodeCommandsStrict(valNode); err != nil {
				return fmt.Errorf("commands: %w", err)
			}
		default:
			return fmt.Errorf("unknown field %q in defaults", key)
		}
	}

	return nil
}

// decodeCommandsStrict decodes Commands with strict field checking.
// yaml.Node.Decode() creates a fresh decoder that doesn't inherit the parent's
// KnownFields(true) setting, so we manually validate unknown fields first.
func (pd *ProjectDefaults) decodeCommandsStrict(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping")
	}

	// Known fields in Commands struct
	knownFields := map[string]bool{
		"test":        true,
		"build":       true,
		"healthcheck": true,
		"deploy":      true,
		"rollback":    true,
	}

	// Check for unknown fields
	for i := 0; i < len(node.Content); i += 2 {
		fieldName := node.Content[i].Value
		if !knownFields[fieldName] {
			return fmt.Errorf("unknown field %q", fieldName)
		}
	}

	// Now decode with the normal decoder
	return node.Decode(&pd.Commands)
}

// IssueTrigger selects which GitHub issue labels open an incident.
type IssueTrigger struct {
	Labels []string `yaml:"labels"`
}

// Triggers declares which event sources apply to a project.
type Triggers struct {
	WorkflowRun  bool         `yaml:"workflow_run"`
	Issues       IssueTrigger `yaml:"issues"`
	GCPLogFilter string       `yaml:"gcp_log_filter"`
}

// Project is one managed application. Empty scalar fields and nil pointers
// inherit from ProjectDefaults; see EffectiveProject.
type Project struct {
	Slug          string            `yaml:"slug"`
	Repo          string            `yaml:"repo"`
	DefaultBranch string            `yaml:"default_branch"`
	Autonomy      string            `yaml:"autonomy"`
	Triggers      Triggers          `yaml:"triggers"`
	Commands      Commands          `yaml:"commands"`
	Env           map[string]string `yaml:"env"`

	Tier2Model        string    `yaml:"tier2_model"`
	DailyBudgetUSD    *float64  `yaml:"daily_budget_usd"`
	SuppressionWindow *Duration `yaml:"suppression_window"`
	MaxDiffLines      *int      `yaml:"max_diff_lines"`
	AllowTestChanges  *bool     `yaml:"allow_test_changes"`
}

// UnmarshalYAML provides field-aware error messages for Project, especially for Duration fields.
func (p *Project) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping for project")
	}

	// Create a default instance
	*p = Project{}

	// Process each key-value pair
	for i := 0; i < len(value.Content); i += 2 {
		keyNode := value.Content[i]
		valNode := value.Content[i+1]

		key := keyNode.Value

		switch key {
		case "slug":
			if err := valNode.Decode(&p.Slug); err != nil {
				return fmt.Errorf("slug: %w", err)
			}
		case "repo":
			if err := valNode.Decode(&p.Repo); err != nil {
				return fmt.Errorf("repo: %w", err)
			}
		case "default_branch":
			if err := valNode.Decode(&p.DefaultBranch); err != nil {
				return fmt.Errorf("default_branch: %w", err)
			}
		case "autonomy":
			if err := valNode.Decode(&p.Autonomy); err != nil {
				return fmt.Errorf("autonomy: %w", err)
			}
		case "triggers":
			if err := valNode.Decode(&p.Triggers); err != nil {
				return fmt.Errorf("triggers: %w", err)
			}
		case "commands":
			if err := valNode.Decode(&p.Commands); err != nil {
				return fmt.Errorf("commands: %w", err)
			}
		case "env":
			if err := valNode.Decode(&p.Env); err != nil {
				return fmt.Errorf("env: %w", err)
			}
		case "tier2_model":
			if err := valNode.Decode(&p.Tier2Model); err != nil {
				return fmt.Errorf("tier2_model: %w", err)
			}
		case "daily_budget_usd":
			if err := valNode.Decode(&p.DailyBudgetUSD); err != nil {
				return fmt.Errorf("daily_budget_usd: %w", err)
			}
		case "suppression_window":
			if err := valNode.Decode(&p.SuppressionWindow); err != nil {
				return fmt.Errorf("suppression_window: %w", err)
			}
		case "max_diff_lines":
			if err := valNode.Decode(&p.MaxDiffLines); err != nil {
				return fmt.Errorf("max_diff_lines: %w", err)
			}
		case "allow_test_changes":
			if err := valNode.Decode(&p.AllowTestChanges); err != nil {
				return fmt.Errorf("allow_test_changes: %w", err)
			}
		default:
			return fmt.Errorf("unknown field %q in project", key)
		}
	}

	return nil
}

// Runtime holds host-level execution limits (SPEC §4.12).
type Runtime struct {
	MaxConcurrentAgents int     `yaml:"max_concurrent_agents"`
	MinFreeRAMMB        int     `yaml:"min_free_ram_mb"`
	MinFreeDiskMB       int     `yaml:"min_free_disk_mb"`
	Tier2MinConfidence  float64 `yaml:"tier2_min_confidence"`
	MaxInputTokens      int     `yaml:"max_input_tokens"`
}

// Registry is the parsed contents of projects.yaml.
type Registry struct {
	Version  int             `yaml:"version"`
	Budgets  Budgets         `yaml:"budgets"`
	SoftMode SoftMode        `yaml:"soft_mode"`
	Defaults ProjectDefaults `yaml:"defaults"`
	Runtime  Runtime         `yaml:"runtime"`
	Projects []Project       `yaml:"projects"`
}

// ParseRegistry decodes projects.yaml with strict field checking, so a typo in
// a key is an error rather than a silently ignored setting. It performs no
// semantic validation; call Validate for that.
func ParseRegistry(data []byte) (Registry, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	var reg Registry
	if err := dec.Decode(&reg); err != nil {
		return Registry{}, fmt.Errorf("%w: parsing registry: %w", ErrInvalidRegistry, err)
	}
	return reg, nil
}
