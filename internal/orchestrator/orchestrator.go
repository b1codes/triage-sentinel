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
	// FIFO, not the dashboard's newest-first: the earliest occurrence of a bug
	// must be the one that opens the suppression window and becomes canonical,
	// and a newest-first batch would starve the oldest rows under load.
	pending, _, err := store.ListIncidents(ctx, o.deps.DB, store.IncidentFilter{
		States:      []string{"received"},
		Limit:       o.deps.BatchSize,
		OldestFirst: true,
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
