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
