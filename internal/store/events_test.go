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
