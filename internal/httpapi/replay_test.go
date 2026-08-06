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
