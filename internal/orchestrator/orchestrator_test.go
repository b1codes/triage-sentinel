package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"regexp"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/store"
	"github.com/b1codes/triage-sentinel/internal/triage"
)

// registryForE2E is the registry every test in this package runs against, so
// fixture and the end-to-end tests cannot disagree about which project is
// registered. The slug matches the service_name label the gcplog fixtures use.
func registryForE2E(t *testing.T) config.Registry {
	t.Helper()
	return config.Registry{
		Defaults: config.ProjectDefaults{SuppressionWindow: config.Duration{Duration: 6 * time.Hour}},
		Projects: []config.Project{
			{Slug: "api", Repo: "github.com/example/api", DefaultBranch: "main"},
		},
	}
}

func fixture(t *testing.T) (*Orchestrator, *store.DB, *bus.Hub, context.Context, time.Time) {
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

	registry := registryForE2E(t)
	if err := store.SyncProjects(ctx, db, []store.ProjectRow{
		{Slug: "api", Repo: "github.com/example/api", DefaultBranch: "main"},
	}, now); err != nil {
		t.Fatalf("SyncProjects() error = %v", err)
	}

	hub := bus.NewHub(64)
	t.Cleanup(hub.Close)

	chain := triage.NewChain(triage.ChainOptions{
		TransientPatterns: []*regexp.Regexp{regexp.MustCompile(`(?i)ECONNRESET`)},
		BotEmail:          "sentinel@example.invalid",
	})

	o, err := New(Deps{
		DB: db, Hub: hub, Chain: chain,
		Registry: func() config.Registry { return registry },
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return o, db, hub, ctx, now
}

func seed(t *testing.T, db *store.DB, ctx context.Context, now time.Time, p store.IngestParams) int64 {
	t.Helper()
	if p.Source == "" {
		p.Source = "gcplog"
	}
	if p.Kind == "" {
		p.Kind = "log.error"
	}
	if p.State == "" {
		p.State = "received"
	}
	if p.OccurredAt.IsZero() {
		p.OccurredAt = now
	}
	res, err := store.IngestIncident(ctx, db, p, now)
	if err != nil {
		t.Fatalf("IngestIncident() error = %v", err)
	}
	return res.ID
}

func stateOf(t *testing.T, db *store.DB, ctx context.Context, id int64) (string, string) {
	t.Helper()
	in, ok, err := store.GetIncident(ctx, db, id)
	if err != nil || !ok {
		t.Fatalf("GetIncident(%d) = %v, %v", id, ok, err)
	}
	return in.State, in.StateReason
}

func TestProcessOnceRoutesByTier0(t *testing.T) {
	tests := []struct {
		name       string
		params     store.IngestParams
		wantState  string
		wantReason string
	}{
		{
			name: "clean incident reaches triaging",
			params: store.IngestParams{
				ProjectSlug: "api", SourceRef: "clean",
				Title: "TypeError: boom", Body: "at handler (src/a.js:1)",
			},
			wantState: "triaging",
		},
		{
			name: "transient noise is filtered",
			params: store.IngestParams{
				ProjectSlug: "api", SourceRef: "noise",
				Title: "job failed", Body: "read tcp: ECONNRESET",
			},
			wantState:  "filtered",
			wantReason: "Transient",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, db, _, ctx, now := fixture(t)
			id := seed(t, db, ctx, now, tc.params)

			moved, err := o.ProcessOnce(ctx)
			if err != nil {
				t.Fatalf("ProcessOnce() error = %v, want nil", err)
			}
			if moved != 1 {
				t.Fatalf("moved = %d, want 1", moved)
			}

			state, reason := stateOf(t, db, ctx, id)
			if state != tc.wantState {
				t.Errorf("state = %q, want %q", state, tc.wantState)
			}
			if tc.wantReason != "" && reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestProcessOnceSuppressesTheSecondOccurrence(t *testing.T) {
	o, db, _, ctx, now := fixture(t)

	first := seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "storm-1",
		Title: "TypeError: boom", Body: "at handler (src/a.js:1)",
	})
	second := seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "storm-2",
		Title: "TypeError: boom", Body: "at handler (src/a.js:7)",
	})

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	firstState, _ := stateOf(t, db, ctx, first)
	if firstState != "triaging" {
		t.Errorf("first incident state = %q, want triaging", firstState)
	}
	secondState, _ := stateOf(t, db, ctx, second)
	if secondState != "suppressed" {
		t.Errorf("second incident state = %q, want suppressed; the same bug opened two incidents", secondState)
	}
}

func TestProcessOnceIsIdempotent(t *testing.T) {
	o, db, _, ctx, now := fixture(t)
	seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "once", Title: "TypeError: boom",
	})

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("first ProcessOnce() error = %v", err)
	}
	moved, err := o.ProcessOnce(ctx)
	if err != nil {
		t.Fatalf("second ProcessOnce() error = %v", err)
	}
	if moved != 0 {
		t.Errorf("moved = %d on the second pass, want 0; the queue must drain", moved)
	}
}

func TestProcessOnceSkipsUnroutableIncidents(t *testing.T) {
	o, db, _, ctx, now := fixture(t)

	// An unroutable incident is written straight to filtered by the writer, so
	// it never enters the queue.
	id := seed(t, db, ctx, now, store.IngestParams{
		SourceRef: "unroutable", Title: "orphan", State: "filtered", StateReason: "unroutable",
	})

	moved, err := o.ProcessOnce(ctx)
	if err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}
	if moved != 0 {
		t.Errorf("moved = %d, want 0", moved)
	}
	state, _ := stateOf(t, db, ctx, id)
	if state != "filtered" {
		t.Errorf("state = %q, want it untouched", state)
	}
}

func TestProcessOncePublishesToTheBus(t *testing.T) {
	o, db, hub, ctx, now := fixture(t)
	client := hub.Subscribe("incidents")
	defer hub.Unsubscribe(client)

	seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "published", Title: "TypeError: boom",
	})
	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	select {
	case ev := <-client.Events():
		if ev.Topic != "incidents" {
			t.Errorf("Topic = %q, want %q", ev.Topic, "incidents")
		}
		if ev.ID == 0 {
			t.Error("Event.ID = 0; it must carry incident_events.id so replay works")
		}
	case <-time.After(time.Second):
		t.Fatal("no event published within one second")
	}
}

func TestProcessOnceQuarantinedProjectIsFiltered(t *testing.T) {
	o, db, _, ctx, now := fixture(t)

	if _, err := db.Writer().ExecContext(ctx,
		`UPDATE projects SET quarantined = 1 WHERE slug = 'api'`); err != nil {
		t.Fatalf("quarantining: %v", err)
	}

	id := seed(t, db, ctx, now, store.IngestParams{
		ProjectSlug: "api", SourceRef: "quarantined", Title: "TypeError: boom",
	})
	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	state, reason := stateOf(t, db, ctx, id)
	if state != "filtered" || reason != "Quarantined" {
		t.Errorf("state/reason = %q/%q, want filtered/Quarantined", state, reason)
	}
}

func TestRunStopsOnContextCancellation(t *testing.T) {
	o, _, _, _, _ := fixture(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within one second of cancellation")
	}
}
