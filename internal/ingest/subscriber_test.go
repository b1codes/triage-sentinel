package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/store"
)

// fakePuller serves scripted batches then blocks until the context ends.
type fakePuller struct {
	mu       sync.Mutex
	batches  [][]Message
	acked    []string
	pullErr  error
	pullCall int
}

func (f *fakePuller) Pull(ctx context.Context, _ int) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pullCall++
	if f.pullErr != nil {
		return nil, f.pullErr
	}
	if len(f.batches) == 0 {
		f.mu.Unlock()
		<-ctx.Done()
		f.mu.Lock()
		return nil, ctx.Err()
	}
	batch := f.batches[0]
	f.batches = f.batches[1:]
	return batch, nil
}

func (f *fakePuller) Ack(_ context.Context, ackIDs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, ackIDs...)
	return nil
}

func (f *fakePuller) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acked...)
}

// recordingHandler captures handled events and can be made to fail.
type recordingHandler struct {
	mu     sync.Mutex
	events []Event
	err    error
}

func (h *recordingHandler) Handle(_ context.Context, ev Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return h.err
	}
	h.events = append(h.events, ev)
	return nil
}

func (h *recordingHandler) handled() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Event(nil), h.events...)
}

func runSubscriber(t *testing.T, puller Puller, handler EventHandler, adapters ...Adapter) *Subscriber {
	t.Helper()

	s, err := NewSubscriber(SubscriberOptions{
		Puller:    puller,
		Router:    NewRouter(adapters...),
		Handler:   handler,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		IdleDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber() error = %v, want nil", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Run did not return after context cancellation")
		}
	})
	return s
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within one second")
}

func TestSubscriberPersistsThenAcks(t *testing.T) {
	puller := &fakePuller{batches: [][]Message{{
		{ID: "m-1", AckID: "ack-1", Attributes: map[string]string{"k": "v"}},
	}}}
	handler := &recordingHandler{}

	runSubscriber(t, puller, handler,
		stubAdapter{name: "s", matchKey: "k", event: Event{Source: "s", SourceRef: "r-1"}})

	waitFor(t, func() bool { return len(handler.handled()) == 1 })
	waitFor(t, func() bool { return len(puller.ackedIDs()) == 1 })

	if puller.ackedIDs()[0] != "ack-1" {
		t.Errorf("acked %v, want ack-1", puller.ackedIDs())
	}
}

func TestSubscriberDoesNotAckWhenTheWriteFails(t *testing.T) {
	puller := &fakePuller{batches: [][]Message{{
		{ID: "m-1", AckID: "ack-1", Attributes: map[string]string{"k": "v"}},
	}}}
	handler := &recordingHandler{err: errors.New("disk full")}

	s := runSubscriber(t, puller, handler,
		stubAdapter{name: "s", matchKey: "k", event: Event{Source: "s", SourceRef: "r-1"}})

	waitFor(t, func() bool { return s.Stats().WriteErrors > 0 })

	if len(puller.ackedIDs()) != 0 {
		t.Errorf("acked %v, want none; an unwritten message must be redelivered", puller.ackedIDs())
	}
}

func TestSubscriberAcksUnpersistedOutcomes(t *testing.T) {
	tests := []struct {
		name      string
		adapter   Adapter
		wantStat  func(Stats) int
		statLabel string
	}{
		{
			name:      "ignored events",
			adapter:   stubAdapter{name: "s", matchKey: "k", err: ErrIgnore},
			wantStat:  func(s Stats) int { return s.Ignored },
			statLabel: "Ignored",
		},
		{
			name:      "unclaimed messages",
			adapter:   stubAdapter{name: "s", matchKey: "other", err: nil},
			wantStat:  func(s Stats) int { return s.Unrouted },
			statLabel: "Unrouted",
		},
		{
			name:      "signature failures",
			adapter:   stubAdapter{name: "s", matchKey: "k", err: ErrSignature},
			wantStat:  func(s Stats) int { return s.SignatureFailures },
			statLabel: "SignatureFailures",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			puller := &fakePuller{batches: [][]Message{{
				{ID: "m-1", AckID: "ack-1", Attributes: map[string]string{"k": "v"}},
			}}}

			s := runSubscriber(t, puller, &recordingHandler{}, tc.adapter)

			waitFor(t, func() bool { return tc.wantStat(s.Stats()) > 0 })
			waitFor(t, func() bool { return len(puller.ackedIDs()) == 1 })
		})
	}
}

func TestSubscriberSurvivesPullErrors(t *testing.T) {
	puller := &fakePuller{pullErr: errors.New("network down")}
	s := runSubscriber(t, puller, &recordingHandler{})

	waitFor(t, func() bool { return s.Stats().PullErrors >= 2 })
	// The loop must keep retrying rather than returning.
}

func TestSubscriberStopsOnContextCancellation(t *testing.T) {
	puller := &fakePuller{}
	s, err := NewSubscriber(SubscriberOptions{
		Puller:  puller,
		Router:  NewRouter(),
		Handler: &recordingHandler{},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewSubscriber() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v, want nil or context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within one second of cancellation")
	}
}

func TestIncidentWriterPersistsJobSteps(t *testing.T) {
	// A CI failure must carry its failing job and step through persistence, or
	// fingerprinting degrades to the workflow name and collapses every failure
	// of one workflow into a single incident.
	ctx := context.Background()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
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

	callerMetadata := map[string]string{"repository": "example/example-api"}
	ev := Event{
		Source: "github", SourceRef: "workflow_run:1", Kind: "workflow_run.failed",
		ProjectSlug: "api", Title: "CI failed", Workflow: "CI",
		JobSteps:   []string{"test", "Run unit tests"},
		Metadata:   callerMetadata,
		OccurredAt: now,
	}

	w := NewIncidentWriter(db, func() time.Time { return now })
	if err := w.Handle(ctx, ev); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}

	incidents, _, err := store.ListIncidents(ctx, db, store.IncidentFilter{})
	if err != nil {
		t.Fatalf("ListIncidents() error = %v", err)
	}
	if len(incidents) != 1 {
		t.Fatalf("len(incidents) = %d, want 1", len(incidents))
	}

	got := incidents[0].Metadata["job_steps"]
	if want := "test\x1fRun unit tests"; got != want {
		t.Errorf("persisted job_steps = %q, want the unit-separated join %q", got, want)
	}
	if incidents[0].Metadata["repository"] != "example/example-api" {
		t.Error("the event's own metadata was lost when job_steps was added")
	}
	if len(callerMetadata) != 1 {
		t.Errorf("the caller's metadata map was mutated (now %v); it must be copied", callerMetadata)
	}
}

func TestNewSubscriberValidates(t *testing.T) {
	tests := []struct {
		name string
		opts SubscriberOptions
	}{
		{name: "no puller", opts: SubscriberOptions{Router: NewRouter(), Handler: &recordingHandler{}}},
		{name: "no router", opts: SubscriberOptions{Puller: &fakePuller{}, Handler: &recordingHandler{}}},
		{name: "no handler", opts: SubscriberOptions{Puller: &fakePuller{}, Router: NewRouter()}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSubscriber(tc.opts); err == nil {
				t.Error("NewSubscriber() error = nil, want error")
			}
		})
	}
}
