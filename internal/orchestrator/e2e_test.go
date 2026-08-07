package orchestrator

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/ingest"
	"github.com/b1codes/triage-sentinel/internal/store"
)

// logEntry builds a Cloud Logging entry for one occurrence of a crash loop.
// Every entry has a distinct insertId — which is exactly why source_ref
// deduplication cannot collapse a storm, and fingerprinting must.
func logEntry(t *testing.T, insertID string, lineNumber int) []byte {
	t.Helper()

	payload := map[string]any{
		"insertId":  insertID,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"severity":  "ERROR",
		"resource": map[string]any{
			"type":   "cloud_run_revision",
			"labels": map[string]string{"service_name": "api"},
		},
		"textPayload": fmt.Sprintf(
			"TypeError: Cannot read properties of undefined\n"+
				"    at handler (/app/src/index.js:12:%d)\n"+
				"    at Layer.handle (/app/node_modules/express/lib/router/layer.js:95:5)",
			lineNumber),
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encoding log entry: %v", err)
	}
	return encoded
}

// TestStormCollapsesToOneIncident is the milestone's acceptance test.
//
// 500 log entries with unique insertIds and one shared root cause must produce
// exactly one incident in triaging, with every other occurrence suppressed and
// the lifetime count preserved. If this fails, M2 pays for every occurrence of
// every crash loop.
func TestStormCollapsesToOneIncident(t *testing.T) {
	o, db, _, ctx, _ := fixture(t)

	resolver := ingest.NewRegistryResolver(registryForE2E(t))
	router := ingest.NewRouter(ingest.NewGCPLogAdapter(resolver))
	writer := ingest.NewIncidentWriter(db, time.Now)

	const storm = 500
	for i := range storm {
		message := ingest.Message{
			ID:          fmt.Sprintf("m-%d", i),
			Data:        logEntry(t, fmt.Sprintf("insert-%d", i), i),
			Attributes:  map[string]string{"logging.googleapis.com/timestamp": "x"},
			PublishTime: time.Now(),
		}

		event, err := router.Route(ctx, message)
		if err != nil {
			t.Fatalf("routing message %d: %v", i, err)
		}
		if err := writer.Handle(ctx, event); err != nil {
			t.Fatalf("persisting message %d: %v", i, err)
		}
	}

	// Drain the whole queue.
	for {
		moved, err := o.ProcessOnce(ctx)
		if err != nil {
			t.Fatalf("ProcessOnce() error = %v", err)
		}
		if moved == 0 {
			break
		}
	}

	counts, err := store.CountByState(ctx, db)
	if err != nil {
		t.Fatalf("CountByState() error = %v", err)
	}

	t.Run("exactly one incident is actionable", func(t *testing.T) {
		if counts["triaging"] != 1 {
			t.Errorf("triaging = %d, want 1; a storm must collapse to a single incident", counts["triaging"])
		}
	})

	t.Run("the rest are suppressed, not lost", func(t *testing.T) {
		if counts["suppressed"] != storm-1 {
			t.Errorf("suppressed = %d, want %d", counts["suppressed"], storm-1)
		}
	})

	t.Run("nothing is left queued", func(t *testing.T) {
		if counts["received"] != 0 {
			t.Errorf("received = %d, want 0", counts["received"])
		}
	})

	t.Run("the storm stays visible while being silent", func(t *testing.T) {
		incidents, _, err := store.ListIncidents(ctx, db, store.IncidentFilter{
			States: []string{"triaging"}, Limit: 1,
		})
		if err != nil || len(incidents) != 1 {
			t.Fatalf("ListIncidents() = %v, %v", len(incidents), err)
		}

		record, ok, err := store.GetFingerprint(ctx, db, incidents[0].Fingerprint)
		if err != nil || !ok {
			t.Fatalf("GetFingerprint() = %v, %v", ok, err)
		}
		if record.TotalOccurrences != storm {
			t.Errorf("TotalOccurrences = %d, want %d", record.TotalOccurrences, storm)
		}
		if record.Strategy != "denylist" {
			t.Errorf("Strategy = %q, want denylist; the app frame should have been selected over the express frame", record.Strategy)
		}
	})
}

// TestDistinctBugsDoNotCollapse is the counterweight. Suppression that is too
// aggressive silently swallows real failures, and nothing else in the system
// catches that.
func TestDistinctBugsDoNotCollapse(t *testing.T) {
	o, db, _, ctx, _ := fixture(t)

	resolver := ingest.NewRegistryResolver(registryForE2E(t))
	router := ingest.NewRouter(ingest.NewGCPLogAdapter(resolver))
	writer := ingest.NewIncidentWriter(db, time.Now)

	bodies := []string{
		"TypeError: Cannot read properties of undefined\n    at handler (/app/src/index.js:12:9)",
		"RangeError: Maximum call stack size exceeded\n    at recurse (/app/src/tree.js:88:3)",
	}

	for i, body := range bodies {
		payload, err := json.Marshal(map[string]any{
			"insertId":  fmt.Sprintf("distinct-%d", i),
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"severity":  "ERROR",
			"resource": map[string]any{
				"labels": map[string]string{"service_name": "api"},
			},
			"textPayload": body,
		})
		if err != nil {
			t.Fatalf("encoding: %v", err)
		}

		event, err := router.Route(ctx, ingest.Message{
			ID: fmt.Sprintf("m-%d", i), Data: payload,
			Attributes: map[string]string{"logging.googleapis.com/timestamp": "x"},
		})
		if err != nil {
			t.Fatalf("routing: %v", err)
		}
		if err := writer.Handle(ctx, event); err != nil {
			t.Fatalf("persisting: %v", err)
		}
	}

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	counts, err := store.CountByState(ctx, db)
	if err != nil {
		t.Fatalf("CountByState() error = %v", err)
	}
	if counts["triaging"] != 2 {
		t.Errorf("triaging = %d, want 2; two distinct bugs were collapsed into one and the second was silently suppressed", counts["triaging"])
	}
}

// TestRestartResumesTheQueue proves crash recovery needs no mechanism: the
// queue is rows, so a new orchestrator picks up exactly where the old one
// stopped (SPEC §4.12).
func TestRestartResumesTheQueue(t *testing.T) {
	o, db, hub, ctx, now := fixture(t)

	for i := range 3 {
		seed(t, db, ctx, now, store.IngestParams{
			ProjectSlug: "api",
			SourceRef:   fmt.Sprintf("resume-%d", i),
			Title:       fmt.Sprintf("TypeError: distinct %d", i),
			Body:        fmt.Sprintf("at handler (src/file%d.js:1)", i),
		})
	}

	// Process with a batch size of one, then discard the orchestrator.
	small, err := New(Deps{
		DB: db, Hub: hub, Chain: o.deps.Chain,
		Registry: o.deps.Registry, Logger: o.deps.Logger,
		Clock: o.deps.Clock, BatchSize: 1,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := small.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	// A fresh orchestrator drains the remainder.
	for {
		moved, err := o.ProcessOnce(ctx)
		if err != nil {
			t.Fatalf("ProcessOnce() error = %v", err)
		}
		if moved == 0 {
			break
		}
	}

	counts, err := store.CountByState(ctx, db)
	if err != nil {
		t.Fatalf("CountByState() error = %v", err)
	}
	if counts["received"] != 0 {
		t.Errorf("received = %d, want 0; a restart must resume the queue", counts["received"])
	}
}
