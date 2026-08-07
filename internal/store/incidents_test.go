package store

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"
)

func ingestFixture(t *testing.T) (*DB, context.Context, time.Time) {
	t.Helper()
	db, now := syncedDB(t)
	ctx := context.Background()
	err := SyncProjects(ctx, db, []ProjectRow{
		{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"},
	}, now)
	if err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}
	return db, ctx, now
}

func sampleParams() IngestParams {
	return IngestParams{
		ProjectSlug: "api",
		Source:      "gcplog",
		SourceRef:   "gcplog:insert-1",
		Kind:        "log.error",
		Title:       "TypeError: undefined is not a function",
		Body:        "at handler (src/index.js:12)",
		Metadata:    map[string]string{"severity": "ERROR"},
		State:       "received",
		OccurredAt:  time.Date(2026, 8, 2, 11, 0, 0, 0, time.UTC),
	}
}

func TestIngestIncidentInsertsThenDeduplicates(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	first, err := IngestIncident(ctx, db, sampleParams(), now)
	if err != nil {
		t.Fatalf("IngestIncident() error = %v, want nil", err)
	}
	if !first.IsNew {
		t.Error("IsNew = false on the first delivery, want true")
	}
	if first.OccurrenceCount != 1 {
		t.Errorf("OccurrenceCount = %d, want 1", first.OccurrenceCount)
	}
	if first.ID == 0 {
		t.Error("ID = 0, want an assigned rowid")
	}

	second, err := IngestIncident(ctx, db, sampleParams(), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("IngestIncident() error = %v, want nil", err)
	}
	if second.IsNew {
		t.Error("IsNew = true on a redelivery; idempotency must come from the unique index")
	}
	if second.ID != first.ID {
		t.Errorf("ID = %d, want the original %d", second.ID, first.ID)
	}
	if second.OccurrenceCount != 2 {
		t.Errorf("OccurrenceCount = %d, want 2", second.OccurrenceCount)
	}

	var rows int
	if err := db.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM incidents`).Scan(&rows); err != nil {
		t.Fatalf("counting incidents: %v", err)
	}
	if rows != 1 {
		t.Errorf("incident rows = %d, want 1", rows)
	}
}

func TestIngestIncidentStoresUnroutableAsNull(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	p := sampleParams()
	p.ProjectSlug = ""
	p.State = "filtered"
	p.StateReason = "unroutable"

	res, err := IngestIncident(ctx, db, p, now)
	if err != nil {
		t.Fatalf("IngestIncident() error = %v, want nil; an unroutable event must persist, not be dropped", err)
	}

	var slug sql.NullString
	var state, reason string
	err = db.Reader().QueryRowContext(ctx,
		`SELECT project_slug, state, COALESCE(state_reason, '') FROM incidents WHERE id = ?`, res.ID,
	).Scan(&slug, &state, &reason)
	if err != nil {
		t.Fatalf("reading incident: %v", err)
	}
	if slug.Valid {
		t.Errorf("project_slug = %q, want NULL", slug.String)
	}
	if state != "filtered" || reason != "unroutable" {
		t.Errorf("state/reason = %q/%q, want filtered/unroutable", state, reason)
	}
}

func TestIngestIncidentSeparatesSourceNamespaces(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	a := sampleParams()
	a.Source, a.SourceRef = "gcplog", "42"
	b := sampleParams()
	b.Source, b.SourceRef = "github", "42"

	if _, err := IngestIncident(ctx, db, a, now); err != nil {
		t.Fatalf("IngestIncident(a) error = %v", err)
	}
	res, err := IngestIncident(ctx, db, b, now)
	if err != nil {
		t.Fatalf("IngestIncident(b) error = %v", err)
	}
	if !res.IsNew {
		t.Error("the same ref under a different source collapsed; the unique index is on (source, source_ref)")
	}
}

func TestIngestIncidentSerialisesMetadata(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	t.Run("nil metadata becomes an empty object", func(t *testing.T) {
		p := sampleParams()
		p.SourceRef, p.Metadata = "gcplog:nil-meta", nil
		res, err := IngestIncident(ctx, db, p, now)
		if err != nil {
			t.Fatalf("IngestIncident() error = %v", err)
		}
		var raw string
		if err := db.Reader().QueryRowContext(ctx,
			`SELECT metadata_json FROM incidents WHERE id = ?`, res.ID).Scan(&raw); err != nil {
			t.Fatalf("reading metadata: %v", err)
		}
		if raw != "{}" {
			t.Errorf("metadata_json = %q, want %q; the column is NOT NULL", raw, "{}")
		}
	})
}

func TestIngestIncidentConcurrentDuplicatesCollapse(t *testing.T) {
	db, ctx, now := ingestFixture(t)

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := IngestIncident(ctx, db, sampleParams(), now); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent IngestIncident() error = %v, want nil", err)
	}

	var rows, count int
	err := db.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*), MAX(occurrence_count) FROM incidents`).Scan(&rows, &count)
	if err != nil {
		t.Fatalf("counting: %v", err)
	}
	if rows != 1 {
		t.Errorf("incident rows = %d, want 1", rows)
	}
	if count != n {
		t.Errorf("occurrence_count = %d, want %d; no delivery may be lost", count, n)
	}
}
