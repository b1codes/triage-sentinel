package store

import (
	"context"
	"testing"
	"time"
)

func syncedDB(t *testing.T) (*DB, time.Time) {
	t.Helper()
	db := openTemp(t)
	if _, err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate() error = %v, want nil", err)
	}
	return db, time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
}

func TestSyncProjectsInsertsAndUpdates(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	rows := []ProjectRow{
		{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"},
		{Slug: "worker", Repo: "github.com/o/worker", DefaultBranch: "main"},
	}
	if err := SyncProjects(ctx, db, rows, now); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	got, err := ListProjects(ctx, db)
	if err != nil {
		t.Fatalf("ListProjects() error = %v, want nil", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(ListProjects()) = %d, want 2", len(got))
	}
	for _, p := range got {
		if !p.Active {
			t.Errorf("project %q Active = false, want true", p.Slug)
		}
	}

	t.Run("re-sync updates repo without duplicating", func(t *testing.T) {
		rows[0].Repo = "github.com/o/api-renamed"
		if err := SyncProjects(ctx, db, rows, now.Add(time.Hour)); err != nil {
			t.Fatalf("SyncProjects() error = %v, want nil", err)
		}
		again, err := ListProjects(ctx, db)
		if err != nil {
			t.Fatalf("ListProjects() error = %v, want nil", err)
		}
		if len(again) != 2 {
			t.Fatalf("len = %d, want 2; sync must upsert rather than insert", len(again))
		}
		p, ok, err := GetProject(ctx, db, "api")
		if err != nil || !ok {
			t.Fatalf("GetProject(api) = %v, %v, want found", ok, err)
		}
		if p.Repo != "github.com/o/api-renamed" {
			t.Errorf("Repo = %q, want the updated value", p.Repo)
		}
	})
}

func TestSyncProjectsDeactivatesRatherThanDeletes(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	both := []ProjectRow{
		{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"},
		{Slug: "worker", Repo: "github.com/o/worker", DefaultBranch: "main"},
	}
	if err := SyncProjects(ctx, db, both, now); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	// worker is removed from the registry.
	if err := SyncProjects(ctx, db, both[:1], now.Add(time.Hour)); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	worker, ok, err := GetProject(ctx, db, "worker")
	if err != nil {
		t.Fatalf("GetProject() error = %v, want nil", err)
	}
	if !ok {
		t.Fatal("worker row was deleted; SPEC §4.1 requires incident history to survive deregistration")
	}
	if worker.Active {
		t.Error("worker Active = true, want false after removal from the registry")
	}

	t.Run("re-adding reactivates", func(t *testing.T) {
		if err := SyncProjects(ctx, db, both, now.Add(2*time.Hour)); err != nil {
			t.Fatalf("SyncProjects() error = %v, want nil", err)
		}
		w, _, err := GetProject(ctx, db, "worker")
		if err != nil {
			t.Fatalf("GetProject() error = %v, want nil", err)
		}
		if !w.Active {
			t.Error("worker Active = false, want true after being re-registered")
		}
	})
}

func TestSyncProjectsPreservesQuarantine(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	rows := []ProjectRow{{Slug: "api", Repo: "github.com/o/api", DefaultBranch: "main"}}
	if err := SyncProjects(ctx, db, rows, now); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	// A breaker quarantines the project (M2 owns the breaker; this simulates it).
	_, err := db.Writer().ExecContext(ctx,
		`UPDATE projects SET quarantined = 1, quarantine_reason = 'consecutive_failures' WHERE slug = 'api'`)
	if err != nil {
		t.Fatalf("quarantining: %v", err)
	}

	if err := SyncProjects(ctx, db, rows, now.Add(time.Hour)); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	p, _, err := GetProject(ctx, db, "api")
	if err != nil {
		t.Fatalf("GetProject() error = %v, want nil", err)
	}
	if !p.Quarantined {
		t.Error("Quarantined = false; a SIGHUP reload must not silently clear a breaker")
	}
	if p.QuarantineReason != "consecutive_failures" {
		t.Errorf("QuarantineReason = %q, want it preserved", p.QuarantineReason)
	}
}

func TestSyncProjectsIsAtomic(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	// Force the second insert to fail once the first has already succeeded.
	// A duplicate slug cannot do this: ON CONFLICT(slug) absorbs it as an
	// upsert by design. A duplicate repo can, because the added index is not
	// the conflict target, so the statement raises instead of updating.
	_, err := db.Writer().ExecContext(ctx,
		`CREATE UNIQUE INDEX test_unique_repo ON projects(repo)`)
	if err != nil {
		t.Fatalf("creating the forcing index: %v", err)
	}

	rows := []ProjectRow{
		{Slug: "api", Repo: "github.com/o/same", DefaultBranch: "main"},
		{Slug: "worker", Repo: "github.com/o/same", DefaultBranch: "main"},
	}
	if err := SyncProjects(ctx, db, rows, now); err == nil {
		t.Fatal("SyncProjects() error = nil, want the forced constraint violation")
	}

	got, err := ListProjects(ctx, db)
	if err != nil {
		t.Fatalf("ListProjects() error = %v, want nil", err)
	}
	if len(got) != 0 {
		t.Errorf("ListProjects() returned %d rows, want 0; the first insert survived a failed sync, so SyncProjects is not one transaction", len(got))
	}
}

func TestSyncProjectsDuplicateSlugDoesNotDuplicateRows(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	// The registry rejects duplicate slugs before this point, so this pins
	// the upsert's behaviour rather than a reachable path: last write wins
	// and no second row appears.
	rows := []ProjectRow{
		{Slug: "api", Repo: "github.com/o/first", DefaultBranch: "main"},
		{Slug: "api", Repo: "github.com/o/second", DefaultBranch: "main"},
	}
	if err := SyncProjects(ctx, db, rows, now); err != nil {
		t.Fatalf("SyncProjects() error = %v, want nil", err)
	}

	got, err := ListProjects(ctx, db)
	if err != nil {
		t.Fatalf("ListProjects() error = %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(ListProjects()) = %d, want 1", len(got))
	}
	if got[0].Repo != "github.com/o/second" {
		t.Errorf("Repo = %q, want the last write to win", got[0].Repo)
	}
}
