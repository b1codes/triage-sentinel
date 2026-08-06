package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestMigrateFreshDatabase(t *testing.T) {
	db := openTemp(t)
	ctx := context.Background()

	if got, err := SchemaVersion(ctx, db); err != nil || got != 0 {
		t.Fatalf("SchemaVersion() on fresh db = %d, %v; want 0, nil", got, err)
	}

	applied, err := Migrate(ctx, db)
	if err != nil {
		t.Fatalf("Migrate() error = %v, want nil", err)
	}
	// Bump this with every migration added: a fresh database applies all of
	// them and lands on the latest version.
	if applied != 2 {
		t.Errorf("Migrate() applied = %d, want 2", applied)
	}

	got, err := SchemaVersion(ctx, db)
	if err != nil {
		t.Fatalf("SchemaVersion() error = %v", err)
	}
	if got != 2 {
		t.Errorf("SchemaVersion() = %d, want 2", got)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := openTemp(t)
	ctx := context.Background()

	if _, err := Migrate(ctx, db); err != nil {
		t.Fatalf("first Migrate() error = %v", err)
	}

	applied, err := Migrate(ctx, db)
	if err != nil {
		t.Fatalf("second Migrate() error = %v, want nil", err)
	}
	if applied != 0 {
		t.Errorf("second Migrate() applied = %d, want 0", applied)
	}
}

func TestMigrateCreatesEverySpecTable(t *testing.T) {
	db := openTemp(t)
	if _, err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	tables := []string{
		"agent_runs", "budget_alerts", "budget_ledger", "budget_windows",
		"fingerprints", "incident_events", "incidents", "ingest_cursor",
		"patches", "projects", "schema_migrations", "settings",
	}
	for _, name := range tables {
		t.Run(name, func(t *testing.T) {
			var found string
			err := db.Reader().QueryRow(
				`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, name,
			).Scan(&found)
			if err != nil {
				t.Errorf("table %s missing: %v", name, err)
			}
		})
	}
}

func TestMigrateCreatesEverySpecIndex(t *testing.T) {
	db := openTemp(t)
	if _, err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	indexes := []string{
		"agent_runs_incident", "agent_runs_live", "budget_alerts_once",
		"budget_ledger_ts", "incident_events_incident", "incidents_fingerprint",
		"incidents_project", "incidents_source_ref", "incidents_state",
		"patches_incident",
	}
	for _, name := range indexes {
		t.Run(name, func(t *testing.T) {
			var found string
			err := db.Reader().QueryRow(
				`SELECT name FROM sqlite_master WHERE type='index' AND name=?`, name,
			).Scan(&found)
			if err != nil {
				t.Errorf("index %s missing: %v", name, err)
			}
		})
	}
}

func TestMigrateEnforcesUniqueSourceRef(t *testing.T) {
	db := openTemp(t)
	ctx := context.Background()
	if _, err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	insert := `INSERT INTO incidents
	  (source, source_ref, kind, title, state, occurred_at, created_at, updated_at)
	  VALUES ('github', 'workflow_run:1', 'workflow_run.failed', 't', 'received',
	          '2026-07-26T00:00:00Z', '2026-07-26T00:00:00Z', '2026-07-26T00:00:00Z')`

	if _, err := db.Writer().ExecContext(ctx, insert); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if _, err := db.Writer().ExecContext(ctx, insert); err == nil {
		t.Error("duplicate (source, source_ref) was accepted; the unique index is the system's idempotency guarantee")
	}
}

func TestMigrateEnforcesForeignKeys(t *testing.T) {
	db := openTemp(t)
	ctx := context.Background()
	if _, err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	_, err := db.Writer().ExecContext(ctx,
		`INSERT INTO incident_events (incident_id, ts, kind, actor)
		 VALUES (999, '2026-07-26T00:00:00Z', 'state_change', 'system')`)
	if err == nil {
		t.Error("incident_events row with a dangling incident_id was accepted; foreign_keys is not enforced")
	}
}

func TestMigrateFSRollsBackAFailingMigration(t *testing.T) {
	db := openTemp(t)
	ctx := context.Background()

	fsys := fstest.MapFS{
		"migrations/0001_good.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE good (id INTEGER PRIMARY KEY);`),
		},
		"migrations/0002_bad.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE partial (id INTEGER PRIMARY KEY);
			              THIS IS NOT SQL;`),
		},
	}

	_, err := MigrateFS(ctx, db, fsys)
	if err == nil {
		t.Fatal("MigrateFS() error = nil, want error")
	}
	if !errors.Is(err, ErrMigrate) {
		t.Errorf("errors.Is(err, ErrMigrate) = false, want true (err = %v)", err)
	}

	if got, _ := SchemaVersion(ctx, db); got != 1 {
		t.Errorf("SchemaVersion() = %d, want 1 (0002 must roll back entirely)", got)
	}

	var name string
	if err := db.Reader().QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name='partial'`,
	).Scan(&name); err == nil {
		t.Error("table 'partial' exists; the failing migration was not applied atomically")
	}
}

func TestMigrateFSRejectsBadFilenames(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		wantText string
	}{
		{name: "no version prefix", file: "migrations/init.sql", wantText: "filename"},
		{name: "three digits", file: "migrations/001_init.sql", wantText: "filename"},
		{name: "uppercase name", file: "migrations/0001_Init.sql", wantText: "filename"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTemp(t)
			fsys := fstest.MapFS{
				tc.file: &fstest.MapFile{Data: []byte(`CREATE TABLE t (id INTEGER PRIMARY KEY);`)},
			}
			_, err := MigrateFS(context.Background(), db, fsys)
			if err == nil {
				t.Fatal("MigrateFS() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.wantText)
			}
		})
	}
}

func TestMigrateFSRejectsNonContiguousVersions(t *testing.T) {
	db := openTemp(t)
	fsys := fstest.MapFS{
		"migrations/0001_a.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE a (id INTEGER PRIMARY KEY);`)},
		"migrations/0003_c.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE c (id INTEGER PRIMARY KEY);`)},
	}

	_, err := MigrateFS(context.Background(), db, fsys)
	if err == nil {
		t.Fatal("MigrateFS() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "contiguous") {
		t.Errorf("error %q does not mention contiguity; a renumbered or missing migration must be caught", err.Error())
	}
}

func TestMigrateOnClosedDatabase(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sentinel.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := Migrate(context.Background(), db); err == nil {
		t.Error("Migrate() on a closed database returned nil error")
	}
}

func TestMigrate0002AddsFingerprintEvidence(t *testing.T) {
	db := openTemp(t)

	if _, err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate() error = %v, want nil", err)
	}

	version, err := SchemaVersion(context.Background(), db)
	if err != nil {
		t.Fatalf("SchemaVersion() error = %v, want nil", err)
	}
	if version < 2 {
		t.Fatalf("SchemaVersion() = %d, want at least 2", version)
	}

	t.Run("columns exist with safe defaults", func(t *testing.T) {
		_, err := db.Writer().Exec(`
			INSERT INTO projects (slug, repo, default_branch, created_at, updated_at)
			VALUES ('p', 'github.com/o/p', 'main', '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z')`)
		if err != nil {
			t.Fatalf("seeding project: %v", err)
		}
		_, err = db.Writer().Exec(`
			INSERT INTO incidents (project_slug, source, source_ref, kind, title, state, occurred_at, created_at, updated_at)
			VALUES ('p', 'gcplog', 'gcplog:1', 'log.error', 't', 'received', '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z')`)
		if err != nil {
			t.Fatalf("seeding incident: %v", err)
		}
		// Deliberately omit strategy and frames_json to prove the defaults apply.
		_, err = db.Writer().Exec(`
			INSERT INTO fingerprints (fingerprint, project_slug, first_incident_id, last_seen_at, suppress_until)
			VALUES ('abc', 'p', 1, '2026-08-02T00:00:00Z', '2026-08-02T06:00:00Z')`)
		if err != nil {
			t.Fatalf("inserting fingerprint: %v", err)
		}

		var strategy, frames string
		err = db.Reader().QueryRow(
			`SELECT strategy, frames_json FROM fingerprints WHERE fingerprint = 'abc'`,
		).Scan(&strategy, &frames)
		if err != nil {
			t.Fatalf("selecting evidence columns: %v", err)
		}
		if strategy != "unknown" {
			t.Errorf("strategy default = %q, want %q", strategy, "unknown")
		}
		if frames != "[]" {
			t.Errorf("frames_json default = %q, want %q", frames, "[]")
		}
	})
}
