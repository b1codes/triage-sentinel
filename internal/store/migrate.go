package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// ErrMigrate is returned when the schema cannot be migrated.
var ErrMigrate = errors.New("migrating database")

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationFilename matches NNNN_lower_snake_name.sql. The four-digit prefix is
// the version; a filename that does not match is rejected rather than skipped,
// so a typo cannot silently drop a migration.
var migrationFilename = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)

const createSchemaMigrations = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version    INTEGER PRIMARY KEY,
  name       TEXT NOT NULL,
  applied_at TEXT NOT NULL
)`

type migration struct {
	version int
	name    string
	sql     string
}

// Migrate applies every pending embedded migration and returns how many ran.
// Migrations are forward-only: there is no down path, because rolling a schema
// backwards on a live incident database is more dangerous than fixing forward.
func Migrate(ctx context.Context, db *DB) (int, error) {
	return MigrateFS(ctx, db, migrationsFS)
}

// MigrateFS applies pending migrations read from fsys. Migrate delegates to it;
// tests use it to exercise failure paths without shipping broken SQL.
func MigrateFS(ctx context.Context, db *DB, fsys fs.FS) (int, error) {
	if _, err := db.Writer().ExecContext(ctx, createSchemaMigrations); err != nil {
		return 0, fmt.Errorf("%w: creating schema_migrations: %w", ErrMigrate, err)
	}

	migrations, err := loadMigrations(fsys)
	if err != nil {
		return 0, err
	}

	current, err := SchemaVersion(ctx, db)
	if err != nil {
		return 0, err
	}

	applied := 0
	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		if err := applyOne(ctx, db, m); err != nil {
			return applied, err
		}
		applied++
	}
	return applied, nil
}

// applyOne runs a single migration and records it in the same transaction, so a
// migration and its version marker can never disagree.
func applyOne(ctx context.Context, db *DB, m migration) error {
	tx, err := db.Writer().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: beginning transaction for %04d_%s: %w",
			ErrMigrate, m.version, m.name, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, m.sql); err != nil {
		return fmt.Errorf("%w: applying %04d_%s: %w", ErrMigrate, m.version, m.name, err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`,
		m.version, m.name, time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("%w: recording %04d_%s: %w", ErrMigrate, m.version, m.name, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: committing %04d_%s: %w", ErrMigrate, m.version, m.name, err)
	}
	return nil
}

// SchemaVersion returns the highest applied migration version, or 0 when none
// have been applied.
func SchemaVersion(ctx context.Context, db *DB) (int, error) {
	if _, err := db.Writer().ExecContext(ctx, createSchemaMigrations); err != nil {
		return 0, fmt.Errorf("%w: creating schema_migrations: %w", ErrMigrate, err)
	}

	var version *int
	if err := db.Reader().QueryRowContext(ctx,
		`SELECT max(version) FROM schema_migrations`).Scan(&version); err != nil {
		return 0, fmt.Errorf("%w: reading schema version: %w", ErrMigrate, err)
	}
	if version == nil {
		return 0, nil
	}
	return *version, nil
}

func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, "migrations")
	if err != nil {
		return nil, fmt.Errorf("%w: reading migrations directory: %w", ErrMigrate, err)
	}

	var migrations []migration
	seen := make(map[int]string)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()

		match := migrationFilename.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf(
				"%w: filename %q must match NNNN_lower_snake_name.sql", ErrMigrate, name)
		}

		version, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("%w: filename %q has an unparseable version: %w",
				ErrMigrate, name, err)
		}
		if version < 1 {
			return nil, fmt.Errorf("%w: filename %q version must be >= 1", ErrMigrate, name)
		}
		if prior, dup := seen[version]; dup {
			return nil, fmt.Errorf("%w: version %d used by both %q and %q",
				ErrMigrate, version, prior, name)
		}
		seen[version] = name

		body, err := fs.ReadFile(fsys, path.Join("migrations", name))
		if err != nil {
			return nil, fmt.Errorf("%w: reading %s: %w", ErrMigrate, name, err)
		}

		migrations = append(migrations, migration{
			version: version,
			name:    name,
			sql:     string(body),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})

	// Contiguity from 1 catches a deleted or renumbered migration, which would
	// otherwise let a database skip a schema change and appear up to date.
	for i, m := range migrations {
		if want := i + 1; m.version != want {
			return nil, fmt.Errorf(
				"%w: migration versions must be contiguous from 1; expected %04d, found %04d (%s)",
				ErrMigrate, want, m.version, m.name)
		}
	}

	return migrations, nil
}
