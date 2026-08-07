package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrSyncProjects is returned when the registry cannot be reconciled with the
// projects table.
var ErrSyncProjects = errors.New("syncing projects")

// ProjectRow is the registry's view of a project — the fields projects.yaml
// owns. Everything else in the projects table is state the sentinel maintains.
type ProjectRow struct {
	Slug          string
	Repo          string
	DefaultBranch string
}

// Project is the database's view of a project, including operational state the
// registry does not describe.
type Project struct {
	Slug                string
	Repo                string
	DefaultBranch       string
	Active              bool
	Quarantined         bool
	QuarantineReason    string
	ConsecutiveFailures int
	IncidentsSeen       int
}

// SyncProjects reconciles the projects table with the registry. Registered
// slugs are upserted with active = 1; rows whose slug is no longer registered
// are set to active = 0.
//
// Rows are never deleted. SPEC §4.1 requires that removing a project from the
// registry preserves its incident history, and incidents.project_slug is a
// foreign key into this table — a delete would either fail or orphan history.
//
// The upsert deliberately touches only the registry-owned columns. Quarantine
// state and failure counters are maintained by breakers, and a SIGHUP reload
// must not silently clear a breaker that is holding a project back.
//
// The whole reconciliation runs in one transaction, so a malformed registry
// cannot leave the table half-updated.
func SyncProjects(ctx context.Context, db *DB, rows []ProjectRow, now time.Time) error {
	ts := now.UTC().Format(time.RFC3339)

	tx, err := db.Writer().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: beginning transaction: %w", ErrSyncProjects, err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, r := range rows {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO projects (slug, repo, default_branch, active, created_at, updated_at)
			VALUES (?, ?, ?, 1, ?, ?)
			ON CONFLICT(slug) DO UPDATE SET
				repo           = excluded.repo,
				default_branch = excluded.default_branch,
				active         = 1,
				updated_at     = excluded.updated_at`,
			r.Slug, r.Repo, r.DefaultBranch, ts, ts)
		if err != nil {
			return fmt.Errorf("%w: upserting %q: %w", ErrSyncProjects, r.Slug, err)
		}
	}

	if err := deactivateUnlisted(ctx, tx, rows, ts); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: committing: %w", ErrSyncProjects, err)
	}
	return nil
}

// deactivateUnlisted sets active = 0 on every row whose slug is absent from
// rows. It builds the NOT IN list with placeholders rather than string
// interpolation, so a slug can never be injected into the statement.
func deactivateUnlisted(ctx context.Context, tx *sql.Tx, rows []ProjectRow, ts string) error {
	if len(rows) == 0 {
		_, err := tx.ExecContext(ctx,
			`UPDATE projects SET active = 0, updated_at = ? WHERE active = 1`, ts)
		if err != nil {
			return fmt.Errorf("%w: deactivating all: %w", ErrSyncProjects, err)
		}
		return nil
	}

	args := make([]any, 0, len(rows)+1)
	args = append(args, ts)
	placeholders := make([]byte, 0, len(rows)*2)
	for i, r := range rows {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, r.Slug)
	}

	query := `UPDATE projects SET active = 0, updated_at = ?
	          WHERE active = 1 AND slug NOT IN (` + string(placeholders) + `)`

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%w: deactivating unlisted: %w", ErrSyncProjects, err)
	}
	return nil
}

const projectColumns = `slug, repo, default_branch, active, quarantined,
	COALESCE(quarantine_reason, ''), consecutive_failures, incidents_seen`

func scanProject(scan func(...any) error) (Project, error) {
	var p Project
	err := scan(&p.Slug, &p.Repo, &p.DefaultBranch, &p.Active, &p.Quarantined,
		&p.QuarantineReason, &p.ConsecutiveFailures, &p.IncidentsSeen)
	return p, err
}

// ListProjects returns every project, active or not, ordered by slug.
func ListProjects(ctx context.Context, db *DB) ([]Project, error) {
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT `+projectColumns+` FROM projects ORDER BY slug`)
	if err != nil {
		return nil, fmt.Errorf("listing projects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Project
	for rows.Next() {
		p, err := scanProject(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scanning project: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating projects: %w", err)
	}
	return out, nil
}

// GetProject returns one project by slug, reporting false when absent.
func GetProject(ctx context.Context, db *DB, slug string) (Project, bool, error) {
	row := db.Reader().QueryRowContext(ctx,
		`SELECT `+projectColumns+` FROM projects WHERE slug = ?`, slug)

	p, err := scanProject(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Project{}, false, nil
	}
	if err != nil {
		return Project{}, false, fmt.Errorf("getting project %q: %w", slug, err)
	}
	return p, true, nil
}
