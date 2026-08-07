package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// TouchIngestCursor records that a source successfully delivered at a point in
// time. A stalled subscriber is the most likely silent failure in the system —
// the process looks healthy while seeing nothing — so this timestamp is what
// makes that detectable (SPEC §12).
func TouchIngestCursor(ctx context.Context, db *DB, source string, at time.Time) error {
	_, err := db.Writer().ExecContext(ctx, `
		INSERT INTO ingest_cursor (source, last_seen_at)
		VALUES (?, ?)
		ON CONFLICT(source) DO UPDATE SET last_seen_at = excluded.last_seen_at`,
		source, at.UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("touching ingest cursor for %q: %w", source, err)
	}
	return nil
}

// LastIngestAt returns the most recent successful ingest across all sources,
// reporting false when nothing has ever been ingested.
func LastIngestAt(ctx context.Context, db *DB) (time.Time, bool, error) {
	var raw sql.NullString
	err := db.Reader().QueryRowContext(ctx,
		`SELECT MAX(last_seen_at) FROM ingest_cursor`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !raw.Valid) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("reading ingest cursor: %w", err)
	}

	parsed, err := time.Parse(time.RFC3339, raw.String)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parsing ingest cursor %q: %w", raw.String, err)
	}
	return parsed.UTC(), true, nil
}
