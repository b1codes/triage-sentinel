package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrIngest is returned when an event cannot be recorded as an incident.
var ErrIngest = errors.New("ingesting incident")

// IngestParams is one normalised event on its way to becoming an incident.
type IngestParams struct {
	// ProjectSlug is empty for an unroutable event, which is stored as NULL
	// rather than dropped — an unroutable event usually means a stale
	// projects.yaml and must stay visible (SPEC §4.2).
	ProjectSlug string

	Source      string
	SourceRef   string
	Kind        string
	Title       string
	Body        string
	State       string
	StateReason string

	Metadata   map[string]string
	OccurredAt time.Time
}

// IngestResult reports what the write did.
type IngestResult struct {
	ID              int64
	IsNew           bool
	OccurrenceCount int
	State           string
}

// IngestIncident records an event, collapsing a redelivery into the existing
// row. Pub/Sub is at-least-once, so idempotency comes from the unique index on
// incidents(source, source_ref) rather than from the transport (SPEC §4.2).
//
// IsNew is derived from the returned occurrence_count rather than from
// changes(): SQLite cannot distinguish an insert from an ON CONFLICT update,
// and a SELECT beforehand would race a concurrent duplicate delivery. A fresh
// row always starts at 1 and the conflict branch always increments, so the
// returned count settles it in a single statement.
//
// The conflict branch deliberately leaves state, title and body alone. The
// first delivery's classification is authoritative; a redelivery is evidence
// of recurrence, not a correction.
func IngestIncident(ctx context.Context, db *DB, p IngestParams, now time.Time) (IngestResult, error) {
	metadata, err := marshalMetadata(p.Metadata)
	if err != nil {
		return IngestResult{}, err
	}

	slug := sql.NullString{String: p.ProjectSlug, Valid: p.ProjectSlug != ""}
	reason := sql.NullString{String: p.StateReason, Valid: p.StateReason != ""}
	ts := now.UTC().Format(time.RFC3339)
	occurred := p.OccurredAt.UTC().Format(time.RFC3339)

	var res IngestResult
	err = db.Writer().QueryRowContext(ctx, `
		INSERT INTO incidents (
			project_slug, source, source_ref, kind, title, body, metadata_json,
			state, state_reason, occurrence_count, occurred_at, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
		ON CONFLICT(source, source_ref) DO UPDATE SET
			occurrence_count = incidents.occurrence_count + 1,
			updated_at       = excluded.updated_at
		RETURNING id, occurrence_count, state`,
		slug, p.Source, p.SourceRef, p.Kind, p.Title, p.Body, metadata,
		p.State, reason, occurred, ts, ts,
	).Scan(&res.ID, &res.OccurrenceCount, &res.State)
	if err != nil {
		return IngestResult{}, fmt.Errorf("%w: %s/%s: %w", ErrIngest, p.Source, p.SourceRef, err)
	}

	res.IsNew = res.OccurrenceCount == 1
	return res, nil
}

// marshalMetadata renders metadata for the NOT NULL metadata_json column. A nil
// map becomes "{}" rather than "null", which would violate the column's
// contract and break every reader.
func marshalMetadata(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("%w: encoding metadata: %w", ErrIngest, err)
	}
	return string(encoded), nil
}
