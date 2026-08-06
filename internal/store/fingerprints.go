package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrFingerprint is returned when a fingerprint cannot be read or recorded.
var ErrFingerprint = errors.New("fingerprint")

// FingerprintObservation is one sighting of a fingerprint.
type FingerprintObservation struct {
	Fingerprint string
	ProjectSlug string
	Strategy    string   // source_roots | denylist | all_frames | workflow | no_frames
	Frames      []string // the normalised frames that produced the hash
	IncidentID  int64
	Window      time.Duration
}

// FingerprintOutcome reports whether this sighting is suppressed.
type FingerprintOutcome struct {
	Suppressed       bool
	FirstIncidentID  int64
	TotalOccurrences int
	SuppressUntil    time.Time
}

// FingerprintRecord is a stored fingerprint with its evidence.
type FingerprintRecord struct {
	Fingerprint      string
	ProjectSlug      string
	Strategy         string
	Frames           []string
	FirstIncidentID  int64
	LastSeenAt       time.Time
	SuppressUntil    time.Time
	TotalOccurrences int
	RepairAttempts   int
}

// ObserveFingerprint records a sighting and reports whether it falls inside an
// open suppression window.
//
// The first sighting opens a window and reports Suppressed false. Sightings
// inside the window report true, increment total_occurrences, and cost nothing
// — this is what stops a crash loop emitting thousands of unique insertIds from
// becoming thousands of Tier 1 calls in M2, where (source, source_ref)
// deduplication cannot help because every entry genuinely is unique
// (SPEC §4.2.2, §4.3.2).
//
// Past the window the fingerprint reopens: a bug that recurs the next day is a
// new incident, not a footnote on a stale one. total_occurrences is a lifetime
// counter and deliberately does not reset.
//
// The read and the write share one transaction, so two concurrent sightings
// cannot both decide they are first.
func ObserveFingerprint(ctx context.Context, db *DB, o FingerprintObservation, now time.Time) (FingerprintOutcome, error) {
	frames, err := json.Marshal(nonNilFrames(o.Frames))
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: encoding frames: %w", ErrFingerprint, err)
	}

	tx, err := db.Writer().BeginTx(ctx, nil)
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: beginning transaction: %w", ErrFingerprint, err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		firstIncidentID  int64
		suppressUntilRaw string
		totalOccurrences int
	)
	err = tx.QueryRowContext(ctx,
		`SELECT first_incident_id, suppress_until, total_occurrences
		   FROM fingerprints WHERE fingerprint = ?`, o.Fingerprint,
	).Scan(&firstIncidentID, &suppressUntilRaw, &totalOccurrences)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		out, err := insertFingerprint(ctx, tx, o, string(frames), now)
		if err != nil {
			return FingerprintOutcome{}, err
		}
		if err := tx.Commit(); err != nil {
			return FingerprintOutcome{}, fmt.Errorf("%w: committing: %w", ErrFingerprint, err)
		}
		return out, nil

	case err != nil:
		return FingerprintOutcome{}, fmt.Errorf("%w: reading %q: %w", ErrFingerprint, o.Fingerprint, err)
	}

	suppressUntil, err := time.Parse(time.RFC3339, suppressUntilRaw)
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: parsing suppress_until %q: %w",
			ErrFingerprint, suppressUntilRaw, err)
	}

	inWindow := now.Before(suppressUntil.UTC())
	newUntil := suppressUntil.UTC()
	if !inWindow {
		newUntil = now.UTC().Add(o.Window)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE fingerprints
		   SET total_occurrences = total_occurrences + 1,
		       last_seen_at      = ?,
		       suppress_until    = ?,
		       strategy          = ?,
		       frames_json       = ?
		 WHERE fingerprint = ?`,
		now.UTC().Format(time.RFC3339), newUntil.Format(time.RFC3339),
		o.Strategy, string(frames), o.Fingerprint)
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: updating %q: %w", ErrFingerprint, o.Fingerprint, err)
	}

	if err := tx.Commit(); err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: committing: %w", ErrFingerprint, err)
	}

	return FingerprintOutcome{
		Suppressed:       inWindow,
		FirstIncidentID:  firstIncidentID,
		TotalOccurrences: totalOccurrences + 1,
		SuppressUntil:    newUntil,
	}, nil
}

func insertFingerprint(ctx context.Context, tx *sql.Tx, o FingerprintObservation, frames string, now time.Time) (FingerprintOutcome, error) {
	until := now.UTC().Add(o.Window)

	_, err := tx.ExecContext(ctx, `
		INSERT INTO fingerprints (
			fingerprint, project_slug, first_incident_id, last_seen_at,
			suppress_until, total_occurrences, strategy, frames_json
		) VALUES (?, ?, ?, ?, ?, 1, ?, ?)`,
		o.Fingerprint, o.ProjectSlug, o.IncidentID,
		now.UTC().Format(time.RFC3339), until.Format(time.RFC3339),
		o.Strategy, frames)
	if err != nil {
		return FingerprintOutcome{}, fmt.Errorf("%w: inserting %q: %w", ErrFingerprint, o.Fingerprint, err)
	}

	return FingerprintOutcome{
		Suppressed:       false,
		FirstIncidentID:  o.IncidentID,
		TotalOccurrences: 1,
		SuppressUntil:    until,
	}, nil
}

// GetFingerprint returns one stored fingerprint, reporting false when absent.
func GetFingerprint(ctx context.Context, db *DB, fingerprint string) (FingerprintRecord, bool, error) {
	var (
		rec        FingerprintRecord
		lastSeen   string
		until      string
		framesJSON string
	)
	err := db.Reader().QueryRowContext(ctx, `
		SELECT fingerprint, project_slug, first_incident_id, last_seen_at,
		       suppress_until, total_occurrences, repair_attempts, strategy, frames_json
		  FROM fingerprints WHERE fingerprint = ?`, fingerprint,
	).Scan(&rec.Fingerprint, &rec.ProjectSlug, &rec.FirstIncidentID, &lastSeen,
		&until, &rec.TotalOccurrences, &rec.RepairAttempts, &rec.Strategy, &framesJSON)

	if errors.Is(err, sql.ErrNoRows) {
		return FingerprintRecord{}, false, nil
	}
	if err != nil {
		return FingerprintRecord{}, false, fmt.Errorf("%w: getting %q: %w", ErrFingerprint, fingerprint, err)
	}

	if rec.LastSeenAt, err = time.Parse(time.RFC3339, lastSeen); err != nil {
		return FingerprintRecord{}, false, fmt.Errorf("%w: parsing last_seen_at: %w", ErrFingerprint, err)
	}
	if rec.SuppressUntil, err = time.Parse(time.RFC3339, until); err != nil {
		return FingerprintRecord{}, false, fmt.Errorf("%w: parsing suppress_until: %w", ErrFingerprint, err)
	}
	if err := json.Unmarshal([]byte(framesJSON), &rec.Frames); err != nil {
		return FingerprintRecord{}, false, fmt.Errorf("%w: decoding frames_json: %w", ErrFingerprint, err)
	}

	rec.LastSeenAt = rec.LastSeenAt.UTC()
	rec.SuppressUntil = rec.SuppressUntil.UTC()
	return rec, true, nil
}

// nonNilFrames guarantees json.Marshal emits [] rather than null, which the
// frames_json column's default and every reader assume.
func nonNilFrames(frames []string) []string {
	if frames == nil {
		return []string{}
	}
	return frames
}
