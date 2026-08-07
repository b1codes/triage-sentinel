package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrStaleTransition is returned when an incident's state no longer matches the
// expected from-state, meaning another worker moved it first.
var ErrStaleTransition = errors.New("incident state changed concurrently")

// ErrEvents is returned when the incident_events table cannot be read or written.
var ErrEvents = errors.New("incident events")

// emptyPayload is the NOT NULL default for payload_json.
const emptyPayload = "{}"

// terminalStates never transition again, so reaching one stamps closed_at.
// parked is deliberately absent: SPEC §3 calls it non-terminal and resumable.
var terminalStates = map[string]bool{
	"filtered": true, "suppressed": true, "dismissed": true, "merged": true,
	"deployed": true, "rejected": true, "aborted": true, "failed": true,
	"escalated": true,
}

// IsTerminalState reports whether a state is final (SPEC §3).
func IsTerminalState(state string) bool { return terminalStates[state] }

// IncidentEvent is one row of the append-only audit trail. Its id is also the
// SSE Last-Event-ID sequence, so replay and the audit trail cannot diverge
// (SPEC §3, §4.11).
type IncidentEvent struct {
	ID         int64
	IncidentID int64
	TS         time.Time
	Kind       string // state_change | note | cost | policy | abort
	Actor      string // system | tier0 | tier1 | tier2 | operator:<name>
	FromState  string
	ToState    string
	Payload    json.RawMessage
}

// TransitionParams describes one state change.
type TransitionParams struct {
	IncidentID int64
	From       string
	To         string
	Reason     string
	Actor      string
	Payload    json.RawMessage
}

// AppendEvent adds a row to the audit trail and returns its id.
func AppendEvent(ctx context.Context, db *DB, ev IncidentEvent, now time.Time) (int64, error) {
	id, err := appendEvent(ctx, db.Writer(), ev, now)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// execer is satisfied by both *sql.DB and *sql.Tx, so the append logic is
// shared between the standalone and in-transaction paths.
type execer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func appendEvent(ctx context.Context, q execer, ev IncidentEvent, now time.Time) (int64, error) {
	payload := string(ev.Payload)
	if len(ev.Payload) == 0 {
		payload = emptyPayload
	}
	ts := now.UTC().Format(time.RFC3339)

	var id int64
	err := q.QueryRowContext(ctx, `
		INSERT INTO incident_events (incident_id, ts, kind, actor, from_state, to_state, payload_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		ev.IncidentID, ts, ev.Kind, ev.Actor,
		nullIfEmpty(ev.FromState), nullIfEmpty(ev.ToState), payload,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("%w: appending %s for incident %d: %w", ErrEvents, ev.Kind, ev.IncidentID, err)
	}
	return id, nil
}

// Transition moves an incident to a new state and appends the matching audit
// row in one transaction, returning the new event id for SSE publication.
//
// The UPDATE carries `AND state = ?`, so a row another worker already moved
// changes zero rows and yields ErrStaleTransition rather than an audit entry
// describing a transition that never happened. Two workers racing the same
// incident would otherwise both append plausible but contradictory histories.
func Transition(ctx context.Context, db *DB, t TransitionParams, now time.Time) (int64, error) {
	ts := now.UTC().Format(time.RFC3339)

	tx, err := db.Writer().BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("%w: beginning transaction: %w", ErrEvents, err)
	}
	defer func() { _ = tx.Rollback() }()

	closedAt := sql.NullString{String: ts, Valid: IsTerminalState(t.To)}

	res, err := tx.ExecContext(ctx, `
		UPDATE incidents
		   SET state = ?, state_reason = ?, updated_at = ?,
		       closed_at = CASE WHEN ? THEN ? ELSE closed_at END
		 WHERE id = ? AND state = ?`,
		t.To, nullIfEmpty(t.Reason), ts,
		closedAt.Valid, ts,
		t.IncidentID, t.From)
	if err != nil {
		return 0, fmt.Errorf("%w: updating incident %d: %w", ErrEvents, t.IncidentID, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%w: counting affected rows: %w", ErrEvents, err)
	}
	if affected == 0 {
		return 0, fmt.Errorf("%w: incident %d is no longer in state %q",
			ErrStaleTransition, t.IncidentID, t.From)
	}

	id, err := appendEvent(ctx, tx, IncidentEvent{
		IncidentID: t.IncidentID,
		Kind:       "state_change",
		Actor:      t.Actor,
		FromState:  t.From,
		ToState:    t.To,
		Payload:    t.Payload,
	}, now)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("%w: committing transition: %w", ErrEvents, err)
	}
	return id, nil
}

const eventColumns = `id, incident_id, ts, kind, actor,
	COALESCE(from_state, ''), COALESCE(to_state, ''), payload_json`

func scanEvents(rows *sql.Rows) ([]IncidentEvent, error) {
	var out []IncidentEvent
	for rows.Next() {
		var (
			ev      IncidentEvent
			ts      string
			payload string
		)
		if err := rows.Scan(&ev.ID, &ev.IncidentID, &ts, &ev.Kind, &ev.Actor,
			&ev.FromState, &ev.ToState, &payload); err != nil {
			return nil, fmt.Errorf("%w: scanning: %w", ErrEvents, err)
		}
		parsed, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return nil, fmt.Errorf("%w: parsing ts %q: %w", ErrEvents, ts, err)
		}
		ev.TS = parsed.UTC()
		ev.Payload = json.RawMessage(payload)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterating: %w", ErrEvents, err)
	}
	return out, nil
}

// EventsAfter returns audit rows with an id strictly greater than afterID, in
// id order. It backs SSE replay from Last-Event-ID (SPEC §4.11).
func EventsAfter(ctx context.Context, db *DB, afterID int64, limit int) ([]IncidentEvent, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT `+eventColumns+` FROM incident_events WHERE id > ? ORDER BY id LIMIT ?`,
		afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: replaying after %d: %w", ErrEvents, afterID, err)
	}
	defer func() { _ = rows.Close() }()
	return scanEvents(rows)
}

// EventsForIncident returns one incident's full timeline in id order.
func EventsForIncident(ctx context.Context, db *DB, incidentID int64) ([]IncidentEvent, error) {
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT `+eventColumns+` FROM incident_events WHERE incident_id = ? ORDER BY id`,
		incidentID)
	if err != nil {
		return nil, fmt.Errorf("%w: reading timeline for %d: %w", ErrEvents, incidentID, err)
	}
	defer func() { _ = rows.Close() }()
	return scanEvents(rows)
}

// nullIfEmpty maps "" to SQL NULL, keeping optional TEXT columns genuinely
// absent rather than storing an empty string that readers must special-case.
func nullIfEmpty(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
