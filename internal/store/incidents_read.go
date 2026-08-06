package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// defaultIncidentLimit bounds an unfiltered page so a dashboard request can
// never pull the whole table into memory.
const defaultIncidentLimit = 100

// Incident is a stored incident.
type Incident struct {
	ID              int64
	ProjectSlug     string // empty when unroutable
	Source          string
	SourceRef       string
	Kind            string
	Fingerprint     string
	Title           string
	Body            string
	State           string
	StateReason     string
	Category        string
	Tier            int
	Confidence      *float64
	OccurrenceCount int
	CostUSD         float64
	Metadata        map[string]string
	OccurredAt      time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ClosedAt        *time.Time
}

// IncidentFilter narrows a listing. Empty slices and nil times mean "any".
type IncidentFilter struct {
	States   []string
	Projects []string
	Sources  []string
	Since    *time.Time
	Until    *time.Time
	Limit    int
	Offset   int

	// OldestFirst reverses the default newest-first ordering.
	//
	// The dashboard wants newest-first; a work queue must not. The process
	// loop drains state='received' FIFO for two reasons: the earliest
	// occurrence of a bug should be the one that opens the suppression
	// window and becomes canonical, and under sustained load a newest-first
	// batch would starve the oldest rows indefinitely.
	OldestFirst bool
}

// orderBy renders the ORDER BY clause for the filter's direction.
func (f IncidentFilter) orderBy() string {
	if f.OldestFirst {
		return ` ORDER BY created_at ASC, id ASC`
	}
	return ` ORDER BY created_at DESC, id DESC`
}

// where builds the shared predicate and its arguments. Every value is bound as
// a placeholder, so a caller-supplied state or slug can never alter the query.
func (f IncidentFilter) where() (string, []any) {
	var clauses []string
	var args []any

	inClause := func(column string, values []string) {
		if len(values) == 0 {
			return
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")
		clauses = append(clauses, column+" IN ("+placeholders+")")
		for _, v := range values {
			args = append(args, v)
		}
	}

	inClause("state", f.States)
	inClause("project_slug", f.Projects)
	inClause("source", f.Sources)

	if f.Since != nil {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, f.Since.UTC().Format(time.RFC3339))
	}
	if f.Until != nil {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, f.Until.UTC().Format(time.RFC3339))
	}

	if len(clauses) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

const incidentColumns = `id, COALESCE(project_slug, ''), source, source_ref, kind,
	COALESCE(fingerprint, ''), title, COALESCE(body, ''), metadata_json,
	state, COALESCE(state_reason, ''), tier, confidence, COALESCE(category, ''),
	occurrence_count, cost_usd, occurred_at, created_at, updated_at, closed_at`

func scanIncident(scan func(...any) error) (Incident, error) {
	var (
		in           Incident
		metadataJSON string
		occurred     string
		created      string
		updated      string
		closed       sql.NullString
	)
	err := scan(&in.ID, &in.ProjectSlug, &in.Source, &in.SourceRef, &in.Kind,
		&in.Fingerprint, &in.Title, &in.Body, &metadataJSON,
		&in.State, &in.StateReason, &in.Tier, &in.Confidence, &in.Category,
		&in.OccurrenceCount, &in.CostUSD, &occurred, &created, &updated, &closed)
	if err != nil {
		return Incident{}, err
	}

	if err := json.Unmarshal([]byte(metadataJSON), &in.Metadata); err != nil {
		return Incident{}, fmt.Errorf("decoding metadata for incident %d: %w", in.ID, err)
	}

	for _, f := range []struct {
		raw string
		dst *time.Time
	}{{occurred, &in.OccurredAt}, {created, &in.CreatedAt}, {updated, &in.UpdatedAt}} {
		parsed, err := time.Parse(time.RFC3339, f.raw)
		if err != nil {
			return Incident{}, fmt.Errorf("parsing timestamp %q on incident %d: %w", f.raw, in.ID, err)
		}
		*f.dst = parsed.UTC()
	}

	if closed.Valid {
		parsed, err := time.Parse(time.RFC3339, closed.String)
		if err != nil {
			return Incident{}, fmt.Errorf("parsing closed_at on incident %d: %w", in.ID, err)
		}
		utc := parsed.UTC()
		in.ClosedAt = &utc
	}
	return in, nil
}

// ListIncidents returns one page of incidents, newest first, together with the
// total number matching the filter. The total is returned so the dashboard can
// paginate without a second round trip.
func ListIncidents(ctx context.Context, db *DB, f IncidentFilter) ([]Incident, int, error) {
	predicate, args := f.where()

	var total int
	if err := db.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM incidents`+predicate, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("counting incidents: %w", err)
	}

	limit := f.Limit
	if limit <= 0 {
		limit = defaultIncidentLimit
	}
	offset := max(f.Offset, 0)

	pageArgs := append(append([]any{}, args...), limit, offset)
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT `+incidentColumns+` FROM incidents`+predicate+
			f.orderBy()+` LIMIT ? OFFSET ?`, pageArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("listing incidents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Incident
	for rows.Next() {
		in, err := scanIncident(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scanning incident: %w", err)
		}
		out = append(out, in)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterating incidents: %w", err)
	}
	return out, total, nil
}

// GetIncident returns one incident, reporting false when absent.
func GetIncident(ctx context.Context, db *DB, id int64) (Incident, bool, error) {
	row := db.Reader().QueryRowContext(ctx,
		`SELECT `+incidentColumns+` FROM incidents WHERE id = ?`, id)

	in, err := scanIncident(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Incident{}, false, nil
	}
	if err != nil {
		return Incident{}, false, fmt.Errorf("getting incident %d: %w", id, err)
	}
	return in, true, nil
}

// CountByState returns the number of incidents in each observed state. States
// with no rows are absent rather than zero, so the caller decides which states
// are worth displaying.
func CountByState(ctx context.Context, db *DB) (map[string]int, error) {
	rows, err := db.Reader().QueryContext(ctx,
		`SELECT state, COUNT(*) FROM incidents GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("counting incidents by state: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[string]int)
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, fmt.Errorf("scanning state count: %w", err)
		}
		counts[state] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating state counts: %w", err)
	}
	return counts, nil
}

// MarkFingerprint attaches a computed fingerprint to an incident. It is
// separate from IngestIncident because fingerprinting needs the body, which is
// only available once the process loop picks the row up.
func MarkFingerprint(ctx context.Context, db *DB, incidentID int64, fingerprint string) error {
	_, err := db.Writer().ExecContext(ctx,
		`UPDATE incidents SET fingerprint = ? WHERE id = ?`, fingerprint, incidentID)
	if err != nil {
		return fmt.Errorf("marking fingerprint on incident %d: %w", incidentID, err)
	}
	return nil
}
