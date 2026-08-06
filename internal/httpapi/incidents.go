package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/b1codes/triage-sentinel/internal/store"
)

// maxPageSize bounds a client-supplied limit so a single request cannot pull
// the whole table into memory.
const maxPageSize = 200

// IncidentSummary is one row of the incident feed.
type IncidentSummary struct {
	ID              int64     `json:"id"`
	ProjectSlug     string    `json:"project_slug"`
	Source          string    `json:"source"`
	Kind            string    `json:"kind"`
	Title           string    `json:"title"`
	State           string    `json:"state"`
	StateReason     string    `json:"state_reason,omitempty"`
	Fingerprint     string    `json:"fingerprint,omitempty"`
	OccurrenceCount int       `json:"occurrence_count"`
	OccurredAt      time.Time `json:"occurred_at"`
	CreatedAt       time.Time `json:"created_at"`
}

// TimelineEntry is one audit row in the detail view.
type TimelineEntry struct {
	ID        int64     `json:"id"`
	TS        time.Time `json:"ts"`
	Kind      string    `json:"kind"`
	Actor     string    `json:"actor"`
	FromState string    `json:"from_state,omitempty"`
	ToState   string    `json:"to_state,omitempty"`
	Payload   any       `json:"payload,omitempty"`
}

// IncidentDetail is one incident with its timeline.
type IncidentDetail struct {
	IncidentSummary
	Body      string            `json:"body,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Strategy  string            `json:"fingerprint_strategy,omitempty"`
	Frames    []string          `json:"fingerprint_frames,omitempty"`
	Events    []TimelineEntry   `json:"events"`
	UpdatedAt time.Time         `json:"updated_at"`
	ClosedAt  *time.Time        `json:"closed_at,omitempty"`
}

func summarize(in store.Incident) IncidentSummary {
	return IncidentSummary{
		ID: in.ID, ProjectSlug: in.ProjectSlug, Source: in.Source, Kind: in.Kind,
		Title: in.Title, State: in.State, StateReason: in.StateReason,
		Fingerprint: in.Fingerprint, OccurrenceCount: in.OccurrenceCount,
		OccurredAt: in.OccurredAt, CreatedAt: in.CreatedAt,
	}
}

// handleIncidents serves GET /api/incidents.
func (s *Server) handleIncidents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := store.IncidentFilter{
		States:   splitCSV(q.Get("state")),
		Projects: splitCSV(q.Get("project")),
		Sources:  splitCSV(q.Get("source")),
		Limit:    intParam(q.Get("limit"), 50, maxPageSize),
		Offset:   intParam(q.Get("offset"), 0, 1<<20),
	}

	incidents, total, err := store.ListIncidents(r.Context(), s.deps.DB, filter)
	if err != nil {
		s.log.Error("listing incidents", "error", err)
		s.writeError(w, http.StatusInternalServerError, "could not list incidents")
		return
	}

	summaries := make([]IncidentSummary, 0, len(incidents))
	for _, in := range incidents {
		summaries = append(summaries, summarize(in))
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"incidents": summaries,
		"total":     total,
		"limit":     filter.Limit,
		"offset":    filter.Offset,
	})
}

// handleIncident serves GET /api/incidents/{id}.
func (s *Server) handleIncident(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "incident id must be an integer")
		return
	}

	incident, found, err := store.GetIncident(r.Context(), s.deps.DB, id)
	if err != nil {
		s.log.Error("getting incident", "error", err, "incident_id", id)
		s.writeError(w, http.StatusInternalServerError, "could not load incident")
		return
	}
	if !found {
		s.writeError(w, http.StatusNotFound, "incident not found")
		return
	}

	events, err := store.EventsForIncident(r.Context(), s.deps.DB, id)
	if err != nil {
		s.log.Error("loading incident timeline", "error", err, "incident_id", id)
		s.writeError(w, http.StatusInternalServerError, "could not load timeline")
		return
	}

	timeline := make([]TimelineEntry, 0, len(events))
	for _, ev := range events {
		timeline = append(timeline, TimelineEntry{
			ID: ev.ID, TS: ev.TS, Kind: ev.Kind, Actor: ev.Actor,
			FromState: ev.FromState, ToState: ev.ToState,
			Payload: rawOrNil(ev.Payload),
		})
	}

	detail := IncidentDetail{
		IncidentSummary: summarize(incident),
		Body:            incident.Body,
		Metadata:        incident.Metadata,
		Events:          timeline,
		UpdatedAt:       incident.UpdatedAt,
		ClosedAt:        incident.ClosedAt,
	}

	// The grouping evidence is what makes M1 a useful observation period.
	if incident.Fingerprint != "" {
		if rec, ok, err := store.GetFingerprint(r.Context(), s.deps.DB, incident.Fingerprint); err == nil && ok {
			detail.Strategy = rec.Strategy
			detail.Frames = rec.Frames
		}
	}

	s.writeJSON(w, http.StatusOK, detail)
}

func rawOrNil(payload []byte) any {
	if len(payload) == 0 || string(payload) == "{}" {
		return nil
	}
	return jsonRaw(payload)
}

// jsonRaw marshals pre-encoded JSON without re-encoding it.
type jsonRaw []byte

// MarshalJSON returns the bytes unchanged.
func (r jsonRaw) MarshalJSON() ([]byte, error) { return r, nil }

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func intParam(raw string, fallback, limit int) int {
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return fallback
	}
	return min(n, limit)
}
