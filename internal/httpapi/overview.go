package httpapi

import (
	"net/http"
	"time"

	"github.com/b1codes/triage-sentinel/internal/store"
)

// OverviewResponse is the dashboard's landing payload. Budget fields arrive
// in M2; M1 reports what it actually knows.
type OverviewResponse struct {
	IncidentsByState map[string]int `json:"incidents_by_state"`
	Projects         int            `json:"projects"`
	LastIngestAt     *time.Time     `json:"last_ingest_at,omitempty"`
	IngestStale      bool           `json:"ingest_stale"`

	// AwaitingTriage is the count resting in triaging. M1 has no Tier 1, so
	// this is displayed plainly rather than as a queue being worked.
	AwaitingTriage int `json:"awaiting_triage"`
}

// ProjectSummary is one row of the projects view.
type ProjectSummary struct {
	Slug             string `json:"slug"`
	Repo             string `json:"repo"`
	DefaultBranch    string `json:"default_branch"`
	Active           bool   `json:"active"`
	Quarantined      bool   `json:"quarantined"`
	QuarantineReason string `json:"quarantine_reason,omitempty"`
	Incidents        int    `json:"incidents"`
}

// handleOverview serves GET /api/overview.
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	counts, err := store.CountByState(r.Context(), s.deps.DB)
	if err != nil {
		s.log.Error("counting incidents", "error", err)
		s.writeError(w, http.StatusInternalServerError, "could not load overview")
		return
	}

	resp := OverviewResponse{
		IncidentsByState: counts,
		Projects:         len(s.Registry().Projects),
		AwaitingTriage:   counts["triaging"],
	}

	if at, ok, err := store.LastIngestAt(r.Context(), s.deps.DB); err == nil && ok {
		resp.LastIngestAt = &at
		resp.IngestStale = s.ingestIsStale(at)
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// handleProjects serves GET /api/projects.
func (s *Server) handleProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := store.ListProjects(r.Context(), s.deps.DB)
	if err != nil {
		s.log.Error("listing projects", "error", err)
		s.writeError(w, http.StatusInternalServerError, "could not list projects")
		return
	}

	summaries := make([]ProjectSummary, 0, len(projects))
	for _, p := range projects {
		_, total, err := store.ListIncidents(r.Context(), s.deps.DB, store.IncidentFilter{
			Projects: []string{p.Slug}, Limit: 1,
		})
		if err != nil {
			s.log.Error("counting project incidents", "error", err, "slug", p.Slug)
		}
		summaries = append(summaries, ProjectSummary{
			Slug: p.Slug, Repo: p.Repo, DefaultBranch: p.DefaultBranch,
			Active: p.Active, Quarantined: p.Quarantined,
			QuarantineReason: p.QuarantineReason, Incidents: total,
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]any{"projects": summaries})
}

// ingestIsStale reports whether the last successful ingest is older than the
// configured threshold. A stalled subscriber is the most likely silent failure
// in the system — the process looks healthy while seeing nothing (SPEC §12).
func (s *Server) ingestIsStale(last time.Time) bool {
	threshold := s.deps.IngestStaleAfter
	if threshold <= 0 {
		return false
	}
	return s.deps.Now().Sub(last) > threshold
}
