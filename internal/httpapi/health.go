package httpapi

import (
	"net/http"

	"github.com/b1codes/triage-sentinel/internal/store"
	"github.com/b1codes/triage-sentinel/internal/sysinfo"
	"github.com/b1codes/triage-sentinel/internal/version"
)

// HealthResponse is the /api/health body. It is the JSON contract the dashboard
// consumes, and the values SPEC §12 requires for operational visibility.
type HealthResponse struct {
	Status        string   `json:"status"`
	Version       string   `json:"version"`
	UptimeSeconds int64    `json:"uptime_seconds"`
	Goroutines    int      `json:"goroutines"`
	RSSBytes      int64    `json:"rss_bytes"`
	FreeRAMBytes  int64    `json:"free_ram_bytes"`
	FreeDiskBytes int64    `json:"free_disk_bytes"`
	DBSizeBytes   int64    `json:"db_size_bytes"`
	SchemaVersion int      `json:"schema_version"`
	SSEClients    int      `json:"sse_clients"`
	Projects      int      `json:"projects"`
	Problems      []string `json:"problems,omitempty"`
}

// handleHealth reports process and host state. It is the only unauthenticated
// route (SPEC §8).
//
// It returns 503 with status "degraded" when a check fails rather than 200 with
// a quiet zero, because the whole point of this endpoint is to notice a process
// that is running but not working.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	snap := sysinfo.Sample()

	resp := HealthResponse{
		Status:        "ok",
		Version:       version.Get(),
		UptimeSeconds: int64(s.deps.Now().Sub(s.deps.Started).Seconds()),
		Goroutines:    snap.Goroutines,
		RSSBytes:      snap.RSSBytes,
		FreeRAMBytes:  snap.FreeRAMBytes,
		SSEClients:    s.deps.Hub.ClientCount(),
		Projects:      len(s.Registry().Projects),
	}

	schemaVersion, err := store.SchemaVersion(r.Context(), s.deps.DB)
	if err != nil {
		resp.Problems = append(resp.Problems, "schema version unavailable: "+err.Error())
	} else {
		resp.SchemaVersion = schemaVersion
		if schemaVersion == 0 {
			resp.Problems = append(resp.Problems, "database has no applied migrations")
		}
	}

	if size, err := s.deps.DB.SizeBytes(); err != nil {
		resp.Problems = append(resp.Problems, "database size unavailable: "+err.Error())
	} else {
		resp.DBSizeBytes = size
	}

	if free, err := sysinfo.FreeDiskBytes(s.deps.Env.DataDir); err != nil {
		resp.Problems = append(resp.Problems, "free disk unavailable: "+err.Error())
	} else {
		resp.FreeDiskBytes = free
	}

	status := http.StatusOK
	if len(resp.Problems) > 0 {
		resp.Status = "degraded"
		status = http.StatusServiceUnavailable
	}

	s.writeJSON(w, status, resp)
}
