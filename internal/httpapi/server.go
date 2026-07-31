// Package httpapi serves the sentinel's JSON API, its SSE stream, and the
// embedded dashboard.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/store"
)

// ErrDeps is returned when NewServer is given incomplete dependencies.
var ErrDeps = errors.New("invalid server dependencies")

// ReplayFunc returns persisted events newer than lastEventID for the given
// topics, so a reconnecting dashboard tab does not miss transitions. Topics is
// empty when the client subscribed to everything.
//
// M0 wires nil (no replay) because incident_events has no rows yet; M1 supplies
// a store-backed implementation. Declaring the seam now fixes the wire contract
// and keeps the SSE handler's shape stable.
type ReplayFunc func(ctx context.Context, lastEventID int64, topics []string) ([]bus.Event, error)

const defaultHeartbeatInterval = 15 * time.Second

// Deps are the server's collaborators. DB, Hub, Env.DashboardPasswordHash and
// Env.Location are required; the rest are optional.
type Deps struct {
	DB       *store.DB
	Hub      *bus.Hub
	Env      config.Env
	Registry config.Registry

	// Static serves the dashboard SPA. Nil disables the dashboard routes, which
	// is how the API-only tests run.
	Static http.Handler

	// Replay backfills an SSE reconnect. Nil disables replay.
	Replay ReplayFunc

	// HeartbeatInterval defaults to 15s. Tests set it low.
	HeartbeatInterval time.Duration

	// Now defaults to time.Now. Injectable so session expiry is testable.
	Now func() time.Time

	// Started is process start time, used for uptime.
	Started time.Time

	Logger *slog.Logger
}

// Server owns the HTTP route table.
type Server struct {
	deps     Deps
	mux      *http.ServeMux
	sessions *sessionStore
	log      *slog.Logger
}

// NewServer validates deps, applies defaults, and builds the route table.
func NewServer(d Deps) (*Server, error) {
	var problems []error

	if d.DB == nil {
		problems = append(problems, errors.New("DB is required"))
	}
	if d.Hub == nil {
		problems = append(problems, errors.New("Hub is required"))
	}
	if d.Env.DashboardPasswordHash == "" {
		problems = append(problems, errors.New("Env.DashboardPasswordHash is required"))
	}
	if d.Env.Location == nil {
		problems = append(problems, errors.New("Env.Location is required"))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w: %w", ErrDeps, errors.Join(problems...))
	}

	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Started.IsZero() {
		d.Started = d.Now()
	}
	if d.HeartbeatInterval <= 0 {
		d.HeartbeatInterval = defaultHeartbeatInterval
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}

	s := &Server{
		deps:     d,
		mux:      http.NewServeMux(),
		sessions: newSessionStore(d.Now),
		log:      d.Logger,
	}
	s.routes()
	return s, nil
}

// Handler returns the server's root handler.
func (s *Server) Handler() http.Handler { return s.mux }

// routes registers every route. Method-qualified patterns give a 405 rather
// than a 404 for a wrong method, which is a materially better error for a
// client (Go 1.22+ ServeMux).
func (s *Server) routes() {
	// Unauthenticated: liveness only (SPEC §8).
	s.mux.HandleFunc("GET /api/health", s.handleHealth)

	s.mux.HandleFunc("GET /api/stream", s.handleStream)
}

// writeJSON writes v as a JSON response. Encoding failures are logged rather
// than surfaced, because the status line has already been written by then.
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Error("encoding json response", "error", err)
	}
}

// errorResponse is the body of every non-2xx JSON response.
type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, errorResponse{Error: message})
}
