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
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/ingest"
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

// topicIncidents is the SSE topic carrying incident state changes.
const topicIncidents = "incidents"

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

	// IngestStats reports subscriber counters for /api/health. Nil means
	// ingestion is not running, which --no-ingest makes explicit.
	IngestStats func() (ingest.Stats, error)

	// IngestStaleAfter is how long without a successful pull before ingestion
	// is reported stale. Zero disables the check.
	IngestStaleAfter time.Duration

	Logger *slog.Logger
}

// Server owns the HTTP route table.
type Server struct {
	deps     Deps
	mux      *http.ServeMux
	static   http.Handler
	sessions *sessionStore
	log      *slog.Logger

	// registry is swapped atomically on SIGHUP. A failed reload leaves the
	// previous value in place (SPEC §4.1).
	registry atomic.Pointer[config.Registry]
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
	if d.Static != nil {
		s.static = d.Static
	} else {
		s.static = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
	}
	s.SetRegistry(d.Registry)
	s.routes()
	return s, nil
}

// SetRegistry atomically replaces the live project registry. Called on SIGHUP
// after a successful reload; a reload that fails validation never reaches here,
// so the previous registry stays in effect (SPEC §4.1).
func (s *Server) SetRegistry(reg config.Registry) {
	s.registry.Store(&reg)
}

// Registry returns the live project registry.
func (s *Server) Registry() config.Registry {
	if reg := s.registry.Load(); reg != nil {
		return *reg
	}
	return config.Registry{}
}

// Handler returns the server's root handler. Requests are split by path
// before either handler ever sees them, rather than registering a static
// catch-all pattern alongside the API patterns on one mux.
//
// Go's ServeMux only synthesizes a 405 for a method-mismatched request when
// no registered pattern matches the path at all. A catch-all pattern always
// matches by path, so on a single shared mux it would win outright for any
// method it accepts — e.g. a GET to /api/login (POST-only) would silently
// fall through to the catch-all as a 404 instead of the 405 a client expects.
// Keeping API routing and static serving on separate handlers, selected here
// by path prefix, sidesteps that precedence trap entirely.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPIPath(r.URL.Path) {
			s.mux.ServeHTTP(w, r)
			return
		}
		s.static.ServeHTTP(w, r)
	})
}

// isAPIPath reports whether p falls under the /api namespace, which the
// static handler must never serve HTML for (SPEC §9).
func isAPIPath(p string) bool {
	clean := path.Clean("/" + strings.TrimPrefix(p, "/"))
	return clean == "/api" || strings.HasPrefix(clean, "/api/")
}

// routes registers every API route. Method-qualified patterns give a 405
// rather than a 404 for a wrong method, which is a materially better error
// for a client (Go 1.22+ ServeMux).
//
// Only liveness and the session-state probe are unauthenticated (SPEC §8);
// everything else goes through requireSession.
func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/session", s.handleSession)
	s.mux.HandleFunc("POST /api/login", s.handleLogin)
	s.mux.HandleFunc("POST /api/logout", s.handleLogout)

	s.mux.Handle("GET /api/stream", s.requireSession(http.HandlerFunc(s.handleStream)))

	// M1 is read-only: every route is a GET. Mutating routes belong to later
	// milestones, which is what lets CSRF stay deferred as M0 planned.
	s.mux.Handle("GET /api/overview", s.requireSession(http.HandlerFunc(s.handleOverview)))
	s.mux.Handle("GET /api/incidents", s.requireSession(http.HandlerFunc(s.handleIncidents)))
	s.mux.Handle("GET /api/incidents/{id}", s.requireSession(http.HandlerFunc(s.handleIncident)))
	s.mux.Handle("GET /api/projects", s.requireSession(http.HandlerFunc(s.handleProjects)))
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
