package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/store"
)

const testBcryptHash = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

// newTestServer builds a server backed by a real migrated database and hub.
func newTestServer(t *testing.T, mutate func(*Deps)) *Server {
	t.Helper()

	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "sentinel.db"))
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := store.Migrate(context.Background(), db); err != nil {
		t.Fatalf("store.Migrate() error = %v", err)
	}

	hub := bus.NewHub(8)
	t.Cleanup(hub.Close)

	deps := Deps{
		DB:  db,
		Hub: hub,
		Env: config.Env{
			DashboardPasswordHash: testBcryptHash,
			DataDir:               dir,
			Location:              time.UTC,
		},
		Started: time.Now().Add(-90 * time.Second),
	}
	if mutate != nil {
		mutate(&deps)
	}

	srv, err := NewServer(deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return srv
}

func TestNewServerRejectsMissingDeps(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "sentinel.db"))
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	hub := bus.NewHub(4)
	defer hub.Close()

	validEnv := config.Env{DashboardPasswordHash: testBcryptHash, DataDir: dir, Location: time.UTC}

	tests := []struct {
		name string
		deps Deps
	}{
		{name: "no database", deps: Deps{Hub: hub, Env: validEnv}},
		{name: "no hub", deps: Deps{DB: db, Env: validEnv}},
		{
			name: "no password hash",
			deps: Deps{DB: db, Hub: hub, Env: config.Env{DataDir: dir, Location: time.UTC}},
		},
		{
			name: "no location",
			deps: Deps{DB: db, Hub: hub, Env: config.Env{DashboardPasswordHash: testBcryptHash, DataDir: dir}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewServer(tc.deps)
			if err == nil {
				t.Fatal("NewServer() error = nil, want error")
			}
			if !errors.Is(err, ErrDeps) {
				t.Errorf("errors.Is(err, ErrDeps) = false, want true (err = %v)", err)
			}
		})
	}
}

func TestHealthReturnsOK(t *testing.T) {
	srv := newTestServer(t, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q, want application/json; charset=utf-8", got)
	}

	var got HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding body: %v (body: %s)", err, rec.Body.String())
	}

	if got.Status != "ok" {
		t.Errorf("Status = %q, want %q", got.Status, "ok")
	}
	if got.Version == "" {
		t.Error("Version is empty")
	}
	if got.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", got.SchemaVersion)
	}
	if got.UptimeSeconds < 90 {
		t.Errorf("UptimeSeconds = %d, want >= 90", got.UptimeSeconds)
	}
	if got.Goroutines < 1 {
		t.Errorf("Goroutines = %d, want >= 1", got.Goroutines)
	}
	if got.DBSizeBytes <= 0 {
		t.Errorf("DBSizeBytes = %d, want > 0", got.DBSizeBytes)
	}
	if got.FreeDiskBytes <= 0 {
		t.Errorf("FreeDiskBytes = %d, want > 0", got.FreeDiskBytes)
	}
	if got.SSEClients != 0 {
		t.Errorf("SSEClients = %d, want 0", got.SSEClients)
	}
}

func TestHealthCountsSSEClients(t *testing.T) {
	hub := bus.NewHub(8)
	srv := newTestServer(t, func(d *Deps) { d.Hub = hub })
	t.Cleanup(hub.Close)

	hub.Subscribe("incidents")
	hub.Subscribe("runs")

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	var got HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if got.SSEClients != 2 {
		t.Errorf("SSEClients = %d, want 2", got.SSEClients)
	}
}

func TestHealthReportsProjectCount(t *testing.T) {
	srv := newTestServer(t, func(d *Deps) {
		d.Registry = config.Registry{Projects: []config.Project{
			{Slug: "a-project"}, {Slug: "b-project"}, {Slug: "c-project"},
		}}
	})

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	var got HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if got.Projects != 3 {
		t.Errorf("Projects = %d, want 3", got.Projects)
	}
}

func TestHealthRejectsNonGET(t *testing.T) {
	srv := newTestServer(t, nil)

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/api/health", nil)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
			}
		})
	}
}

func TestHealthReportsDegradedWhenSchemaMissing(t *testing.T) {
	// A database that was never migrated is a real failure mode: the process
	// would appear healthy while every query fails.
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "unmigrated.db"))
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	hub := bus.NewHub(4)
	defer hub.Close()

	srv, err := NewServer(Deps{
		DB:  db,
		Hub: hub,
		Env: config.Env{DashboardPasswordHash: testBcryptHash, DataDir: dir, Location: time.UTC},
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var got HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if got.Status != "degraded" {
		t.Errorf("Status = %q, want %q", got.Status, "degraded")
	}
	if got.SchemaVersion != 0 {
		t.Errorf("SchemaVersion = %d, want 0", got.SchemaVersion)
	}
}
