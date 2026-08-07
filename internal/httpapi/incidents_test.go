package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/store"
)

func TestIncidentRoutes(t *testing.T) {
	db, ctx, now, id := replayFixture(t)
	if _, err := storeTransitionForTest(t, db, ctx, id, now); err != nil {
		t.Fatalf("seeding transition: %v", err)
	}

	srv := newTestServerWithDB(t, db)

	t.Run("list requires a session", func(t *testing.T) {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/incidents", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	cookie := loginForTest(t, srv)

	t.Run("list returns the page and total", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
		var body struct {
			Incidents []IncidentSummary `json:"incidents"`
			Total     int               `json:"total"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if body.Total != 1 || len(body.Incidents) != 1 {
			t.Fatalf("total/len = %d/%d, want 1/1", body.Total, len(body.Incidents))
		}
		if body.Incidents[0].Title != "boom" {
			t.Errorf("Title = %q, want %q", body.Incidents[0].Title, "boom")
		}
	})

	t.Run("state filter is applied", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents?state=filtered")
		var body struct {
			Total int `json:"total"`
		}
		_ = json.NewDecoder(rec.Body).Decode(&body)
		if body.Total != 0 {
			t.Errorf("total = %d, want 0", body.Total)
		}
	})

	t.Run("detail includes the timeline", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents/"+strconv.FormatInt(id, 10))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
		var detail IncidentDetail
		if err := json.NewDecoder(rec.Body).Decode(&detail); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if len(detail.Events) == 0 {
			t.Error("Events is empty; the detail view renders the timeline from it")
		}
	})

	t.Run("unknown id is 404 not 500", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents/999999")
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rec.Code)
		}
	})

	t.Run("non-numeric id is 400", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/incidents/abc")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("overview reports state counters", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/overview")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var body OverviewResponse
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if body.IncidentsByState["triaging"] != 1 {
			t.Errorf("IncidentsByState = %v, want triaging 1", body.IncidentsByState)
		}
	})

	t.Run("projects lists the registry with counts", func(t *testing.T) {
		rec := authedGet(t, srv, cookie, "/api/projects")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var body struct {
			Projects []ProjectSummary `json:"projects"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if len(body.Projects) != 1 || body.Projects[0].Slug != "api" {
			t.Errorf("Projects = %v, want the one registered project", body.Projects)
		}
	})

	t.Run("mutating methods are rejected", func(t *testing.T) {
		// M1 is read-only; every mutating route belongs to a later milestone.
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/incidents", nil)
		req.AddCookie(cookie)
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
	})
}

// newTestServerWithDB reuses M0's server helper but swaps in a pre-seeded
// database, since the route tests need incidents that already exist.
func newTestServerWithDB(t *testing.T, db *store.DB) *Server {
	t.Helper()
	return newTestServer(t, func(d *Deps) {
		d.DB = db
		d.Env.DashboardPasswordHash = hashFor(t, testPassword)
		d.Replay = NewStoreReplay(db)
		d.HeartbeatInterval = time.Hour
		d.Registry = config.Registry{
			Projects: []config.Project{
				{Slug: "api", Repo: "github.com/example/api", DefaultBranch: "main"},
			},
		}
	})
}

// loginForTest signs in and returns the session cookie.
func loginForTest(t *testing.T, srv *Server) *http.Cookie {
	t.Helper()
	resp := login(t, srv, testPassword)
	cookie := sessionCookie(t, resp)
	if cookie == nil {
		t.Fatal("login returned no session cookie")
	}
	return cookie
}

// authedGet issues an authenticated GET and returns the recorder.
func authedGet(t *testing.T, srv *Server, cookie *http.Cookie, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// storeTransitionForTest moves the seeded incident to triaging so the list and
// detail views have a timeline to render.
func storeTransitionForTest(t *testing.T, db *store.DB, ctx context.Context, id int64, now time.Time) (int64, error) {
	t.Helper()
	return store.Transition(ctx, db, store.TransitionParams{
		IncidentID: id, From: "received", To: "triaging", Actor: "tier0",
	}, now)
}
