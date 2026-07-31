package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/b1codes/triage-sentinel/internal/config"
)

func projectCount(t *testing.T, srv *Server) int {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	var got HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	return got.Projects
}

func TestSetRegistrySwapsLiveConfiguration(t *testing.T) {
	srv := newTestServer(t, func(d *Deps) {
		d.Registry = config.Registry{Projects: []config.Project{{Slug: "one-project"}}}
	})

	if got := projectCount(t, srv); got != 1 {
		t.Fatalf("Projects = %d, want 1", got)
	}

	srv.SetRegistry(config.Registry{Projects: []config.Project{
		{Slug: "one-project"}, {Slug: "two-project"},
	}})

	if got := projectCount(t, srv); got != 2 {
		t.Errorf("Projects after SetRegistry = %d, want 2", got)
	}
}

func TestSetRegistryIsRaceFree(t *testing.T) {
	srv := newTestServer(t, nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				srv.SetRegistry(config.Registry{
					Projects: make([]config.Project, n+1),
				})
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
				rec := httptest.NewRecorder()
				srv.Handler().ServeHTTP(rec, req)
			}
		}()
	}
	wg.Wait()
}
