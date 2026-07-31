package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func fakeDist() fstest.MapFS {
	return fstest.MapFS{
		"index.html":            &fstest.MapFile{Data: []byte(`<!doctype html><div id="root"></div>`)},
		"assets/app-abc123.js":  &fstest.MapFile{Data: []byte(`console.log("app")`)},
		"assets/app-abc123.css": &fstest.MapFile{Data: []byte(`body{}`)},
	}
}

func TestStaticServesIndexAtRoot(t *testing.T) {
	h := NewStaticHandler(fakeDist())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), `id="root"`) {
		t.Errorf("body does not look like index.html: %s", rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); !strings.Contains(got, "no-cache") {
		t.Errorf("index.html Cache-Control = %q, want no-cache; a cached shell pins stale asset hashes", got)
	}
}

func TestStaticServesHashedAssetsImmutably(t *testing.T) {
	h := NewStaticHandler(fakeDist())

	req := httptest.NewRequest(http.MethodGet, "/assets/app-abc123.js", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != `console.log("app")` {
		t.Errorf("body = %q, want the asset contents", rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); !strings.Contains(got, "immutable") {
		t.Errorf("asset Cache-Control = %q, want immutable", got)
	}
}

func TestStaticFallsBackToIndexForClientRoutes(t *testing.T) {
	h := NewStaticHandler(fakeDist())

	for _, path := range []string{"/incidents", "/incidents/42", "/spend"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}
			if !strings.Contains(rec.Body.String(), `id="root"`) {
				t.Error("client-side route did not fall back to index.html")
			}
		})
	}
}

func TestStaticDoesNotFallBackForAPIPaths(t *testing.T) {
	h := NewStaticHandler(fakeDist())

	req := httptest.NewRequest(http.MethodGet, "/api/does-not-exist", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if strings.Contains(rec.Body.String(), `id="root"`) {
		t.Error("an unknown API path returned index.html; a client would parse HTML as JSON")
	}
}

func TestStaticDoesNotFallBackForMissingAssets(t *testing.T) {
	h := NewStaticHandler(fakeDist())

	req := httptest.NewRequest(http.MethodGet, "/assets/missing-file.js", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d; a missing asset must 404 rather than return HTML", rec.Code, http.StatusNotFound)
	}
}

func TestStaticWithoutIndexReportsHowToFix(t *testing.T) {
	h := NewStaticHandler(fstest.MapFS{".gitkeep": &fstest.MapFile{}})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "make web") {
		t.Errorf("body %q does not tell the operator to run make web", rec.Body.String())
	}
}

func TestStaticNilFSReturns404(t *testing.T) {
	h := NewStaticHandler(nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestServerServesDashboardWhenStaticSet(t *testing.T) {
	srv := newTestServer(t, func(d *Deps) {
		d.Static = NewStaticHandler(fakeDist())
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), `id="root"`) {
		t.Error("server did not serve the dashboard shell at /")
	}
}

func TestServerWithoutStaticReturns404AtRoot(t *testing.T) {
	srv := newTestServer(t, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestNewDevProxyHandlerRejectsBadTarget(t *testing.T) {
	if _, err := NewDevProxyHandler("://not a url"); err == nil {
		t.Error("NewDevProxyHandler() error = nil, want error")
	}
}

func TestNewDevProxyHandlerForwards(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/incidents" {
			t.Errorf("upstream path = %q, want /incidents", r.URL.Path)
		}
		_, _ = w.Write([]byte("from vite"))
	}))
	defer upstream.Close()

	h, err := NewDevProxyHandler(upstream.URL)
	if err != nil {
		t.Fatalf("NewDevProxyHandler() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/incidents", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Body.String() != "from vite" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "from vite")
	}
}
