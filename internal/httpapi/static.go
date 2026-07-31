package httpapi

import (
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"
)

const notBuiltMessage = `The dashboard has not been built.

Run:
    make web

Then restart the sentinel, or run with -dev to proxy to the Vite dev server.
`

// NewStaticHandler serves the embedded SPA. A nil dist yields a handler that
// 404s everything, which is how API-only configurations run.
//
// Two behaviours matter more than they look:
//
//   - index.html is served with no-cache while hashed assets are immutable. A
//     cached shell would keep pointing at asset hashes that no longer exist
//     after a deploy, which presents as a blank page.
//   - History fallback applies only to paths that are neither /api/* nor
//     asset-like. Returning index.html for an unknown API path would make a
//     client parse HTML as JSON; returning it for a missing asset would mask a
//     broken build.
func NewStaticHandler(dist fs.FS) http.Handler {
	if dist == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
	}

	fileServer := http.FileServerFS(dist)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := fs.Stat(dist, "index.html"); err != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, notBuiltMessage)
			return
		}

		upath := path.Clean("/" + strings.TrimPrefix(r.URL.Path, "/"))

		// An unmatched API route must never receive HTML.
		if upath == "/api" || strings.HasPrefix(upath, "/api/") {
			http.NotFound(w, r)
			return
		}

		name := strings.TrimPrefix(upath, "/")
		if name == "" {
			serveIndex(w, r, dist)
			return
		}

		info, err := fs.Stat(dist, name)
		switch {
		case err == nil && !info.IsDir():
			setAssetCacheHeaders(w, name)
			fileServer.ServeHTTP(w, r)
		case looksLikeAsset(name):
			// A missing hashed asset is a broken build, not a client route.
			http.NotFound(w, r)
		default:
			serveIndex(w, r, dist)
		}
	})
}

func serveIndex(w http.ResponseWriter, r *http.Request, dist fs.FS) {
	body, err := fs.ReadFile(dist, "index.html")
	if err != nil {
		http.Error(w, notBuiltMessage, http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, must-revalidate")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// setAssetCacheHeaders marks Vite's content-hashed output immutable.
func setAssetCacheHeaders(w http.ResponseWriter, name string) {
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		return
	}
	w.Header().Set("Cache-Control", "no-cache, must-revalidate")
}

// looksLikeAsset reports whether a path should 404 rather than fall back to the
// SPA shell. Anything with a file extension is treated as an asset request.
func looksLikeAsset(name string) bool {
	return path.Ext(name) != ""
}

// NewDevProxyHandler reverse-proxies to the Vite dev server so `make dev` gets
// hot reload while the Go API stays authoritative.
func NewDevProxyHandler(target string) (http.Handler, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("parsing dev server URL %q: %w", target, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("dev server URL %q needs a scheme and host", target)
	}
	return httputil.NewSingleHostReverseProxy(parsed), nil
}
