package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const testPassword = "hunter2-correct-horse"

func hashFor(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt.GenerateFromPassword() error = %v", err)
	}
	return string(hash)
}

// newAuthServer returns a server whose password hash really matches
// testPassword, plus a mutable clock for expiry tests.
func newAuthServer(t *testing.T) (*Server, *time.Time) {
	t.Helper()

	now := time.Now()
	srv := newTestServer(t, func(d *Deps) {
		d.Env.DashboardPasswordHash = hashFor(t, testPassword)
		d.Now = func() time.Time { return now }
		d.HeartbeatInterval = time.Hour
	})
	return srv, &now
}

func login(t *testing.T, srv *Server, password string) *http.Response {
	t.Helper()

	body := strings.NewReader(`{"password":` + quote(password) + `}`)
	req := httptest.NewRequest(http.MethodPost, "/api/login", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec.Result()
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func sessionCookie(t *testing.T, resp *http.Response) *http.Cookie {
	t.Helper()
	for _, c := range resp.Cookies() {
		if c.Name == SessionCookieName {
			return c
		}
	}
	t.Fatalf("response has no %s cookie", SessionCookieName)
	return nil
}

func TestLoginSucceedsWithCorrectPassword(t *testing.T) {
	srv, _ := newAuthServer(t)

	resp := login(t, srv, testPassword)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	c := sessionCookie(t, resp)
	if c.Value == "" {
		t.Error("session cookie value is empty")
	}
	if !c.HttpOnly {
		t.Error("session cookie is not HttpOnly; JavaScript could read it")
	}
	if c.SameSite != http.SameSiteStrictMode {
		t.Errorf("SameSite = %v, want Strict (it is the CSRF mitigation in M0)", c.SameSite)
	}
	if c.Path != "/" {
		t.Errorf("Path = %q, want /", c.Path)
	}
}

func TestLoginRejectsWrongPassword(t *testing.T) {
	srv, _ := newAuthServer(t)

	resp := login(t, srv, "wrong-password")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
	for _, c := range resp.Cookies() {
		if c.Name == SessionCookieName && c.Value != "" {
			t.Error("a session cookie was set for a failed login")
		}
	}
}

func TestLoginRejectsBadRequests(t *testing.T) {
	srv, _ := newAuthServer(t)

	tests := []struct {
		name     string
		method   string
		body     string
		wantCode int
	}{
		{name: "malformed json", method: http.MethodPost, body: `{"password":`, wantCode: http.StatusBadRequest},
		{name: "empty password", method: http.MethodPost, body: `{"password":""}`, wantCode: http.StatusUnauthorized},
		{name: "missing field", method: http.MethodPost, body: `{}`, wantCode: http.StatusUnauthorized},
		{name: "wrong method", method: http.MethodGet, body: ``, wantCode: http.StatusMethodNotAllowed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/api/login", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Errorf("status = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
		})
	}
}

func TestLoginRejectsOversizedBody(t *testing.T) {
	srv, _ := newAuthServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/login",
		strings.NewReader(`{"password":"`+strings.Repeat("x", 1<<20)+`"}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusNoContent {
		t.Error("an oversized login body was accepted; the request body must be capped")
	}
}

func TestRequireSessionBlocksAnonymous(t *testing.T) {
	srv, _ := newAuthServer(t)

	protected := srv.requireSession(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		name   string
		cookie *http.Cookie
	}{
		{name: "no cookie", cookie: nil},
		{name: "empty value", cookie: &http.Cookie{Name: SessionCookieName, Value: ""}},
		{name: "unknown value", cookie: &http.Cookie{Name: SessionCookieName, Value: "not-a-real-session"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
			if tc.cookie != nil {
				req.AddCookie(tc.cookie)
			}
			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
			}
		})
	}
}

func TestRequireSessionAllowsValidSession(t *testing.T) {
	srv, _ := newAuthServer(t)

	resp := login(t, srv, testPassword)
	c := sessionCookie(t, resp)

	protected := srv.requireSession(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	req.AddCookie(c)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestSessionExpires(t *testing.T) {
	srv, now := newAuthServer(t)

	resp := login(t, srv, testPassword)
	c := sessionCookie(t, resp)

	protected := srv.requireSession(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	check := func() int {
		req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
		req.AddCookie(c)
		rec := httptest.NewRecorder()
		protected.ServeHTTP(rec, req)
		return rec.Code
	}

	if got := check(); got != http.StatusOK {
		t.Fatalf("status before expiry = %d, want %d", got, http.StatusOK)
	}

	*now = now.Add(SessionTTL + time.Minute)

	if got := check(); got != http.StatusUnauthorized {
		t.Errorf("status after expiry = %d, want %d", got, http.StatusUnauthorized)
	}
}

func TestLogoutRevokesSession(t *testing.T) {
	srv, _ := newAuthServer(t)

	resp := login(t, srv, testPassword)
	c := sessionCookie(t, resp)

	logoutReq := httptest.NewRequest(http.MethodPost, "/api/logout", nil)
	logoutReq.AddCookie(c)
	logoutRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(logoutRec, logoutReq)

	if logoutRec.Code != http.StatusNoContent {
		t.Fatalf("logout status = %d, want %d", logoutRec.Code, http.StatusNoContent)
	}

	protected := srv.requireSession(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/whatever", nil)
	req.AddCookie(c)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status after logout = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestSessionsAreDistinctPerLogin(t *testing.T) {
	srv, _ := newAuthServer(t)

	first := sessionCookie(t, login(t, srv, testPassword))
	second := sessionCookie(t, login(t, srv, testPassword))

	if first.Value == second.Value {
		t.Error("two logins produced the same session token; tokens must be random per session")
	}
}

func TestSessionEndpointReportsState(t *testing.T) {
	srv, _ := newAuthServer(t)

	anonReq := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	anonRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(anonRec, anonReq)

	if anonRec.Code != http.StatusOK {
		t.Fatalf("anonymous /api/session status = %d, want %d", anonRec.Code, http.StatusOK)
	}

	var anon SessionResponse
	if err := json.Unmarshal(anonRec.Body.Bytes(), &anon); err != nil {
		t.Fatalf("decoding anonymous body: %v", err)
	}
	if anon.Authenticated {
		t.Error("Authenticated = true for an anonymous request")
	}

	c := sessionCookie(t, login(t, srv, testPassword))

	authReq := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	authReq.AddCookie(c)
	authRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(authRec, authReq)

	var authed SessionResponse
	if err := json.Unmarshal(authRec.Body.Bytes(), &authed); err != nil {
		t.Fatalf("decoding authenticated body: %v", err)
	}
	if !authed.Authenticated {
		t.Error("Authenticated = false for a logged-in request")
	}
}

func TestHealthStaysUnauthenticated(t *testing.T) {
	srv, _ := newAuthServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("health status = %d, want %d; liveness must not require a session (SPEC §8)", rec.Code, http.StatusOK)
	}
}

func TestStreamRequiresSession(t *testing.T) {
	srv, _ := newAuthServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stream", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("stream status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
