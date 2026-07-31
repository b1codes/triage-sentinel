package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	// SessionCookieName is the dashboard session cookie.
	SessionCookieName = "sentinel_session"

	// SessionTTL is how long a session stays valid without re-login.
	SessionTTL = 12 * time.Hour

	// maxLoginBodyBytes caps the login request body. Without it an
	// unauthenticated caller could stream an arbitrarily large body into
	// memory.
	maxLoginBodyBytes = 4 << 10

	sessionTokenBytes = 32
)

// sessionStore holds dashboard sessions in memory. Sessions therefore end when
// the process restarts, which is acceptable for a single-operator loopback
// service and avoids persisting a credential-equivalent token to SQLite
// (SPEC §14).
type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]time.Time // token -> expiry
	now      func() time.Time
}

func newSessionStore(now func() time.Time) *sessionStore {
	if now == nil {
		now = time.Now
	}
	return &sessionStore{
		sessions: make(map[string]time.Time),
		now:      now,
	}
}

// issue creates a new session token.
func (s *sessionStore) issue() (string, error) {
	buf := make([]byte, sessionTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating session token: %w", err)
	}
	token := hex.EncodeToString(buf)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.collectExpiredLocked()
	s.sessions[token] = s.now().Add(SessionTTL)
	return token, nil
}

// valid reports whether a token names a live session, removing it if expired.
func (s *sessionStore) valid(token string) bool {
	if token == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	expiry, ok := s.sessions[token]
	if !ok {
		return false
	}
	if s.now().After(expiry) {
		delete(s.sessions, token)
		return false
	}
	return true
}

func (s *sessionStore) revoke(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, token)
}

// collectExpiredLocked drops expired sessions. The caller must hold s.mu.
func (s *sessionStore) collectExpiredLocked() {
	now := s.now()
	for token, expiry := range s.sessions {
		if now.After(expiry) {
			delete(s.sessions, token)
		}
	}
}

// loginRequest is the POST /api/login body.
type loginRequest struct {
	Password string `json:"password"`
}

// SessionResponse is the GET /api/session body, so the SPA knows whether to
// render a login form.
type SessionResponse struct {
	Authenticated bool `json:"authenticated"`
}

// handleLogin verifies the password and issues a session cookie.
//
// bcrypt is compared even for an empty password so a missing field and a wrong
// password take the same path, and bcrypt's own cost is the brute-force
// mitigation — no artificial delay is added.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxLoginBodyBytes)).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "request body must be JSON with a password field")
		return
	}

	err := bcrypt.CompareHashAndPassword(
		[]byte(s.deps.Env.DashboardPasswordHash), []byte(req.Password))
	if err != nil {
		if !errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			// A malformed stored hash is an operator error, not a bad password.
			s.log.Error("comparing dashboard password", "error", err)
		}
		s.writeError(w, http.StatusUnauthorized, "invalid password")
		return
	}

	token, err := s.sessions.issue()
	if err != nil {
		s.log.Error("issuing session", "error", err)
		s.writeError(w, http.StatusInternalServerError, "could not start a session")
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
		Expires:  s.deps.Now().Add(SessionTTL),
	})
	w.WriteHeader(http.StatusNoContent)
}

// handleLogout revokes the caller's session and clears the cookie.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(SessionCookieName); err == nil {
		s.sessions.revoke(c.Value)
	}

	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
		MaxAge:   -1,
	})
	w.WriteHeader(http.StatusNoContent)
}

// handleSession reports whether the caller is authenticated. It is
// unauthenticated by design: the SPA calls it on load to decide between the
// dashboard and the login form.
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, SessionResponse{Authenticated: s.authenticated(r)})
}

func (s *Server) authenticated(r *http.Request) bool {
	c, err := r.Cookie(SessionCookieName)
	if err != nil {
		return false
	}
	return s.sessions.valid(c.Value)
}

// requireSession rejects unauthenticated requests with 401.
func (s *Server) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authenticated(r) {
			s.writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
