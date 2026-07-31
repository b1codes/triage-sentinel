package httpapi

import "time"

// sessionStore holds dashboard sessions in memory. Sessions therefore end when
// the process restarts, which is acceptable for a single-operator loopback
// service and avoids persisting a credential-equivalent token (SPEC §14).
//
// Task 12 adds issue/validate/revoke.
type sessionStore struct {
	now func() time.Time
}

func newSessionStore(now func() time.Time) *sessionStore {
	return &sessionStore{now: now}
}
