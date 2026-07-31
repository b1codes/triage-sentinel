package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
)

// readUntil reads lines until the accumulated text contains want, or fails.
func readUntil(t *testing.T, r *bufio.Reader, want string, timeout time.Duration) string {
	t.Helper()

	type result struct {
		text string
		err  error
	}
	lines := make(chan result, 64)

	go func() {
		for {
			line, err := r.ReadString('\n')
			lines <- result{text: line, err: err}
			if err != nil {
				return
			}
		}
	}()

	var sb strings.Builder
	deadline := time.After(timeout)
	for {
		select {
		case res := <-lines:
			sb.WriteString(res.text)
			if strings.Contains(sb.String(), want) {
				return sb.String()
			}
			if res.err != nil {
				t.Fatalf("stream ended before %q appeared; got:\n%s", want, sb.String())
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q; got:\n%s", want, sb.String())
		}
	}
}

// startStream boots an httptest server and opens a streaming request.
func startStream(t *testing.T, srv *Server, query string, header http.Header) *bufio.Reader {
	t.Helper()

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/stream"+query, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	// /api/stream is behind requireSession (Task 12); issue a session directly
	// rather than round-tripping a login for every streaming test.
	token, err := srv.sessions.issue()
	if err != nil {
		t.Fatalf("issuing session: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: token})

	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}
	if got := resp.Header.Get("Cache-Control"); !strings.Contains(got, "no-cache") {
		t.Errorf("Cache-Control = %q, want it to contain no-cache", got)
	}
	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", got)
	}

	return bufio.NewReader(resp.Body)
}

func TestStreamDeliversPublishedEvent(t *testing.T) {
	srv := newTestServer(t, func(d *Deps) { d.HeartbeatInterval = time.Hour })
	r := startStream(t, srv, "?topics=incidents", nil)

	// Wait for the hub to register the subscriber before publishing.
	waitForClients(t, srv, 1)

	srv.deps.Hub.Publish(bus.Event{
		ID:    7,
		Topic: "incidents",
		Type:  "created",
		Data:  json.RawMessage(`{"slug":"api"}`),
	})

	got := readUntil(t, r, "\n\n", 3*time.Second)

	for _, want := range []string{"id: 7", "event: created", `data: {"slug":"api"}`} {
		if !strings.Contains(got, want) {
			t.Errorf("frame %q missing %q", got, want)
		}
	}
}

func TestStreamOmitsIDWhenZero(t *testing.T) {
	srv := newTestServer(t, func(d *Deps) { d.HeartbeatInterval = time.Hour })
	r := startStream(t, srv, "", nil)
	waitForClients(t, srv, 1)

	srv.deps.Hub.Publish(bus.Event{Topic: "runs", Type: "started"})

	got := readUntil(t, r, "\n\n", 3*time.Second)
	if strings.Contains(got, "id:") {
		t.Errorf("frame %q contains an id field; ID 0 means unpersisted and must be omitted", got)
	}
	if !strings.Contains(got, "event: started") {
		t.Errorf("frame %q missing event: started", got)
	}
}

func TestStreamFiltersByTopic(t *testing.T) {
	srv := newTestServer(t, func(d *Deps) { d.HeartbeatInterval = time.Hour })
	r := startStream(t, srv, "?topics=incidents", nil)
	waitForClients(t, srv, 1)

	srv.deps.Hub.Publish(bus.Event{Topic: "runs", Type: "should-not-arrive"})
	srv.deps.Hub.Publish(bus.Event{Topic: "incidents", Type: "should-arrive"})

	got := readUntil(t, r, "should-arrive", 3*time.Second)
	if strings.Contains(got, "should-not-arrive") {
		t.Errorf("received an event for an unsubscribed topic:\n%s", got)
	}
}

func TestStreamSendsHeartbeat(t *testing.T) {
	srv := newTestServer(t, func(d *Deps) { d.HeartbeatInterval = 20 * time.Millisecond })
	r := startStream(t, srv, "", nil)

	got := readUntil(t, r, ": heartbeat", 3*time.Second)
	if !strings.Contains(got, ": heartbeat") {
		t.Errorf("no heartbeat comment in:\n%s", got)
	}
}

func TestStreamRejectsInvalidLastEventID(t *testing.T) {
	srv := newTestServer(t, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/stream", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Last-Event-ID", "not-a-number")
	token, err := srv.sessions.issue()
	if err != nil {
		t.Fatalf("issuing session: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: token})

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestStreamReplaysBeforeLiveEvents(t *testing.T) {
	var gotLastID int64
	var gotTopics []string

	srv := newTestServer(t, func(d *Deps) {
		d.HeartbeatInterval = time.Hour
		d.Replay = func(_ context.Context, lastEventID int64, topics []string) ([]bus.Event, error) {
			gotLastID = lastEventID
			gotTopics = topics
			return []bus.Event{
				{ID: 11, Topic: "incidents", Type: "replayed-one"},
				{ID: 12, Topic: "incidents", Type: "replayed-two"},
			}, nil
		}
	})

	r := startStream(t, srv, "?topics=incidents", http.Header{"Last-Event-ID": []string{"10"}})

	got := readUntil(t, r, "replayed-two", 3*time.Second)

	if gotLastID != 10 {
		t.Errorf("Replay received lastEventID = %d, want 10", gotLastID)
	}
	if len(gotTopics) != 1 || gotTopics[0] != "incidents" {
		t.Errorf("Replay received topics = %v, want [incidents]", gotTopics)
	}
	if !strings.Contains(got, "id: 11") || !strings.Contains(got, "id: 12") {
		t.Errorf("replayed frames missing from:\n%s", got)
	}
	if strings.Index(got, "replayed-one") > strings.Index(got, "replayed-two") {
		t.Error("replayed events arrived out of order")
	}
}

func TestStreamSkipsReplayWithoutLastEventID(t *testing.T) {
	called := false
	srv := newTestServer(t, func(d *Deps) {
		d.HeartbeatInterval = 20 * time.Millisecond
		d.Replay = func(context.Context, int64, []string) ([]bus.Event, error) {
			called = true
			return nil, nil
		}
	})

	r := startStream(t, srv, "", nil)
	readUntil(t, r, ": heartbeat", 3*time.Second)

	if called {
		t.Error("Replay was called without a Last-Event-ID header")
	}
}

func TestStreamUnsubscribesOnDisconnect(t *testing.T) {
	srv := newTestServer(t, func(d *Deps) { d.HeartbeatInterval = 20 * time.Millisecond })

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/stream", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	token, err := srv.sessions.issue()
	if err != nil {
		t.Fatalf("issuing session: %v", err)
	}
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: token})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("opening stream: %v", err)
	}

	waitForClients(t, srv, 1)

	cancel()
	_ = resp.Body.Close()

	waitForClients(t, srv, 0)
}

func waitForClients(t *testing.T, srv *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srv.deps.Hub.ClientCount() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("hub ClientCount() = %d, want %d", srv.deps.Hub.ClientCount(), want)
}
