package httpapi

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
)

// handleStream streams bus events to a dashboard client as Server-Sent Events.
//
// Ordering is deliberate: headers are flushed first so the browser opens the
// stream immediately, then any replay is written, then live events. Heartbeat
// comments keep intermediaries and browsers from closing an idle connection
// (SPEC §4.11).
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	lastEventID, err := parseLastEventID(r.Header.Get("Last-Event-ID"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	topics := parseTopics(r.URL.Query().Get("topics"))

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	// Defeats proxy response buffering, which would otherwise hold frames until
	// a buffer filled and make the stream look dead.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		s.log.Error("flushing sse headers", "error", err)
		return
	}

	// Subscribe before replaying so no event emitted during replay is missed.
	client := s.deps.Hub.Subscribe(topics...)
	defer s.deps.Hub.Unsubscribe(client)

	if s.deps.Replay != nil && lastEventID > 0 {
		events, err := s.deps.Replay(r.Context(), lastEventID, topics)
		if err != nil {
			// The live stream is still useful, so log and carry on rather than
			// failing a connection that can still deliver value.
			s.log.Error("replaying sse events", "error", err, "last_event_id", lastEventID)
		}
		for _, ev := range events {
			if err := writeSSEEvent(w, ev); err != nil {
				return
			}
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}

	heartbeat := time.NewTicker(s.deps.HeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case ev, ok := <-client.Events():
			if !ok {
				return
			}
			if err := writeSSEEvent(w, ev); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}

		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		}
	}
}

// writeSSEEvent writes one event frame. The id field is omitted when ID is 0,
// which means the event was never persisted and so cannot be replayed from.
func writeSSEEvent(w http.ResponseWriter, ev bus.Event) error {
	if ev.ID > 0 {
		if _, err := fmt.Fprintf(w, "id: %d\n", ev.ID); err != nil {
			return err
		}
	}
	if ev.Type != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", ev.Type); err != nil {
			return err
		}
	}

	data := "{}"
	if len(ev.Data) > 0 {
		data = string(ev.Data)
	}
	_, err := fmt.Fprintf(w, "data: %s\n\n", data)
	return err
}

// parseTopics splits a comma-separated topics parameter. An empty parameter
// means every topic.
func parseTopics(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	topics := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			topics = append(topics, p)
		}
	}
	return topics
}

// parseLastEventID parses the Last-Event-ID header. An absent header is 0. A
// malformed one is rejected rather than ignored, so a client bug surfaces as a
// 400 instead of silently losing replay.
func parseLastEventID(raw string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}

	id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Last-Event-ID %q must be an integer", raw)
	}
	if id < 0 {
		return 0, fmt.Errorf("Last-Event-ID %q must not be negative", raw)
	}
	return id, nil
}
