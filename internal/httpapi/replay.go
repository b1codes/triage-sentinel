package httpapi

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/store"
)

// replayLimit bounds a single reconnect backfill. A tab that has been closed
// for a week refetches over HTTP instead of replaying a month of history.
const replayLimit = 500

// NewStoreReplay returns the ReplayFunc the SSE handler uses to backfill a
// reconnecting client.
//
// This is the seam M0 declared and wired to nil. store deliberately imports
// nothing but config (SPEC §4), so it returns []store.IncidentEvent and the
// mapping to bus.Event lives here — the alternative would invert the dependency
// graph.
func NewStoreReplay(db *store.DB) ReplayFunc {
	return func(ctx context.Context, lastEventID int64, topics []string) ([]bus.Event, error) {
		if !wantsTopic(topics, topicIncidents) {
			return nil, nil
		}

		rows, err := store.EventsAfter(ctx, db, lastEventID, replayLimit)
		if err != nil {
			return nil, fmt.Errorf("replaying incident events: %w", err)
		}

		events := make([]bus.Event, 0, len(rows))
		for _, row := range rows {
			data, err := json.Marshal(map[string]any{
				"incident_id": row.IncidentID,
				"kind":        row.Kind,
				"actor":       row.Actor,
				"from_state":  row.FromState,
				"state":       row.ToState,
				"ts":          row.TS,
				"payload":     json.RawMessage(row.Payload),
			})
			if err != nil {
				return nil, fmt.Errorf("encoding replay event %d: %w", row.ID, err)
			}
			events = append(events, bus.Event{
				ID:    row.ID,
				Topic: topicIncidents,
				Type:  "incident." + row.Kind,
				Data:  data,
			})
		}
		return events, nil
	}
}

// wantsTopic reports whether a subscriber wants a topic. An empty topic list
// means every topic, matching bus.Client.wants.
func wantsTopic(topics []string, topic string) bool {
	if len(topics) == 0 {
		return true
	}
	for _, t := range topics {
		if t == topic {
			return true
		}
	}
	return false
}
