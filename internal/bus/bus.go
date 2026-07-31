// Package bus fans server-sent events out to connected dashboard clients.
package bus

import (
	"encoding/json"
	"sort"
	"sync"
)

// TypeResync tells a client that it fell behind and must refetch current state
// over HTTP rather than reconstruct it from the stream.
const TypeResync = "resync"

// Event is one message on the bus.
type Event struct {
	// ID is supplied by the publisher and never assigned or rewritten by the
	// hub. From M1 it carries incident_events.id so SSE Last-Event-ID replay
	// reads from the same sequence as the audit trail; a hub-local counter
	// would create a second ID space that silently diverges.
	ID int64 `json:"id"`

	Topic string          `json:"topic"`
	Type  string          `json:"type"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// Client is one subscriber's view of the bus.
type Client struct {
	ch     chan Event
	topics map[string]bool

	// lastWasResync suppresses a run of consecutive resync events; one is
	// enough to make a client refetch.
	lastWasResync bool
	closed        bool
}

// Events returns the client's event channel. It is closed when the client is
// unsubscribed or the hub shuts down, which is the signal for an SSE handler to
// return.
func (c *Client) Events() <-chan Event { return c.ch }

// Topics returns the client's subscribed topics in sorted order. An empty slice
// means every topic.
func (c *Client) Topics() []string {
	topics := make([]string, 0, len(c.topics))
	for t := range c.topics {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	return topics
}

// wants reports whether this client should receive an event on topic. A client
// with no topics receives everything.
func (c *Client) wants(topic string) bool {
	if len(c.topics) == 0 {
		return true
	}
	return c.topics[topic]
}

// Hub broadcasts events to subscribed clients without ever blocking on a slow
// one (SPEC §4.11).
type Hub struct {
	mu      sync.Mutex
	clients map[*Client]struct{}
	bufSize int
	closed  bool
}

// NewHub creates a hub whose clients each buffer bufSize events. A bufSize
// below 1 is raised to 1 so a client can always hold a resync.
func NewHub(bufSize int) *Hub {
	if bufSize < 1 {
		bufSize = 1
	}
	return &Hub{
		clients: make(map[*Client]struct{}),
		bufSize: bufSize,
	}
}

// Subscribe registers a client for the given topics; passing none subscribes to
// every topic. Subscribing to a closed hub returns a client whose channel is
// already closed, so callers need no special case.
func (h *Hub) Subscribe(topics ...string) *Client {
	set := make(map[string]bool, len(topics))
	for _, t := range topics {
		if t != "" {
			set[t] = true
		}
	}

	c := &Client{
		ch:     make(chan Event, h.bufSize),
		topics: set,
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		c.closed = true
		close(c.ch)
		return c
	}
	h.clients[c] = struct{}{}
	return c
}

// Unsubscribe removes a client and closes its channel. It is safe to call more
// than once.
func (h *Hub) Unsubscribe(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	h.closeClientLocked(c)
}

// Publish delivers ev to every subscribed client. It never blocks: a client
// whose buffer is full has that buffer drained and replaced with a single
// resync event, because everything queued for it was already stale.
func (h *Hub) Publish(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	for c := range h.clients {
		if c.closed || !c.wants(ev.Topic) {
			continue
		}

		select {
		case c.ch <- ev:
			c.lastWasResync = false
		default:
			h.overflowLocked(c, ev.Topic)
		}
	}
}

// overflowLocked handles a full client buffer. The caller must hold h.mu.
func (h *Hub) overflowLocked(c *Client, topic string) {
	// Drop everything queued: it is stale, and a partial history is worse for
	// the client than an instruction to refetch.
	for {
		select {
		case <-c.ch:
		default:
			goto drained
		}
	}

drained:
	if c.lastWasResync {
		return
	}
	select {
	case c.ch <- Event{Topic: topic, Type: TypeResync}:
		c.lastWasResync = true
	default:
		// Unreachable: the buffer was just drained and holds at least one slot.
	}
}

// ClientCount returns the number of subscribed clients. It is reported by
// /api/health (SPEC §12).
func (h *Hub) ClientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// Close unsubscribes every client and rejects further publishing. It is safe to
// call more than once.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}
	h.closed = true

	for c := range h.clients {
		h.closeClientLocked(c)
		delete(h.clients, c)
	}
}

// closeClientLocked closes a client's channel exactly once. The caller must
// hold h.mu.
func (h *Hub) closeClientLocked(c *Client) {
	if c.closed {
		return
	}
	c.closed = true
	close(c.ch)
}
