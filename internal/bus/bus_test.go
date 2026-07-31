package bus

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func drain(c *Client) []Event {
	var got []Event
	for {
		select {
		case ev, ok := <-c.Events():
			if !ok {
				return got
			}
			got = append(got, ev)
		default:
			return got
		}
	}
}

func TestPublishDeliversToMatchingTopic(t *testing.T) {
	h := NewHub(8)
	defer h.Close()

	c := h.Subscribe("incidents")
	h.Publish(Event{ID: 1, Topic: "incidents", Type: "created"})

	got := drain(c)
	if len(got) != 1 {
		t.Fatalf("received %d events, want 1", len(got))
	}
	if got[0].ID != 1 || got[0].Type != "created" {
		t.Errorf("got %+v, want ID 1 type created", got[0])
	}
}

func TestPublishSkipsNonMatchingTopic(t *testing.T) {
	h := NewHub(8)
	defer h.Close()

	c := h.Subscribe("incidents")
	h.Publish(Event{ID: 1, Topic: "runs", Type: "started"})

	if got := drain(c); len(got) != 0 {
		t.Errorf("received %d events, want 0", len(got))
	}
}

func TestSubscribeWithNoTopicsReceivesEverything(t *testing.T) {
	h := NewHub(8)
	defer h.Close()

	c := h.Subscribe()
	h.Publish(Event{ID: 1, Topic: "incidents"})
	h.Publish(Event{ID: 2, Topic: "runs"})
	h.Publish(Event{ID: 3, Topic: "budget"})

	if got := drain(c); len(got) != 3 {
		t.Errorf("received %d events, want 3", len(got))
	}
}

func TestPublishPreservesCallerSuppliedID(t *testing.T) {
	h := NewHub(8)
	defer h.Close()

	c := h.Subscribe()
	h.Publish(Event{ID: 4242, Topic: "incidents"})

	got := drain(c)
	if len(got) != 1 {
		t.Fatalf("received %d events, want 1", len(got))
	}
	if got[0].ID != 4242 {
		t.Errorf("ID = %d, want 4242; the hub must not assign or rewrite IDs", got[0].ID)
	}
}

func TestPublishDataIsCarriedThrough(t *testing.T) {
	h := NewHub(8)
	defer h.Close()

	c := h.Subscribe()
	h.Publish(Event{Topic: "incidents", Type: "created", Data: json.RawMessage(`{"slug":"api"}`)})

	got := drain(c)
	if len(got) != 1 {
		t.Fatalf("received %d events, want 1", len(got))
	}
	if string(got[0].Data) != `{"slug":"api"}` {
		t.Errorf("Data = %s, want {\"slug\":\"api\"}", got[0].Data)
	}
}

// TestSlowConsumerGetsResyncAndHubNeverBlocks is the core backpressure
// guarantee: a browser tab that stops reading must never be able to make the
// hub grow memory or stall a publisher.
func TestSlowConsumerGetsResyncAndHubNeverBlocks(t *testing.T) {
	const bufSize = 2
	h := NewHub(bufSize)
	defer h.Close()

	c := h.Subscribe("incidents")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 10; i++ {
			h.Publish(Event{ID: int64(i), Topic: "incidents", Type: "created"})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a slow consumer; the hub must never block")
	}

	got := drain(c)
	if len(got) > bufSize {
		t.Errorf("buffered %d events, want <= %d", len(got), bufSize)
	}

	var sawResync bool
	for _, ev := range got {
		if ev.Type == TypeResync {
			sawResync = true
		}
	}
	if !sawResync {
		t.Error("slow consumer received no resync event; it would never know to refetch")
	}
}

func TestFastConsumerIsUnaffectedBySlowOne(t *testing.T) {
	h := NewHub(2)
	defer h.Close()

	slow := h.Subscribe("incidents")
	fast := h.Subscribe("incidents")

	var wg sync.WaitGroup
	wg.Add(1)
	received := 0
	go func() {
		defer wg.Done()
		timeout := time.After(2 * time.Second)
		for received < 5 {
			select {
			case ev, ok := <-fast.Events():
				if !ok {
					return
				}
				if ev.Type != TypeResync {
					received++
				}
			case <-timeout:
				return
			}
		}
	}()

	for i := 1; i <= 5; i++ {
		h.Publish(Event{ID: int64(i), Topic: "incidents"})
		// Pace publishes slightly so the actively-draining fast consumer's
		// goroutine (blocked in a select with a timer case, which is slower to
		// service than the publisher's plain non-blocking send, especially
		// under -race instrumentation) gets scheduled between sends. Without
		// this, the publisher can legitimately outrun the reader and overflow
		// both clients regardless of hub correctness, since nothing here
		// guarantees the reader goroutine runs between iterations.
		time.Sleep(2 * time.Millisecond)
	}
	wg.Wait()

	if received != 5 {
		t.Errorf("fast consumer received %d events, want 5", received)
	}
	_ = slow
}

func TestClientCountTracksSubscriptions(t *testing.T) {
	h := NewHub(4)
	defer h.Close()

	if got := h.ClientCount(); got != 0 {
		t.Errorf("ClientCount() = %d, want 0", got)
	}

	a := h.Subscribe("incidents")
	b := h.Subscribe("runs")
	if got := h.ClientCount(); got != 2 {
		t.Errorf("ClientCount() = %d, want 2", got)
	}

	h.Unsubscribe(a)
	if got := h.ClientCount(); got != 1 {
		t.Errorf("ClientCount() after Unsubscribe = %d, want 1", got)
	}

	h.Unsubscribe(b)
	if got := h.ClientCount(); got != 0 {
		t.Errorf("ClientCount() after all Unsubscribe = %d, want 0", got)
	}
}

func TestUnsubscribeIsIdempotent(t *testing.T) {
	h := NewHub(4)
	defer h.Close()

	c := h.Subscribe("incidents")
	h.Unsubscribe(c)
	h.Unsubscribe(c) // must not panic on a double close

	if got := h.ClientCount(); got != 0 {
		t.Errorf("ClientCount() = %d, want 0", got)
	}
}

func TestUnsubscribedClientChannelIsClosed(t *testing.T) {
	h := NewHub(4)
	defer h.Close()

	c := h.Subscribe("incidents")
	h.Unsubscribe(c)

	select {
	case _, ok := <-c.Events():
		if ok {
			t.Error("channel delivered an event after Unsubscribe")
		}
	case <-time.After(time.Second):
		t.Error("channel not closed after Unsubscribe; an SSE handler would leak")
	}
}

func TestCloseClosesEveryClient(t *testing.T) {
	h := NewHub(4)
	a := h.Subscribe("incidents")
	b := h.Subscribe("runs")

	h.Close()

	for name, c := range map[string]*Client{"a": a, "b": b} {
		select {
		case _, ok := <-c.Events():
			if ok {
				t.Errorf("client %s delivered an event after Close", name)
			}
		case <-time.After(time.Second):
			t.Errorf("client %s channel not closed after Close", name)
		}
	}
}

func TestPublishAfterCloseIsNoOp(t *testing.T) {
	h := NewHub(4)
	h.Close()
	h.Publish(Event{Topic: "incidents"}) // must not panic
	if got := h.ClientCount(); got != 0 {
		t.Errorf("ClientCount() = %d, want 0", got)
	}
}

func TestSubscribeAfterCloseReturnsClosedClient(t *testing.T) {
	h := NewHub(4)
	h.Close()

	c := h.Subscribe("incidents")
	select {
	case _, ok := <-c.Events():
		if ok {
			t.Error("client from a closed hub delivered an event")
		}
	case <-time.After(time.Second):
		t.Error("client from a closed hub has an open channel; a handler would block forever")
	}
}

func TestConcurrentPublishSubscribeUnsubscribe(t *testing.T) {
	h := NewHub(4)
	defer h.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					h.Publish(Event{Topic: "incidents", Type: "created"})
				}
			}
		}()
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c := h.Subscribe("incidents")
					drain(c)
					h.Unsubscribe(c)
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestTopicsAreCopied(t *testing.T) {
	h := NewHub(4)
	defer h.Close()

	topics := []string{"incidents"}
	c := h.Subscribe(topics...)
	topics[0] = "clobbered"

	if got := c.Topics(); len(got) != 1 || got[0] != "incidents" {
		t.Errorf("Topics() = %v, want [incidents]; the hub must copy the caller's slice", got)
	}
}
