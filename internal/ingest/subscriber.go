package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrSubscriber is returned when the subscriber cannot be constructed.
var ErrSubscriber = errors.New("invalid subscriber options")

const (
	defaultMaxMessages = 100
	defaultIdleDelay   = 2 * time.Second
	minBackoff         = time.Second
	maxBackoff         = 60 * time.Second
)

// Stats are the subscriber's counters, surfaced by /api/health.
type Stats struct {
	Pulled            int
	Handled           int
	Ignored           int
	Unrouted          int
	Malformed         int
	SignatureFailures int
	PullErrors        int
	WriteErrors       int
	LastPullAt        time.Time
}

// MalformedHandler persists a message an adapter claimed but could not parse.
type MalformedHandler interface {
	HandleMalformed(ctx context.Context, m Message, cause string) error
}

// CursorTouch records a successful delivery for staleness detection.
type CursorTouch func(ctx context.Context, at time.Time) error

// SubscriberOptions configures the pull loop.
type SubscriberOptions struct {
	Puller    Puller
	Router    *Router
	Handler   EventHandler
	Malformed MalformedHandler
	Cursor    CursorTouch
	Logger    *slog.Logger

	MaxMessages int

	// IdleDelay is how long to wait after an empty pull.
	//
	// Verified against current Pub/Sub REST documentation: returnImmediately
	// is deprecated and left unset, so the server does hold the request for a
	// bounded time until a message arrives. Pacing therefore comes from the
	// server, not from here — but the same documentation says the call may
	// still return zero messages when that bound elapses, so a small delay
	// remains as a guard against spinning. Zero disables it entirely.
	IdleDelay time.Duration

	Clock func() time.Time
}

// Subscriber runs the pull loop: fetch, normalise, persist, acknowledge.
type Subscriber struct {
	opts  SubscriberOptions
	mu    sync.Mutex
	stats Stats
}

// NewSubscriber validates options and builds the loop.
func NewSubscriber(opts SubscriberOptions) (*Subscriber, error) {
	var problems []error
	if opts.Puller == nil {
		problems = append(problems, errors.New("Puller is required"))
	}
	if opts.Router == nil {
		problems = append(problems, errors.New("Router is required"))
	}
	if opts.Handler == nil {
		problems = append(problems, errors.New("Handler is required"))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w: %w", ErrSubscriber, errors.Join(problems...))
	}

	if opts.MaxMessages <= 0 {
		opts.MaxMessages = defaultMaxMessages
	}
	if opts.IdleDelay < 0 {
		opts.IdleDelay = defaultIdleDelay
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Subscriber{opts: opts}, nil
}

// Run pulls until ctx is cancelled. It never returns on a transport error:
// a stalled subscriber that exited would be indistinguishable from a healthy
// idle one, which is the silent failure SPEC §12 singles out.
func (s *Subscriber) Run(ctx context.Context) error {
	backoff := minBackoff

	for {
		if ctx.Err() != nil {
			return nil
		}

		messages, err := s.opts.Puller.Pull(ctx, s.opts.MaxMessages)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.record(func(st *Stats) { st.PullErrors++ })
			s.opts.Logger.Error("pulling messages", "error", err, "retry_in", backoff)

			if !sleepCtx(ctx, jitter(backoff)) {
				return nil
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = minBackoff

		if len(messages) == 0 {
			if s.opts.IdleDelay > 0 && !sleepCtx(ctx, s.opts.IdleDelay) {
				return nil
			}
			continue
		}

		s.processBatch(ctx, messages)
	}
}

// processBatch normalises and persists a batch, then acknowledges only the
// messages that reached a durable resting place (SPEC §4.2).
func (s *Subscriber) processBatch(ctx context.Context, messages []Message) {
	ackIDs := make([]string, 0, len(messages))

	for _, m := range messages {
		s.record(func(st *Stats) { st.Pulled++ })

		if s.handleMessage(ctx, m) {
			ackIDs = append(ackIDs, m.AckID)
		}
	}

	if len(ackIDs) == 0 {
		return
	}
	if err := s.opts.Puller.Ack(ctx, ackIDs); err != nil {
		// The messages are already durable, so a failed ack costs only a
		// redelivery, which the unique index absorbs.
		s.opts.Logger.Error("acknowledging messages", "error", err, "count", len(ackIDs))
		return
	}

	now := s.opts.Clock()
	s.record(func(st *Stats) { st.LastPullAt = now })

	if s.opts.Cursor != nil {
		if err := s.opts.Cursor(ctx, now); err != nil {
			s.opts.Logger.Error("touching ingest cursor", "error", err)
		}
	}
}

// handleMessage reports whether the message may be acknowledged. Every branch
// except a write failure acks, because no other case is fixed by redelivery
// (design §9).
func (s *Subscriber) handleMessage(ctx context.Context, m Message) bool {
	ev, err := s.opts.Router.Route(ctx, m)

	switch {
	case err == nil:
		if err := s.opts.Handler.Handle(ctx, ev); err != nil {
			s.record(func(st *Stats) { st.WriteErrors++ })
			s.opts.Logger.Error("persisting event", "error", err,
				"source", ev.Source, "source_ref", ev.SourceRef)
			// The only case redelivery genuinely fixes.
			return false
		}
		s.record(func(st *Stats) { st.Handled++ })
		return true

	case errors.Is(err, ErrIgnore):
		s.record(func(st *Stats) { st.Ignored++ })
		return true

	case errors.Is(err, ErrNoAdapter):
		s.record(func(st *Stats) { st.Unrouted++ })
		s.opts.Logger.Warn("no adapter claimed message", "message_id", m.ID,
			"attributes", len(m.Attributes))
		return true

	case errors.Is(err, ErrSignature):
		s.record(func(st *Stats) { st.SignatureFailures++ })
		// Logged loudly: this means either a misconfigured secret or an
		// attempt to inject events past the relay. Acked deliberately —
		// nacking would let an attacker drive an unbounded redelivery loop.
		s.opts.Logger.Error("webhook signature verification failed",
			"message_id", m.ID, "error", err)
		return true

	case errors.Is(err, ErrMalformed):
		s.record(func(st *Stats) { st.Malformed++ })
		if s.opts.Malformed != nil {
			if perr := s.opts.Malformed.HandleMalformed(ctx, m, err.Error()); perr != nil {
				s.opts.Logger.Error("persisting malformed message", "error", perr, "message_id", m.ID)
				return false
			}
		}
		return true

	default:
		s.record(func(st *Stats) { st.WriteErrors++ })
		s.opts.Logger.Error("routing message", "error", err, "message_id", m.ID)
		return false
	}
}

// Stats returns a snapshot of the counters.
func (s *Subscriber) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *Subscriber) record(mutate func(*Stats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mutate(&s.stats)
}

// jitter spreads retries so a transient outage does not produce a synchronised
// thundering herd when several sources recover together.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

// sleepCtx waits for d, reporting false if the context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
