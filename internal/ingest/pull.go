package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
)

// ErrPull is returned when the Pub/Sub REST API cannot be reached or its
// response cannot be understood.
var ErrPull = errors.New("pulling from pub/sub")

const (
	defaultPubSubBase = "https://pubsub.googleapis.com"
	pubSubScope       = "https://www.googleapis.com/auth/pubsub"

	// pullTimeout bounds one request. It must exceed the server's long-poll
	// hold, or every empty poll would surface as a client timeout.
	pullTimeout = 120 * time.Second
)

// Puller fetches and acknowledges messages. The interface exists so the
// subscriber can be driven by a fake in tests, and so a different transport can
// be substituted without touching the loop.
type Puller interface {
	Pull(ctx context.Context, max int) ([]Message, error)
	Ack(ctx context.Context, ackIDs []string) error
}

// RESTOptions configures the REST puller.
type RESTOptions struct {
	// Subscription is the fully-qualified name,
	// projects/<project>/subscriptions/<name>.
	Subscription string

	// Client must carry Pub/Sub credentials. Use NewPubSubClient in
	// production; tests pass an httptest client.
	Client *http.Client

	// BaseURL overrides the API root. Empty means the real service.
	BaseURL string
}

// RESTPuller talks to Pub/Sub over its REST API.
//
// This is design decision §2.1: the official gRPC client costs +14 MB of binary
// and takes the module graph from 32 to ~200, against the 15–25 MB RSS target
// the whole architecture exists to satisfy. The spec's ack-after-durable-write
// rule removes the main reason to want that client, because ack-deadline
// extension — the hardest part of a hand-rolled loop — is never needed when
// messages are acked within milliseconds of receipt.
//
// Long-poll behaviour, verified against current Pub/Sub REST documentation
// rather than assumed: returnImmediately is deprecated and must be left unset.
// With it unset the server waits, for a bounded time, until at least one
// message is available — a genuine server-side hold — but it may still return
// zero messages when that bound elapses. Two consequences follow: an empty
// pull is normal rather than an error, and the subscriber needs no aggressive
// poll interval (Task 14's IdleDelay exists only as a guard against spinning,
// not as the pacing mechanism).
type RESTPuller struct {
	subscription string
	client       *http.Client
	baseURL      string
}

// NewRESTPuller validates options and builds a puller.
func NewRESTPuller(opts RESTOptions) (*RESTPuller, error) {
	if opts.Subscription == "" {
		return nil, fmt.Errorf("%w: subscription is required", ErrPull)
	}
	if !strings.HasPrefix(opts.Subscription, "projects/") || !strings.Contains(opts.Subscription, "/subscriptions/") {
		return nil, fmt.Errorf(
			"%w: subscription %q must be projects/<project>/subscriptions/<name>",
			ErrPull, opts.Subscription)
	}

	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: pullTimeout}
	}
	baseURL := opts.BaseURL
	if baseURL == "" {
		baseURL = defaultPubSubBase
	}

	return &RESTPuller{
		subscription: opts.Subscription,
		client:       client,
		baseURL:      strings.TrimSuffix(baseURL, "/"),
	}, nil
}

// NewPubSubClient returns an HTTP client carrying application default
// credentials scoped to Pub/Sub. Token refresh is handled by the oauth2
// library, which is why hand-rolling the transport stays small.
func NewPubSubClient(ctx context.Context) (*http.Client, error) {
	client, err := google.DefaultClient(ctx, pubSubScope)
	if err != nil {
		return nil, fmt.Errorf("%w: obtaining google credentials: %w", ErrPull, err)
	}
	client.Timeout = pullTimeout
	return client, nil
}

type receivedMessage struct {
	AckID   string `json:"ackId"`
	Message struct {
		MessageID   string            `json:"messageId"`
		Data        string            `json:"data"`
		Attributes  map[string]string `json:"attributes"`
		PublishTime time.Time         `json:"publishTime"`
	} `json:"message"`
}

type pullResponse struct {
	ReceivedMessages []receivedMessage `json:"receivedMessages"`
}

// Pull fetches up to max messages. An empty result is not an error: it is the
// normal outcome of a long poll with no traffic.
//
// A message whose data will not base64-decode is skipped rather than failing
// the batch. One malformed message must not block every other message behind
// it, which would stall ingestion indefinitely.
func (p *RESTPuller) Pull(ctx context.Context, max int) ([]Message, error) {
	if max <= 0 {
		max = 100
	}

	// returnImmediately is deliberately omitted: it is deprecated, and setting
	// it true makes the server answer with zero messages even when a backlog
	// exists, which would turn every poll into a wasted round trip.
	body, err := p.post(ctx, "pull", map[string]any{"maxMessages": max})
	if err != nil {
		return nil, err
	}

	var decoded pullResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("%w: decoding pull response: %w", ErrPull, err)
	}

	messages := make([]Message, 0, len(decoded.ReceivedMessages))
	for _, rm := range decoded.ReceivedMessages {
		data, err := base64.StdEncoding.DecodeString(rm.Message.Data)
		if err != nil {
			continue
		}
		messages = append(messages, Message{
			ID:          rm.Message.MessageID,
			AckID:       rm.AckID,
			Data:        data,
			Attributes:  rm.Message.Attributes,
			PublishTime: rm.Message.PublishTime,
		})
	}
	return messages, nil
}

// Ack acknowledges messages. An empty list is a no-op rather than a request.
func (p *RESTPuller) Ack(ctx context.Context, ackIDs []string) error {
	if len(ackIDs) == 0 {
		return nil
	}
	_, err := p.post(ctx, "acknowledge", map[string]any{"ackIds": ackIDs})
	return err
}

func (p *RESTPuller) post(ctx context.Context, action string, payload any) ([]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding %s request: %w", ErrPull, action, err)
	}

	url := fmt.Sprintf("%s/v1/%s:%s", p.baseURL, p.subscription, action)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("%w: building %s request: %w", ErrPull, action, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %s request: %w", ErrPull, action, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Cap the read so a pathological response cannot exhaust memory on an
	// 8 GB host.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: reading %s response: %w", ErrPull, action, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s returned %s: %s",
			ErrPull, action, resp.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}
