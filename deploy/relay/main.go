// Command relay receives GitHub webhooks and republishes them to Pub/Sub.
//
// It exists because the sentinel host is behind NAT and cannot receive
// inbound webhooks (SPEC §1.3, §4.2.1). It is a separate Go module on
// purpose: it needs the Google client libraries, and keeping those out of the
// root module is what holds the sentinel binary to ~12 MB.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
)

// maxBody caps a webhook payload. GitHub's own limit is 25 MB.
const maxBody = 25 << 20

// forwardedHeaders are copied onto the Pub/Sub message as attributes. The
// signature is included so the control plane can independently re-verify it:
// a compromised relay must not be able to inject events (SPEC §4.2.1, §10).
var forwardedHeaders = []string{
	"X-GitHub-Event",
	"X-GitHub-Delivery",
	"X-GitHub-Hook-ID",
	"X-Hub-Signature-256",
}

// publisher publishes one message. The interface exists so the handler is
// testable without a Pub/Sub client.
type publisher interface {
	Publish(ctx context.Context, data []byte, attributes map[string]string) error
}

type pubsubPublisher struct {
	client *pubsub.Client
	topic  string
}

func (p *pubsubPublisher) Publish(ctx context.Context, data []byte, attributes map[string]string) error {
	result := p.client.Publisher(p.topic).Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	})
	if _, err := result.Get(ctx); err != nil {
		return fmt.Errorf("publishing to %s: %w", p.topic, err)
	}
	return nil
}

// newHandler builds the webhook handler.
//
// Verification happens before anything else. Without it the relay would be an
// open publish endpoint: anyone who found the Cloud Run URL could inject
// arbitrary events into the sentinel's ingestion path.
func newHandler(secret string, pub publisher) http.Handler {
	key := []byte(secret)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}

		if err := verify(key, r.Header.Get("X-Hub-Signature-256"), body); err != nil {
			slog.Warn("rejected delivery", "error", err,
				"delivery", r.Header.Get("X-GitHub-Delivery"))
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}

		attributes := make(map[string]string, len(forwardedHeaders))
		for _, header := range forwardedHeaders {
			if value := r.Header.Get(header); value != "" {
				attributes[strings.ToLower(header)] = value
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()

		if err := pub.Publish(ctx, body, attributes); err != nil {
			slog.Error("publishing delivery", "error", err,
				"delivery", r.Header.Get("X-GitHub-Delivery"))
			// A 5xx makes GitHub retry, which is correct: the message never
			// reached Pub/Sub, so dropping it would lose the event.
			http.Error(w, "publish failed", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
}

// verify checks the HMAC in constant time. Comparing hex strings with ==
// would leak timing information about the expected digest.
func verify(key []byte, header string, body []byte) error {
	encoded, found := strings.CutPrefix(header, "sha256=")
	if !found {
		return errors.New("missing or malformed X-Hub-Signature-256")
	}
	provided, err := hex.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("signature is not hex: %w", err)
	}

	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return errors.New("digest mismatch")
	}
	return nil
}

func main() {
	secret := os.Getenv("GITHUB_WEBHOOK_SECRET")
	project := os.Getenv("GCP_PROJECT_ID")
	topic := os.Getenv("PUBSUB_TOPIC")

	if secret == "" || project == "" || topic == "" {
		slog.Error("GITHUB_WEBHOOK_SECRET, GCP_PROJECT_ID and PUBSUB_TOPIC are all required")
		os.Exit(1)
	}

	client, err := pubsub.NewClient(context.Background(), project)
	if err != nil {
		slog.Error("creating pubsub client", "error", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	handler := newHandler(secret, &pubsubPublisher{
		client: client,
		topic:  fmt.Sprintf("projects/%s/topics/%s", project, topic),
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	slog.Info("relay listening", "port", port, "topic", topic)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("serving", "error", err)
		os.Exit(1)
	}
}
