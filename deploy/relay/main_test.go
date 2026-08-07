package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
)

type capturedPublish struct {
	data       []byte
	attributes map[string]string
}

type fakePublisher struct {
	published []capturedPublish
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, data []byte, attrs map[string]string) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, capturedPublish{data: data, attributes: attrs})
	return nil
}

const secret = "it-is-a-secret-to-everybody"

func signature(body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestRelayRejectsBadSignatures(t *testing.T) {
	body := []byte(`{"action":"completed"}`)

	tests := []struct {
		name string
		sig  string
	}{
		{name: "wrong secret", sig: "sha256=" + hex.EncodeToString([]byte("nope"))},
		{name: "missing header", sig: ""},
		{name: "no prefix", sig: "abcdef"},
		{name: "not hex", sig: "sha256=zzzz"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &fakePublisher{}
			handler := newHandler(secret, publisher)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("X-GitHub-Event", "workflow_run")
			if tc.sig != "" {
				req.Header.Set("X-Hub-Signature-256", tc.sig)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
			if len(publisher.published) != 0 {
				t.Error("a message was published despite a bad signature; the relay would be an open publish endpoint")
			}
		})
	}
}

func TestRelayPublishesVerifiedDeliveries(t *testing.T) {
	body := []byte(`{"action":"completed"}`)
	publisher := &fakePublisher{}
	handler := newHandler(secret, publisher)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "workflow_run")
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	req.Header.Set("X-Hub-Signature-256", signature(body))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(publisher.published))
	}

	got := publisher.published[0]
	if !bytes.Equal(got.data, body) {
		t.Errorf("published data = %q, want the raw body byte for byte", got.data)
	}

	t.Run("signature is forwarded so the control plane can re-verify", func(t *testing.T) {
		if got.attributes["x-hub-signature-256"] != signature(body) {
			t.Errorf("attributes = %v, want the forwarded signature", got.attributes)
		}
	})
	t.Run("event type is forwarded for adapter matching", func(t *testing.T) {
		if got.attributes["x-github-event"] != "workflow_run" {
			t.Errorf("attributes[x-github-event] = %q, want workflow_run", got.attributes["x-github-event"])
		}
	})
}

func TestRelayRejectsNonPost(t *testing.T) {
	handler := newHandler(secret, &fakePublisher{})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestRelayReportsPublishFailure(t *testing.T) {
	// A 5xx makes GitHub retry, which is what should happen when the message
	// never reached Pub/Sub.
	body := []byte(`{}`)
	handler := newHandler(secret, &fakePublisher{err: context.DeadlineExceeded})

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-Hub-Signature-256", signature(body))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 so GitHub retries", rec.Code)
	}
}
