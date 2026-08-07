package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func gcpMessage(t *testing.T, file string) Message {
	t.Helper()
	return Message{
		ID:   "msg-gcp",
		Data: readPayload(t, file),
		Attributes: map[string]string{
			"logging.googleapis.com/timestamp": "2026-08-02T11:04:05Z",
		},
	}
}

func testGCPAdapter(t *testing.T) *GCPLogAdapter {
	t.Helper()
	return NewGCPLogAdapter(NewRegistryResolver(registryFixture(t)))
}

func TestGCPLogAdapterMatch(t *testing.T) {
	a := testGCPAdapter(t)

	tests := []struct {
		name  string
		attrs map[string]string
		want  bool
	}{
		{name: "logging timestamp attribute", attrs: map[string]string{"logging.googleapis.com/timestamp": "x"}, want: true},
		{name: "log name attribute", attrs: map[string]string{"logging.googleapis.com/logName": "x"}, want: true},
		{name: "github attributes are not ours", attrs: map[string]string{"x-github-event": "push"}, want: false},
		{name: "nil attributes", attrs: nil, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Match(tc.attrs); got != tc.want {
				t.Errorf("Match(%v) = %v, want %v", tc.attrs, got, tc.want)
			}
		})
	}
}

func TestGCPLogAdapterNormalizesTextPayload(t *testing.T) {
	ev, err := testGCPAdapter(t).Normalize(context.Background(), gcpMessage(t, "gcplog_text_error.json"))
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil", err)
	}

	checks := []struct{ name, got, want string }{
		{name: "source", got: ev.Source, want: "gcplog"},
		{name: "kind", got: ev.Kind, want: "log.error"},
		{name: "source ref is the insert id", got: ev.SourceRef, want: "gcplog:1a2b3c4d5e"},
		{name: "slug resolved from service_name", got: ev.ProjectSlug, want: "example-api"},
	}
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("got %q, want %q", c.got, c.want)
			}
		})
	}

	t.Run("title is the first payload line", func(t *testing.T) {
		if ev.Title != "TypeError: Cannot read properties of undefined" {
			t.Errorf("Title = %q, want the first line", ev.Title)
		}
	})

	t.Run("body keeps the stack for fingerprinting", func(t *testing.T) {
		if !strings.Contains(ev.Body, "src/index.js") {
			t.Errorf("Body = %q, want it to retain the stack frames", ev.Body)
		}
	})

	t.Run("severity is in metadata", func(t *testing.T) {
		if ev.Metadata["severity"] != "ERROR" {
			t.Errorf("Metadata[severity] = %q, want ERROR", ev.Metadata["severity"])
		}
	})

	t.Run("timestamp is parsed", func(t *testing.T) {
		if ev.OccurredAt.IsZero() {
			t.Error("OccurredAt is zero")
		}
	})
}

func TestGCPLogAdapterPrefersStackTraceFromJSONPayload(t *testing.T) {
	ev, err := testGCPAdapter(t).Normalize(context.Background(), gcpMessage(t, "gcplog_json_stack.json"))
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil", err)
	}
	if ev.ProjectSlug != "example-worker" {
		t.Errorf("ProjectSlug = %q, want %q", ev.ProjectSlug, "example-worker")
	}
	if !strings.Contains(ev.Body, "worker.py") {
		t.Errorf("Body = %q, want the stack_trace field, which is what fingerprinting needs", ev.Body)
	}
	if ev.Title != "ValueError: invalid literal for int() with base 10: 'x'" {
		t.Errorf("Title = %q, want the message field", ev.Title)
	}
}

func TestGCPLogAdapterIgnoresLowSeverity(t *testing.T) {
	tests := []struct{ name, severity string }{
		{name: "info", severity: "INFO"},
		{name: "debug", severity: "DEBUG"},
		{name: "notice", severity: "NOTICE"},
		{name: "warning", severity: "WARNING"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.ReplaceAll(string(readPayload(t, "gcplog_text_error.json")),
				`"severity": "ERROR"`, `"severity": "`+tc.severity+`"`)
			_, err := testGCPAdapter(t).Normalize(context.Background(), Message{
				Data:       []byte(body),
				Attributes: map[string]string{"logging.googleapis.com/timestamp": "x"},
			})
			if !errors.Is(err, ErrIgnore) {
				t.Errorf("Normalize() error = %v, want ErrIgnore for severity %s", err, tc.severity)
			}
		})
	}
}

func TestGCPLogAdapterRejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		body string
		want error
	}{
		{name: "not json", body: `{"insertId":`, want: ErrMalformed},
		{name: "no insert id", body: `{"severity":"ERROR","textPayload":"boom"}`, want: ErrMalformed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := testGCPAdapter(t).Normalize(context.Background(), Message{
				Data:       []byte(tc.body),
				Attributes: map[string]string{"logging.googleapis.com/timestamp": "x"},
			})
			if !errors.Is(err, tc.want) {
				t.Errorf("Normalize() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestGCPLogAdapterUnresolvedLabelsAreUnroutable(t *testing.T) {
	body := strings.ReplaceAll(string(readPayload(t, "gcplog_text_error.json")),
		"example-api", "not-registered")

	ev, err := testGCPAdapter(t).Normalize(context.Background(), Message{
		Data:       []byte(body),
		Attributes: map[string]string{"logging.googleapis.com/timestamp": "x"},
	})
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil; an unroutable entry must still normalise", err)
	}
	if ev.ProjectSlug != "" {
		t.Errorf("ProjectSlug = %q, want empty", ev.ProjectSlug)
	}
}
