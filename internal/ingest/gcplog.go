package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// actionableSeverities are the Cloud Logging severities worth opening an
// incident for. Anything lower is ignored at zero cost, before any storage or
// fingerprinting work happens.
var actionableSeverities = map[string]bool{
	"ERROR": true, "CRITICAL": true, "ALERT": true, "EMERGENCY": true,
}

// GCPLogAdapter normalises Cloud Logging entries delivered by a logging sink.
type GCPLogAdapter struct {
	resolver Resolver
}

// NewGCPLogAdapter builds the adapter.
func NewGCPLogAdapter(resolver Resolver) *GCPLogAdapter {
	return &GCPLogAdapter{resolver: resolver}
}

// Name identifies the adapter.
func (a *GCPLogAdapter) Name() string { return "gcplog" }

// Match claims any message carrying a Cloud Logging attribute. A logging sink
// stamps these onto every message it publishes.
func (a *GCPLogAdapter) Match(attrs map[string]string) bool {
	for k := range attrs {
		if strings.HasPrefix(strings.ToLower(k), "logging.googleapis.com/") {
			return true
		}
	}
	return false
}

type logResource struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels"`
}

type logEntry struct {
	InsertID    string            `json:"insertId"`
	Timestamp   time.Time         `json:"timestamp"`
	Severity    string            `json:"severity"`
	LogName     string            `json:"logName"`
	Resource    logResource       `json:"resource"`
	Labels      map[string]string `json:"labels"`
	TextPayload string            `json:"textPayload"`
	JSONPayload map[string]any    `json:"jsonPayload"`
}

// Normalize converts a log entry to an Event.
//
// SourceRef is gcplog:<insertId>, which is globally unique per entry. That
// makes (source, source_ref) deduplication useless against a crash loop —
// every entry genuinely is distinct — so fingerprint suppression is what stops
// an error storm becoming an invoice (SPEC §4.2.2). Extracting a body the
// fingerprinter can work with is therefore this adapter's most important job.
func (a *GCPLogAdapter) Normalize(_ context.Context, m Message) (Event, error) {
	var entry logEntry
	if err := json.Unmarshal(m.Data, &entry); err != nil {
		return Event{}, fmt.Errorf("%w: decoding log entry: %w", ErrMalformed, err)
	}
	if entry.InsertID == "" {
		return Event{}, fmt.Errorf("%w: log entry has no insertId, so it cannot be deduplicated", ErrMalformed)
	}

	severity := strings.ToUpper(entry.Severity)
	if !actionableSeverities[severity] {
		return Event{}, fmt.Errorf("%w: severity %q is below ERROR", ErrIgnore, entry.Severity)
	}

	title, body := entry.messageAndBody()
	if title == "" && body == "" {
		return Event{}, fmt.Errorf("%w: log entry has no payload", ErrIgnore)
	}

	slug, _ := a.resolver.SlugForLabels(mergeLabels(entry.Resource.Labels, entry.Labels))

	metadata := map[string]string{
		"severity":      severity,
		"log_name":      entry.LogName,
		"resource_type": entry.Resource.Type,
		"insert_id":     entry.InsertID,
	}
	for k, v := range entry.Resource.Labels {
		metadata["resource."+k] = v
	}

	return Event{
		Source:      "gcplog",
		Kind:        "log.error",
		SourceRef:   "gcplog:" + entry.InsertID,
		ProjectSlug: slug,
		Title:       title,
		Body:        body,
		OccurredAt:  entry.Timestamp,
		Metadata:    metadata,
	}, nil
}

// messageAndBody extracts a one-line title and the fullest available body.
// A structured stack_trace is preferred over the message, because the frames
// are what fingerprinting groups on.
func (e logEntry) messageAndBody() (string, string) {
	if e.TextPayload != "" {
		return firstLine(e.TextPayload), e.TextPayload
	}

	message := stringField(e.JSONPayload, "message")
	stack := stringField(e.JSONPayload, "stack_trace", "stack", "exception", "error")

	switch {
	case stack != "":
		title := message
		if title == "" {
			title = firstLine(stack)
		}
		return firstLine(title), stack
	case message != "":
		return firstLine(message), message
	}

	if len(e.JSONPayload) > 0 {
		if encoded, err := json.Marshal(e.JSONPayload); err == nil {
			return firstLine(string(encoded)), string(encoded)
		}
	}
	return "", ""
}

func stringField(payload map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := payload[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return strings.TrimSpace(s[:idx])
	}
	return strings.TrimSpace(s)
}

// mergeLabels combines resource and entry labels, with entry labels winning so
// an explicit project_slug label can override an inferred service name.
func mergeLabels(resource, entry map[string]string) map[string]string {
	merged := make(map[string]string, len(resource)+len(entry))
	for k, v := range resource {
		merged[k] = v
	}
	for k, v := range entry {
		merged[k] = v
	}
	return merged
}
