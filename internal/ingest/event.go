// Package ingest pulls events from Pub/Sub, normalises them through per-source
// adapters, and routes them to a registered project.
package ingest

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors an adapter or the router may return. They are distinct
// because the subscriber handles each differently, and conflating them would
// either lose events or spam the log (design §9):
//
//   - ErrIgnore    structurally valid but uninteresting. Ack, count, do not persist.
//   - ErrNoAdapter no adapter claimed the message. Ack, count, log at warn.
//   - ErrMalformed an adapter claimed it and could not parse it. Ack and persist
//     as filtered/unparseable, so it stays visible rather than silently gone.
var (
	ErrIgnore    = errors.New("event is uninteresting")
	ErrNoAdapter = errors.New("no adapter claimed the message")
	ErrMalformed = errors.New("event payload is malformed")
)

// Message is one Pub/Sub message, independent of the transport that fetched it.
// Keeping it transport-neutral is what lets the REST puller, a fake, and any
// future client share every adapter.
type Message struct {
	ID          string
	AckID       string
	Data        []byte
	Attributes  map[string]string
	PublishTime time.Time
}

// Event is a normalised event ready to become an incident (SPEC §4.2).
type Event struct {
	Source    string // "github" | "gcplog"
	Kind      string // "workflow_run.failed" | "issues.opened" | "log.error"
	SourceRef string // stable per-source identity; unique with Source

	// ProjectSlug is empty when the event is unroutable, which is recorded
	// rather than dropped — it usually means a stale projects.yaml.
	ProjectSlug string

	Title string
	Body  string

	// AuthorEmail carries commit attribution when the source has it, so the
	// Tier 0 SelfInflicted filter can close the self-repair loop.
	AuthorEmail string

	Metadata   map[string]string
	OccurredAt time.Time

	// Workflow and JobSteps are populated for CI failures, which have no stack
	// trace and are fingerprinted by failing job and step instead
	// (design §4.4.2).
	Workflow string
	JobSteps []string
}

// Adapter normalises one source's messages.
type Adapter interface {
	// Name identifies the adapter in logs and metrics.
	Name() string

	// Match reports whether this adapter owns the message, judged from
	// attributes alone so the body need not be parsed twice.
	Match(attrs map[string]string) bool

	// Normalize converts a raw message into a canonical event. It returns
	// ErrIgnore for messages that are structurally valid but uninteresting,
	// and ErrMalformed for ones it claimed but cannot parse.
	Normalize(ctx context.Context, m Message) (Event, error)
}

// Resolver maps source-specific identity onto a registered project slug.
type Resolver interface {
	// SlugForRepo accepts either "owner/name" or "github.com/owner/name".
	SlugForRepo(repo string) (string, bool)

	// SlugForLabels resolves a Cloud Logging resource's labels.
	SlugForLabels(labels map[string]string) (string, bool)
}
