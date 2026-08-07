package ingest

import (
	"context"
	"fmt"
	"strings"

	"github.com/b1codes/triage-sentinel/internal/config"
)

// Router dispatches a message to the first adapter that claims it.
type Router struct {
	adapters []Adapter
}

// NewRouter builds a router. Registration order is significant: the first
// adapter whose Match returns true wins, so a broad adapter must be registered
// after a narrow one.
func NewRouter(adapters ...Adapter) *Router {
	return &Router{adapters: adapters}
}

// Route normalises a message through its owning adapter. It returns
// ErrNoAdapter when nothing claims the message, and propagates the adapter's
// own error otherwise.
func (r *Router) Route(ctx context.Context, m Message) (Event, error) {
	for _, a := range r.adapters {
		if !a.Match(m.Attributes) {
			continue
		}
		ev, err := a.Normalize(ctx, m)
		if err != nil {
			return Event{}, fmt.Errorf("adapter %s: %w", a.Name(), err)
		}
		return ev, nil
	}
	return Event{}, fmt.Errorf("%w: message %s", ErrNoAdapter, m.ID)
}

// RegistryResolver resolves project slugs from the loaded registry.
type RegistryResolver struct {
	byRepo map[string]string
	bySlug map[string]string
}

// labelKeys are the Cloud Logging resource labels consulted, in priority order.
// project_slug is first so an operator can pin routing explicitly when a
// service name and a slug diverge.
var labelKeys = []string{"project_slug", "service_name", "function_name", "job", "namespace_name"}

// NewRegistryResolver indexes a registry for lookup. Repository keys are
// lowercased and reduced to "owner/name", so github.com/Owner/Repo and
// owner/repo resolve identically.
func NewRegistryResolver(reg config.Registry) *RegistryResolver {
	r := &RegistryResolver{
		byRepo: make(map[string]string, len(reg.Projects)),
		bySlug: make(map[string]string, len(reg.Projects)),
	}
	for _, p := range reg.Projects {
		r.byRepo[normalizeRepo(p.Repo)] = p.Slug
		r.bySlug[strings.ToLower(p.Slug)] = p.Slug
	}
	return r
}

// SlugForRepo resolves a repository reference to a slug.
func (r *RegistryResolver) SlugForRepo(repo string) (string, bool) {
	if repo == "" {
		return "", false
	}
	slug, ok := r.byRepo[normalizeRepo(repo)]
	return slug, ok
}

// SlugForLabels resolves a Cloud Logging resource's labels to a slug by
// matching known label values against registered slugs.
//
// A logging sink cannot attach arbitrary attributes to the messages it
// publishes, so routing must be inferred from the log entry itself. When no
// label matches, the event is recorded as unroutable and shown in the
// dashboard rather than dropped — which is the signal to fix either the sink
// or the naming (SPEC §4.2).
func (r *RegistryResolver) SlugForLabels(labels map[string]string) (string, bool) {
	for _, key := range labelKeys {
		value, present := labels[key]
		if !present || value == "" {
			continue
		}
		if slug, ok := r.bySlug[strings.ToLower(value)]; ok {
			return slug, true
		}
	}
	return "", false
}

// normalizeRepo reduces a repository reference to a lowercase "owner/name".
func normalizeRepo(repo string) string {
	trimmed := strings.ToLower(strings.TrimSpace(repo))
	trimmed = strings.TrimSuffix(trimmed, ".git")
	trimmed = strings.TrimPrefix(trimmed, "https://")
	trimmed = strings.TrimPrefix(trimmed, "github.com/")

	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 {
		return trimmed
	}
	return parts[len(parts)-2] + "/" + parts[len(parts)-1]
}
