package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/b1codes/triage-sentinel/internal/config"
)

// stubAdapter is a controllable adapter for routing tests. Real adapter
// behaviour is tested in the github and gcplog test files.
type stubAdapter struct {
	name     string
	matchKey string
	event    Event
	err      error
}

func (s stubAdapter) Name() string { return s.name }

func (s stubAdapter) Match(attrs map[string]string) bool {
	_, ok := attrs[s.matchKey]
	return ok
}

func (s stubAdapter) Normalize(context.Context, Message) (Event, error) {
	if s.err != nil {
		return Event{}, s.err
	}
	return s.event, nil
}

func TestRouterSelectsTheMatchingAdapter(t *testing.T) {
	router := NewRouter(
		stubAdapter{name: "github", matchKey: "x-github-event", event: Event{Source: "github"}},
		stubAdapter{name: "gcplog", matchKey: "logging.googleapis.com/timestamp", event: Event{Source: "gcplog"}},
	)

	tests := []struct {
		name       string
		attrs      map[string]string
		wantSource string
		wantErr    error
	}{
		{
			name:       "github attributes route to github",
			attrs:      map[string]string{"x-github-event": "workflow_run"},
			wantSource: "github",
		},
		{
			name:       "logging attributes route to gcplog",
			attrs:      map[string]string{"logging.googleapis.com/timestamp": "2026-08-02T00:00:00Z"},
			wantSource: "gcplog",
		},
		{
			name:    "unclaimed message reports ErrNoAdapter",
			attrs:   map[string]string{"unrelated": "1"},
			wantErr: ErrNoAdapter,
		},
		{
			name:    "no attributes at all reports ErrNoAdapter",
			attrs:   nil,
			wantErr: ErrNoAdapter,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := router.Route(context.Background(), Message{Attributes: tc.attrs})
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Route() error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Route() error = %v, want nil", err)
			}
			if ev.Source != tc.wantSource {
				t.Errorf("Source = %q, want %q", ev.Source, tc.wantSource)
			}
		})
	}
}

func TestRouterPropagatesAdapterErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "ignore propagates unchanged", err: ErrIgnore},
		{name: "malformed propagates unchanged", err: ErrMalformed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := NewRouter(stubAdapter{name: "s", matchKey: "k", err: tc.err})
			_, err := router.Route(context.Background(), Message{Attributes: map[string]string{"k": "v"}})
			if !errors.Is(err, tc.err) {
				t.Errorf("Route() error = %v, want %v", err, tc.err)
			}
		})
	}
}

func TestRouterUsesFirstMatchingAdapter(t *testing.T) {
	router := NewRouter(
		stubAdapter{name: "first", matchKey: "k", event: Event{Source: "first"}},
		stubAdapter{name: "second", matchKey: "k", event: Event{Source: "second"}},
	)
	ev, err := router.Route(context.Background(), Message{Attributes: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	if ev.Source != "first" {
		t.Errorf("Source = %q, want %q; registration order must decide", ev.Source, "first")
	}
}

func registryFixture(t *testing.T) config.Registry {
	t.Helper()
	return config.Registry{
		Projects: []config.Project{
			{Slug: "example-api", Repo: "github.com/example/example-api", DefaultBranch: "main"},
			{Slug: "example-worker", Repo: "github.com/example/example-worker", DefaultBranch: "main"},
		},
	}
}

func TestRegistryResolverSlugForRepo(t *testing.T) {
	r := NewRegistryResolver(registryFixture(t))

	tests := []struct {
		name     string
		repo     string
		wantSlug string
		wantOK   bool
	}{
		{name: "owner/name form", repo: "example/example-api", wantSlug: "example-api", wantOK: true},
		{name: "full host form", repo: "github.com/example/example-api", wantSlug: "example-api", wantOK: true},
		{name: "case-insensitive", repo: "Example/Example-API", wantSlug: "example-api", wantOK: true},
		{name: "unknown repo", repo: "example/not-registered", wantOK: false},
		{name: "empty", repo: "", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slug, ok := r.SlugForRepo(tc.repo)
			if ok != tc.wantOK {
				t.Fatalf("SlugForRepo(%q) ok = %v, want %v", tc.repo, ok, tc.wantOK)
			}
			if ok && slug != tc.wantSlug {
				t.Errorf("slug = %q, want %q", slug, tc.wantSlug)
			}
		})
	}
}

func TestRegistryResolverSlugForLabels(t *testing.T) {
	r := NewRegistryResolver(registryFixture(t))

	tests := []struct {
		name     string
		labels   map[string]string
		wantSlug string
		wantOK   bool
	}{
		{
			name:     "service_name matches a slug",
			labels:   map[string]string{"service_name": "example-api"},
			wantSlug: "example-api", wantOK: true,
		},
		{
			name:     "function_name matches a slug",
			labels:   map[string]string{"function_name": "example-worker"},
			wantSlug: "example-worker", wantOK: true,
		},
		{
			name:     "an explicit project_slug label wins",
			labels:   map[string]string{"project_slug": "example-api", "service_name": "example-worker"},
			wantSlug: "example-api", wantOK: true,
		},
		{name: "no label matches", labels: map[string]string{"service_name": "unknown"}, wantOK: false},
		{name: "nil labels", labels: nil, wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slug, ok := r.SlugForLabels(tc.labels)
			if ok != tc.wantOK {
				t.Fatalf("SlugForLabels(%v) ok = %v, want %v", tc.labels, ok, tc.wantOK)
			}
			if ok && slug != tc.wantSlug {
				t.Errorf("slug = %q, want %q", slug, tc.wantSlug)
			}
		})
	}
}

func TestEventZeroValueIsUsable(t *testing.T) {
	var ev Event
	if ev.OccurredAt != (time.Time{}) {
		t.Error("zero Event has a non-zero OccurredAt")
	}
}
