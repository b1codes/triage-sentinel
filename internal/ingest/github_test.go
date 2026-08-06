package ingest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testSecret = "it-is-a-secret-to-everybody"

func readPayload(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading testdata/%s: %v", name, err)
	}
	return data
}

func sign(t *testing.T, secret string, body []byte) string {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func githubMessage(t *testing.T, event, file string) Message {
	t.Helper()
	body := readPayload(t, file)
	return Message{
		ID:   "msg-1",
		Data: body,
		Attributes: map[string]string{
			"x-github-event":      event,
			"x-github-delivery":   "delivery-1",
			"x-hub-signature-256": sign(t, testSecret, body),
		},
	}
}

type fakeJobs struct {
	steps []string
	err   error
}

func (f fakeJobs) FailedJobSteps(context.Context, string, int64) ([]string, error) {
	return f.steps, f.err
}

func testGitHubAdapter(t *testing.T, jobs JobFetcher) *GitHubAdapter {
	t.Helper()
	return NewGitHubAdapter(GitHubOptions{
		Secret:   testSecret,
		Resolver: NewRegistryResolver(registryFixture(t)),
		Jobs:     jobs,
	})
}

func TestGitHubAdapterMatch(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{})

	tests := []struct {
		name  string
		attrs map[string]string
		want  bool
	}{
		{name: "github event header claims it", attrs: map[string]string{"x-github-event": "workflow_run"}, want: true},
		{name: "case-insensitive header", attrs: map[string]string{"X-GitHub-Event": "issues"}, want: true},
		{name: "logging attributes are not ours", attrs: map[string]string{"logging.googleapis.com/timestamp": "x"}, want: false},
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

func TestGitHubAdapterVerifiesSignature(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{steps: []string{"test", "Run unit tests"}})
	body := readPayload(t, "github_workflow_run_failure.json")

	tests := []struct {
		name      string
		signature string
	}{
		{name: "wrong secret", signature: sign(t, "wrong-secret", body)},
		{name: "missing signature", signature: ""},
		{name: "truncated signature", signature: "sha256=abcd"},
		{name: "no algorithm prefix", signature: hex.EncodeToString([]byte("x"))},
		{name: "signature of a different body", signature: sign(t, testSecret, []byte("{}"))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := Message{
				Data: body,
				Attributes: map[string]string{
					"x-github-event":      "workflow_run",
					"x-hub-signature-256": tc.signature,
				},
			}
			_, err := a.Normalize(context.Background(), m)
			if !errors.Is(err, ErrSignature) {
				t.Fatalf("Normalize() error = %v, want ErrSignature; a compromised relay must not be able to inject events", err)
			}
		})
	}
}

func TestGitHubAdapterNormalizesWorkflowRunFailure(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{steps: []string{"test", "Run unit tests"}})

	ev, err := a.Normalize(context.Background(), githubMessage(t, "workflow_run", "github_workflow_run_failure.json"))
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil", err)
	}

	checks := []struct {
		name string
		got  string
		want string
	}{
		{name: "source", got: ev.Source, want: "github"},
		{name: "kind", got: ev.Kind, want: "workflow_run.failed"},
		{name: "source ref", got: ev.SourceRef, want: "workflow_run:1234567890"},
		{name: "project slug", got: ev.ProjectSlug, want: "example-api"},
		{name: "workflow", got: ev.Workflow, want: "CI"},
		{name: "author email", got: ev.AuthorEmail, want: "person@example.com"},
	}
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("got %q, want %q", c.got, c.want)
			}
		})
	}

	t.Run("failed job steps are captured for fingerprinting", func(t *testing.T) {
		if len(ev.JobSteps) != 2 {
			t.Fatalf("JobSteps = %v, want two entries; fingerprinting on workflow name alone would collapse every ci.yml failure", ev.JobSteps)
		}
	})

	t.Run("occurred at is parsed", func(t *testing.T) {
		if ev.OccurredAt.IsZero() {
			t.Error("OccurredAt is zero")
		}
	})

	t.Run("metadata carries the run url", func(t *testing.T) {
		if ev.Metadata["html_url"] == "" {
			t.Error("Metadata[html_url] is empty; the dashboard links to it")
		}
	})
}

func TestGitHubAdapterIgnoresUninteresting(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{})

	tests := []struct {
		name  string
		event string
		file  string
	}{
		{name: "successful workflow run", event: "workflow_run", file: "github_workflow_run_success.json"},
		{name: "unhandled event type", event: "star", file: "github_issues_opened.json"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.Normalize(context.Background(), githubMessage(t, tc.event, tc.file))
			if !errors.Is(err, ErrIgnore) {
				t.Errorf("Normalize() error = %v, want ErrIgnore", err)
			}
		})
	}
}

func TestGitHubAdapterNormalizesIssue(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{})

	ev, err := a.Normalize(context.Background(), githubMessage(t, "issues", "github_issues_opened.json"))
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil", err)
	}
	if ev.Kind != "issues.opened" {
		t.Errorf("Kind = %q, want %q", ev.Kind, "issues.opened")
	}
	if ev.SourceRef != "issue:example/example-api#17" {
		t.Errorf("SourceRef = %q, want %q", ev.SourceRef, "issue:example/example-api#17")
	}
	if ev.AuthorEmail != "" {
		t.Errorf("AuthorEmail = %q, want empty; an issue has no commit author", ev.AuthorEmail)
	}
}

func TestGitHubAdapterRejectsMalformedBody(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{})
	body := []byte(`{"action": "completed", "workflow_run":`)

	_, err := a.Normalize(context.Background(), Message{
		Data: body,
		Attributes: map[string]string{
			"x-github-event":      "workflow_run",
			"x-hub-signature-256": sign(t, testSecret, body),
		},
	})
	if !errors.Is(err, ErrMalformed) {
		t.Errorf("Normalize() error = %v, want ErrMalformed", err)
	}
}

func TestGitHubAdapterUnroutableRepoKeepsEmptySlug(t *testing.T) {
	a := testGitHubAdapter(t, fakeJobs{steps: []string{"test"}})
	// Repoint the payload at a repository that is not registered.
	body := []byte(strings.ReplaceAll(
		string(readPayload(t, "github_workflow_run_failure.json")),
		"example/example-api", "example/not-registered"))

	ev, err := a.Normalize(context.Background(), Message{
		Data: body,
		Attributes: map[string]string{
			"x-github-event":      "workflow_run",
			"x-hub-signature-256": sign(t, testSecret, body),
		},
	})
	if err != nil {
		t.Fatalf("Normalize() error = %v, want nil; an unroutable event must normalise so it can be recorded", err)
	}
	if ev.ProjectSlug != "" {
		t.Errorf("ProjectSlug = %q, want empty for an unregistered repository", ev.ProjectSlug)
	}
}

func TestHTTPJobFetcher(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer tok")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jobs":[
			{"name":"lint","conclusion":"success","steps":[]},
			{"name":"test","conclusion":"failure","steps":[
				{"name":"Checkout","conclusion":"success"},
				{"name":"Run unit tests","conclusion":"failure"}
			]}
		]}`))
	}))
	defer srv.Close()

	original := githubAPIBase
	githubAPIBase = srv.URL
	t.Cleanup(func() { githubAPIBase = original })

	steps, err := NewGitHubJobFetcher("tok", srv.Client()).
		FailedJobSteps(context.Background(), "example/example-api", 1234567890)
	if err != nil {
		t.Fatalf("FailedJobSteps() error = %v, want nil", err)
	}
	if len(steps) != 2 || steps[0] != "test" || steps[1] != "Run unit tests" {
		t.Errorf("steps = %v, want [test, Run unit tests]", steps)
	}
}
