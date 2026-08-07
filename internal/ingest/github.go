package ingest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrSignature is returned when a forwarded webhook signature does not verify.
var ErrSignature = errors.New("webhook signature verification failed")

// JobFetcher resolves the failing job and step names for a workflow run.
//
// This exists because the workflow_run webhook payload describes the run, not
// its jobs: it carries id, name, head_branch, head_sha, status, conclusion and
// timestamps, with no jobs array and no step names (verified against current
// GitHub REST documentation). Job-level detail requires a separate call to
// /repos/{owner}/{repo}/actions/runs/{run_id}/jobs, which is why GITHUB_TOKEN
// is a prerequisite from M1 rather than M4.
//
// Putting discovery behind an interface keeps the adapter testable without the
// network, and keeps fingerprinting off the workflow name — which would
// collapse every ci.yml failure into a single incident (design §4.4.2).
type JobFetcher interface {
	FailedJobSteps(ctx context.Context, repo string, runID int64) ([]string, error)
}

// GitHubOptions configures the adapter.
type GitHubOptions struct {
	// Secret is GITHUB_WEBHOOK_SECRET. The Cloud Run relay already verified
	// the signature; re-verifying here means a compromised relay cannot inject
	// events (SPEC §4.2.1).
	Secret   string
	Resolver Resolver
	Jobs     JobFetcher
}

// GitHubAdapter normalises GitHub webhook deliveries forwarded by the relay.
type GitHubAdapter struct {
	secret   []byte
	resolver Resolver
	jobs     JobFetcher
}

// NewGitHubAdapter builds the adapter.
func NewGitHubAdapter(opts GitHubOptions) *GitHubAdapter {
	return &GitHubAdapter{
		secret:   []byte(opts.Secret),
		resolver: opts.Resolver,
		jobs:     opts.Jobs,
	}
}

// Name identifies the adapter.
func (a *GitHubAdapter) Name() string { return "github" }

// Match claims any message carrying a GitHub event header.
func (a *GitHubAdapter) Match(attrs map[string]string) bool {
	_, ok := attrValue(attrs, "x-github-event")
	return ok
}

// attrValue reads an attribute case-insensitively. Pub/Sub preserves attribute
// case, and GitHub's own header casing has changed over time, so matching
// exactly would be brittle.
func attrValue(attrs map[string]string, key string) (string, bool) {
	for k, v := range attrs {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

type githubRepository struct {
	FullName string `json:"full_name"`
}

type githubCommitAuthor struct {
	Email string `json:"email"`
}

type githubHeadCommit struct {
	ID      string             `json:"id"`
	Message string             `json:"message"`
	Author  githubCommitAuthor `json:"author"`
}

type githubWorkflowRun struct {
	ID         int64            `json:"id"`
	Name       string           `json:"name"`
	Path       string           `json:"path"`
	HeadBranch string           `json:"head_branch"`
	Status     string           `json:"status"`
	Conclusion string           `json:"conclusion"`
	HTMLURL    string           `json:"html_url"`
	UpdatedAt  time.Time        `json:"updated_at"`
	HeadCommit githubHeadCommit `json:"head_commit"`
}

type githubIssue struct {
	Number    int       `json:"number"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	HTMLURL   string    `json:"html_url"`
	CreatedAt time.Time `json:"created_at"`
	Labels    []struct {
		Name string `json:"name"`
	} `json:"labels"`
}

type githubPayload struct {
	Action      string             `json:"action"`
	WorkflowRun *githubWorkflowRun `json:"workflow_run"`
	Issue       *githubIssue       `json:"issue"`
	Repository  githubRepository   `json:"repository"`
}

// Normalize verifies the signature, then converts the delivery to an Event.
func (a *GitHubAdapter) Normalize(ctx context.Context, m Message) (Event, error) {
	if err := a.verify(m); err != nil {
		return Event{}, err
	}

	eventType, _ := attrValue(m.Attributes, "x-github-event")

	var payload githubPayload
	if err := json.Unmarshal(m.Data, &payload); err != nil {
		return Event{}, fmt.Errorf("%w: decoding %s delivery: %w", ErrMalformed, eventType, err)
	}

	switch strings.ToLower(eventType) {
	case "workflow_run":
		return a.normalizeWorkflowRun(ctx, payload)
	case "issues":
		return a.normalizeIssue(payload)
	default:
		return Event{}, fmt.Errorf("%w: github event %q is not subscribed", ErrIgnore, eventType)
	}
}

// verify re-checks the HMAC the relay already verified. hmac.Equal is required:
// comparing hex strings with == leaks timing information.
func (a *GitHubAdapter) verify(m Message) error {
	signature, ok := attrValue(m.Attributes, "x-hub-signature-256")
	if !ok || signature == "" {
		return fmt.Errorf("%w: no x-hub-signature-256 attribute", ErrSignature)
	}

	encoded, found := strings.CutPrefix(signature, "sha256=")
	if !found {
		return fmt.Errorf("%w: signature %q has no sha256= prefix", ErrSignature, signature)
	}
	provided, err := hex.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("%w: signature is not hex: %w", ErrSignature, err)
	}

	mac := hmac.New(sha256.New, a.secret)
	mac.Write(m.Data)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return fmt.Errorf("%w: computed digest does not match", ErrSignature)
	}
	return nil
}

func (a *GitHubAdapter) normalizeWorkflowRun(ctx context.Context, p githubPayload) (Event, error) {
	run := p.WorkflowRun
	if run == nil {
		return Event{}, fmt.Errorf("%w: workflow_run delivery has no workflow_run object", ErrMalformed)
	}
	if !strings.EqualFold(run.Conclusion, "failure") {
		return Event{}, fmt.Errorf("%w: workflow run concluded %q", ErrIgnore, run.Conclusion)
	}

	slug, _ := a.resolver.SlugForRepo(p.Repository.FullName)

	steps, err := a.failedSteps(ctx, p.Repository.FullName, run.ID)
	if err != nil {
		// Job discovery is best-effort: a failure here must not lose the
		// event. Fingerprinting degrades to the run's identity, which is
		// narrower than the workflow name and therefore safe (design §4.4).
		steps = []string{"run:" + strconv.FormatInt(run.ID, 10)}
	}

	return Event{
		Source:      "github",
		Kind:        "workflow_run.failed",
		SourceRef:   "workflow_run:" + strconv.FormatInt(run.ID, 10),
		ProjectSlug: slug,
		Title:       fmt.Sprintf("%s failed on %s", run.Name, run.HeadBranch),
		Body:        run.HeadCommit.Message,
		AuthorEmail: run.HeadCommit.Author.Email,
		Workflow:    run.Name,
		JobSteps:    steps,
		OccurredAt:  run.UpdatedAt,
		Metadata: map[string]string{
			"repository":  p.Repository.FullName,
			"html_url":    run.HTMLURL,
			"head_sha":    run.HeadCommit.ID,
			"head_branch": run.HeadBranch,
			"workflow":    run.Name,
			"path":        run.Path,
		},
	}, nil
}

func (a *GitHubAdapter) failedSteps(ctx context.Context, repo string, runID int64) ([]string, error) {
	if a.jobs == nil {
		return nil, errors.New("no job fetcher configured")
	}
	steps, err := a.jobs.FailedJobSteps(ctx, repo, runID)
	if err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		return nil, errors.New("no failing job reported")
	}
	return steps, nil
}

func (a *GitHubAdapter) normalizeIssue(p githubPayload) (Event, error) {
	issue := p.Issue
	if issue == nil {
		return Event{}, fmt.Errorf("%w: issues delivery has no issue object", ErrMalformed)
	}
	if p.Action != "opened" && p.Action != "labeled" {
		return Event{}, fmt.Errorf("%w: issue action %q is not subscribed", ErrIgnore, p.Action)
	}

	slug, _ := a.resolver.SlugForRepo(p.Repository.FullName)

	labels := make([]string, 0, len(issue.Labels))
	for _, l := range issue.Labels {
		labels = append(labels, l.Name)
	}

	return Event{
		Source:      "github",
		Kind:        "issues." + p.Action,
		SourceRef:   fmt.Sprintf("issue:%s#%d", p.Repository.FullName, issue.Number),
		ProjectSlug: slug,
		Title:       issue.Title,
		Body:        issue.Body,
		OccurredAt:  issue.CreatedAt,
		Metadata: map[string]string{
			"repository": p.Repository.FullName,
			"html_url":   issue.HTMLURL,
			"labels":     strings.Join(labels, ","),
			"number":     strconv.Itoa(issue.Number),
		},
	}, nil
}
