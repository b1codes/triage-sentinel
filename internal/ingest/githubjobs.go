package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// githubAPIBase is the REST API root. It is a variable so tests can point it at
// an httptest server.
var githubAPIBase = "https://api.github.com"

// httpJobFetcher reads failing job and step names from the Actions API.
type httpJobFetcher struct {
	token  string
	client *http.Client
}

// NewGitHubJobFetcher returns a JobFetcher backed by the Actions API. It needs
// only read scope; M4 is the first milestone requiring write. Passing nil for
// client uses a client with a bounded timeout.
func NewGitHubJobFetcher(token string, client *http.Client) JobFetcher {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &httpJobFetcher{token: token, client: client}
}

type jobsResponse struct {
	Jobs []struct {
		Name       string `json:"name"`
		Conclusion string `json:"conclusion"`
		Steps      []struct {
			Name       string `json:"name"`
			Conclusion string `json:"conclusion"`
		} `json:"steps"`
	} `json:"jobs"`
}

// FailedJobSteps returns the names of the first failing job and its first
// failing step, which is what the workflow fingerprint groups on.
func (f *httpJobFetcher) FailedJobSteps(ctx context.Context, repo string, runID int64) ([]string, error) {
	url := fmt.Sprintf("%s/repos/%s/actions/runs/%d/jobs",
		githubAPIBase, strings.TrimPrefix(repo, "github.com/"), runID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building jobs request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if f.token != "" {
		req.Header.Set("Authorization", "Bearer "+f.token)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching jobs for run %d: %w", runID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching jobs for run %d: status %s", runID, resp.Status)
	}

	var decoded jobsResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decoding jobs for run %d: %w", runID, err)
	}

	for _, job := range decoded.Jobs {
		if !strings.EqualFold(job.Conclusion, "failure") {
			continue
		}
		for _, step := range job.Steps {
			if strings.EqualFold(step.Conclusion, "failure") {
				return []string{job.Name, step.Name}, nil
			}
		}
		return []string{job.Name}, nil
	}
	return nil, fmt.Errorf("no failing job in run %s", strconv.FormatInt(runID, 10))
}
