package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/httpapi"
)

func TestRunVersion(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"version"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatalf("run() error = %v, want nil", err)
	}
	if strings.TrimSpace(out.String()) == "" {
		t.Error("version subcommand printed nothing")
	}
}

func TestRunUnknownSubcommand(t *testing.T) {
	err := run(context.Background(), []string{"frobnicate"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("run() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "frobnicate") {
		t.Errorf("error %q does not name the unknown subcommand", err.Error())
	}
}

func TestRunHelpListsEverySubcommand(t *testing.T) {
	var out bytes.Buffer
	_ = run(context.Background(), []string{"-h"}, &out, &out)

	for _, want := range []string{"serve", "migrate", "validate", "hash-password", "version"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("help output does not mention %q:\n%s", want, out.String())
		}
	}
}

// envFileFor writes a minimal valid .env pointing at a temp data dir.
func envFileFor(t *testing.T, dataDir string) string {
	t.Helper()

	// Format-valid bcrypt hash; no login happens in these tests.
	const hash = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

	path := filepath.Join(t.TempDir(), ".env")
	body := "DASHBOARD_PASSWORD_HASH=" + hash + "\n" +
		"SENTINEL_DATA_DIR=" + dataDir + "\n" +
		"SENTINEL_LISTEN_ADDR=127.0.0.1:0\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing env file: %v", err)
	}
	return path
}

func TestRunMigrateCreatesDatabase(t *testing.T) {
	dataDir := t.TempDir()
	envFile := envFileFor(t, dataDir)

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"migrate",
		"-env-file", envFile,
		"-config", "../../projects.example.yaml",
	}, &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("run() error = %v, want nil", err)
	}

	if _, statErr := os.Stat(filepath.Join(dataDir, "sentinel.db")); statErr != nil {
		t.Errorf("database not created: %v", statErr)
	}
	if !strings.Contains(out.String(), "schema") {
		t.Errorf("migrate printed no schema summary:\n%s", out.String())
	}
}

func TestRunMigrateIsIdempotent(t *testing.T) {
	dataDir := t.TempDir()
	envFile := envFileFor(t, dataDir)
	args := []string{"migrate", "-env-file", envFile, "-config", "../../projects.example.yaml"}

	for i := 0; i < 2; i++ {
		if err := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
			t.Fatalf("run() attempt %d error = %v, want nil", i+1, err)
		}
	}
}

func TestRunValidateAcceptsExampleRegistry(t *testing.T) {
	envFile := envFileFor(t, t.TempDir())

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"validate",
		"-env-file", envFile,
		"-config", "../../projects.example.yaml",
	}, &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("run() error = %v, want nil", err)
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("validate did not report success:\n%s", out.String())
	}
}

func TestRunValidateRejectsBadRegistry(t *testing.T) {
	envFile := envFileFor(t, t.TempDir())

	bad := filepath.Join(t.TempDir(), "projects.yaml")
	if err := os.WriteFile(bad, []byte("version: 99\n"), 0o600); err != nil {
		t.Fatalf("writing registry: %v", err)
	}

	err := run(context.Background(), []string{
		"validate", "-env-file", envFile, "-config", bad,
	}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("run() error = nil, want error for an invalid registry")
	}
}

func TestRunValidateRejectsMissingEnvFile(t *testing.T) {
	err := run(context.Background(), []string{
		"validate",
		"-env-file", filepath.Join(t.TempDir(), "absent"),
		"-config", "../../projects.example.yaml",
	}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("run() error = nil, want error for a missing env file")
	}
}

func TestRunServeShutsDownOnContextCancel(t *testing.T) {
	dataDir := t.TempDir()
	envFile := envFileFor(t, dataDir)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"serve", "--no-ingest", "-env-file", envFile, "-config", "../../projects.example.yaml",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	// Listening on port 0 means the server is up as soon as the database exists.
	waitForFile(t, filepath.Join(dataDir, "sentinel.db"))
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("serve returned %v on shutdown, want nil", err)
		}
	case <-timeoutAfter(t):
		t.Fatal("serve did not return within the shutdown deadline")
	}
}

// syncBuffer is an io.Writer safe for one goroutine to write to (serve's
// listening-address log line) while the test goroutine polls its contents.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// envFileForWithPassword is envFileFor but with a real bcrypt hash of a known
// password, so a test can actually log in over HTTP rather than only
// exercising format validation.
func envFileForWithPassword(t *testing.T, dataDir, password string) string {
	t.Helper()

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt.GenerateFromPassword() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), ".env")
	body := "DASHBOARD_PASSWORD_HASH=" + string(hash) + "\n" +
		"SENTINEL_DATA_DIR=" + dataDir + "\n" +
		"SENTINEL_LISTEN_ADDR=127.0.0.1:0\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing env file: %v", err)
	}
	return path
}

var listenAddrPattern = regexp.MustCompile(`sentinel listening on http://(\S+)`)

// waitForListenAddr polls out for serve's "sentinel listening on ..." line and
// returns the address it bound (port 0 resolves to a random port, so the test
// cannot know it up front).
func waitForListenAddr(t *testing.T, out *syncBuffer) string {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m := listenAddrPattern.FindStringSubmatch(out.String()); m != nil {
			return m[1]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("serve never printed a listening address:\n%s", out.String())
	return ""
}

// TestRunServeShutsDownPromptlyWithOpenSSEConnection guards against a
// shutdown-ordering regression: http.Server.Shutdown does not cancel
// in-flight request contexts, it only waits for handlers to return on their
// own. handleStream's loop is parked in a select on client.Events(), which
// only unblocks when the bus hub closes. If hub.Close() ran after
// Shutdown(shutdownCtx) returned (as it did in an earlier version of serve),
// an open dashboard SSE connection would make every SIGTERM/SIGINT hang for
// the full 15s shutdownTimeout instead of exiting promptly — the normal
// operating mode, since a dashboard tab is usually open. This test opens a
// real SSE connection through the full HTTP stack and asserts shutdown
// completes in well under 15s.
func TestRunServeShutsDownPromptlyWithOpenSSEConnection(t *testing.T) {
	dataDir := t.TempDir()
	const password = "sse-shutdown-test-password"
	envFile := envFileForWithPassword(t, dataDir, password)

	ctx, cancel := context.WithCancel(context.Background())

	var stdout syncBuffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"serve", "--no-ingest", "-env-file", envFile, "-config", "../../projects.example.yaml",
		}, &stdout, &bytes.Buffer{})
	}()

	addr := waitForListenAddr(t, &stdout)
	base := "http://" + addr

	client := &http.Client{}

	loginResp, err := client.Post(base+"/api/login", "application/json",
		strings.NewReader(`{"password":"`+password+`"}`))
	if err != nil {
		t.Fatalf("login request: %v", err)
	}
	_ = loginResp.Body.Close()
	if loginResp.StatusCode != http.StatusNoContent {
		t.Fatalf("login status = %d, want %d", loginResp.StatusCode, http.StatusNoContent)
	}

	var session *http.Cookie
	for _, c := range loginResp.Cookies() {
		if c.Name == httpapi.SessionCookieName {
			session = c
		}
	}
	if session == nil {
		t.Fatal("login response carried no session cookie")
	}

	streamReq, err := http.NewRequest(http.MethodGet, base+"/api/stream", nil)
	if err != nil {
		t.Fatalf("building stream request: %v", err)
	}
	streamReq.AddCookie(session)

	streamResp, err := client.Do(streamReq)
	if err != nil {
		t.Fatalf("opening SSE stream: %v", err)
	}
	defer func() { _ = streamResp.Body.Close() }()
	if streamResp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d, want %d", streamResp.StatusCode, http.StatusOK)
	}

	// handleStream flushes headers synchronously before entering its blocking
	// select, and client.Do above already returned after headers arrived, so
	// the handler goroutine is now parked in the select on client.Events().
	// A short grace period absorbs any remaining scheduling slack.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("serve returned %v on shutdown with an open SSE connection, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not shut down within 5s with an open SSE connection " +
			"(want well under the 15s shutdownTimeout; hub.Close() may be running after Shutdown() again)")
	}

	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("shutdown with an open SSE connection took %v, want well under the 15s shutdownTimeout", elapsed)
	}
}

func TestRunHashPasswordProducesAVerifiableHash(t *testing.T) {
	var out bytes.Buffer
	err := runWithStdin(context.Background(),
		[]string{"hash-password"}, strings.NewReader("my-password\n"), &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("run() error = %v, want nil", err)
	}

	hash := strings.TrimSpace(out.String())
	if !strings.HasPrefix(hash, "$2") {
		t.Fatalf("output %q is not a bcrypt hash", hash)
	}
	if err := compareHash(hash, "my-password"); err != nil {
		t.Errorf("generated hash does not verify against the input password: %v", err)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never appeared", path)
}

func timeoutAfter(t *testing.T) <-chan time.Time {
	t.Helper()
	return time.After(20 * time.Second)
}

func compareHash(hash, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

func TestServeRequiresIngestSecrets(t *testing.T) {
	tests := []struct {
		name    string
		missing string
	}{
		{name: "no gcp project", missing: "GCP_PROJECT_ID"},
		{name: "no subscription", missing: "PUBSUB_SUBSCRIPTION"},
		{name: "no webhook secret", missing: "GITHUB_WEBHOOK_SECRET"},
		{name: "no github token", missing: "GITHUB_TOKEN"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := completeIngestEnv()
			delete(env, tc.missing)

			err := assertIngestEnv(envFrom(env))
			if err == nil {
				t.Fatalf("assertIngestEnv() error = nil, want an error naming %s", tc.missing)
			}
			if !strings.Contains(err.Error(), tc.missing) {
				t.Errorf("error %q does not name %s", err.Error(), tc.missing)
			}
		})
	}
}

func TestAssertIngestEnvAcceptsCompleteEnvironment(t *testing.T) {
	if err := assertIngestEnv(envFrom(completeIngestEnv())); err != nil {
		t.Errorf("assertIngestEnv() error = %v, want nil", err)
	}
}

func TestAssertIngestEnvReportsEveryProblemAtOnce(t *testing.T) {
	err := assertIngestEnv(config.Env{})
	if err == nil {
		t.Fatal("assertIngestEnv() error = nil, want an error")
	}
	for _, want := range []string{
		"GCP_PROJECT_ID", "PUBSUB_SUBSCRIPTION", "GITHUB_WEBHOOK_SECRET", "GITHUB_TOKEN",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s; all problems must be reported at once", err.Error(), want)
		}
	}
}

func completeIngestEnv() map[string]string {
	return map[string]string{
		"GCP_PROJECT_ID":        "example-project",
		"PUBSUB_SUBSCRIPTION":   "projects/example-project/subscriptions/sentinel",
		"GITHUB_WEBHOOK_SECRET": "shhh",
		"GITHUB_TOKEN":          "ghp_test",
	}
}

func envFrom(m map[string]string) config.Env {
	return config.Env{
		GCPProjectID:        m["GCP_PROJECT_ID"],
		PubSubSubscription:  m["PUBSUB_SUBSCRIPTION"],
		GitHubWebhookSecret: m["GITHUB_WEBHOOK_SECRET"],
		GitHubToken:         m["GITHUB_TOKEN"],
	}
}

func TestNoIngestFlagIsParsed(t *testing.T) {
	// --no-ingest must be an explicit opt-out. Silently skipping ingestion
	// when credentials are absent would reproduce the exact silent failure
	// SPEC §12 singles out.
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"version", "--no-ingest"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v, want nil; --no-ingest must be a recognised flag", err)
	}
}
