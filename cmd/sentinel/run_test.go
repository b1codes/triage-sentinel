package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
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
			"serve", "-env-file", envFile, "-config", "../../projects.example.yaml",
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
