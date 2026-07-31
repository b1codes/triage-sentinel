package config

import (
	"errors"
	"strings"
	"testing"
)

// bcryptHash is a syntactically valid bcrypt hash. LoadEnv only checks the
// prefix, so the plaintext it corresponds to is irrelevant here; Task 12
// generates real hashes with bcrypt.GenerateFromPassword where the plaintext
// matters.
const bcryptHash = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

func lookupFrom(m map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func TestLoadEnvDefaults(t *testing.T) {
	env, err := LoadEnv(lookupFrom(map[string]string{
		"DASHBOARD_PASSWORD_HASH": bcryptHash,
	}))
	if err != nil {
		t.Fatalf("LoadEnv() error = %v, want nil", err)
	}

	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "listen addr", got: env.ListenAddr, want: "127.0.0.1:8787"},
		{name: "data dir", got: env.DataDir, want: "./var"},
		{name: "timezone", got: env.Timezone, want: "UTC"},
		{name: "log level", got: env.LogLevel, want: "info"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %q, want %q", tc.got, tc.want)
			}
		})
	}

	if env.Location == nil {
		t.Fatal("Location = nil, want resolved *time.Location")
	}
	if env.Location.String() != "UTC" {
		t.Errorf("Location = %q, want %q", env.Location.String(), "UTC")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	env, err := LoadEnv(lookupFrom(map[string]string{
		"DASHBOARD_PASSWORD_HASH": bcryptHash,
		"SENTINEL_LISTEN_ADDR":    "127.0.0.1:9000",
		"SENTINEL_DATA_DIR":       "/tmp/sentinel",
		"SENTINEL_TIMEZONE":       "America/Toronto",
		"SENTINEL_LOG_LEVEL":      "debug",
		"ANTHROPIC_API_KEY":       "sk-ant-test",
		"GITHUB_TOKEN":            "ghp_test",
	}))
	if err != nil {
		t.Fatalf("LoadEnv() error = %v, want nil", err)
	}

	if env.ListenAddr != "127.0.0.1:9000" {
		t.Errorf("ListenAddr = %q, want %q", env.ListenAddr, "127.0.0.1:9000")
	}
	if env.Location.String() != "America/Toronto" {
		t.Errorf("Location = %q, want %q", env.Location.String(), "America/Toronto")
	}
	if env.AnthropicAPIKey != "sk-ant-test" {
		t.Errorf("AnthropicAPIKey = %q, want %q", env.AnthropicAPIKey, "sk-ant-test")
	}
}

func TestLoadEnvErrors(t *testing.T) {
	tests := []struct {
		name     string
		vars     map[string]string
		wantText string
	}{
		{
			name:     "missing password hash",
			vars:     map[string]string{},
			wantText: "DASHBOARD_PASSWORD_HASH",
		},
		{
			name:     "password hash not bcrypt",
			vars:     map[string]string{"DASHBOARD_PASSWORD_HASH": "plaintext"},
			wantText: "bcrypt",
		},
		{
			name: "listen addr without port",
			vars: map[string]string{
				"DASHBOARD_PASSWORD_HASH": bcryptHash,
				"SENTINEL_LISTEN_ADDR":    "127.0.0.1",
			},
			wantText: "SENTINEL_LISTEN_ADDR",
		},
		{
			name: "unknown timezone",
			vars: map[string]string{
				"DASHBOARD_PASSWORD_HASH": bcryptHash,
				"SENTINEL_TIMEZONE":       "Mars/Olympus_Mons",
			},
			wantText: "SENTINEL_TIMEZONE",
		},
		{
			name: "invalid log level",
			vars: map[string]string{
				"DASHBOARD_PASSWORD_HASH": bcryptHash,
				"SENTINEL_LOG_LEVEL":      "chatty",
			},
			wantText: "SENTINEL_LOG_LEVEL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadEnv(lookupFrom(tc.vars))
			if err == nil {
				t.Fatal("LoadEnv() error = nil, want error")
			}
			if !errors.Is(err, ErrInvalidEnv) {
				t.Errorf("errors.Is(err, ErrInvalidEnv) = false, want true (err = %v)", err)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.wantText)
			}
		})
	}
}

func TestLoadEnvReportsAllProblemsAtOnce(t *testing.T) {
	_, err := LoadEnv(lookupFrom(map[string]string{
		"SENTINEL_LOG_LEVEL": "chatty",
		"SENTINEL_TIMEZONE":  "Mars/Olympus_Mons",
	}))
	if err == nil {
		t.Fatal("LoadEnv() error = nil, want error")
	}

	for _, want := range []string{
		"DASHBOARD_PASSWORD_HASH",
		"SENTINEL_LOG_LEVEL",
		"SENTINEL_TIMEZONE",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q; all problems must be reported at once", err.Error(), want)
		}
	}
}
