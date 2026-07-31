// Package config loads and validates the sentinel's environment
// configuration and its project registry.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// ErrInvalidEnv is returned by LoadEnv when the environment fails validation.
var ErrInvalidEnv = errors.New("invalid environment configuration")

// Env holds host-specific configuration and secrets read from the process
// environment. Secrets in this struct are never persisted (SPEC §14).
type Env struct {
	AnthropicAPIKey       string
	GitHubToken           string
	GitHubWebhookSecret   string
	GCPProjectID          string
	PubSubSubscription    string
	SlackWebhookURL       string
	DashboardPasswordHash string

	ListenAddr string
	DataDir    string
	Timezone   string
	LogLevel   string

	// Location is Timezone resolved once at load. All budget-window
	// arithmetic uses it; no call site may use time.Local.
	Location *time.Location
}

// OSLookup reads from the real process environment. Pass it to LoadEnv in
// production; pass a map-backed func in tests.
func OSLookup(key string) (string, bool) {
	return os.LookupEnv(key)
}

const (
	defaultListenAddr = "127.0.0.1:8787"
	defaultDataDir    = "./var"
	defaultTimezone   = "UTC"
	defaultLogLevel   = "info"
)

// validLogLevels is intentionally unexported and never mutated.
var validLogLevels = map[string]bool{
	"debug": true, "info": true, "warn": true, "error": true,
}

// LoadEnv reads configuration through lookup, applies defaults, and validates
// every value. It accumulates all problems and returns them together so an
// operator sees the complete list rather than fixing them one restart at a
// time. The returned error wraps ErrInvalidEnv.
func LoadEnv(lookup func(string) (string, bool)) (Env, error) {
	get := func(key, fallback string) string {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return fallback
	}

	env := Env{
		AnthropicAPIKey:       get("ANTHROPIC_API_KEY", ""),
		GitHubToken:           get("GITHUB_TOKEN", ""),
		GitHubWebhookSecret:   get("GITHUB_WEBHOOK_SECRET", ""),
		GCPProjectID:          get("GCP_PROJECT_ID", ""),
		PubSubSubscription:    get("PUBSUB_SUBSCRIPTION", ""),
		SlackWebhookURL:       get("SLACK_WEBHOOK_URL", ""),
		DashboardPasswordHash: get("DASHBOARD_PASSWORD_HASH", ""),
		ListenAddr:            get("SENTINEL_LISTEN_ADDR", defaultListenAddr),
		DataDir:               get("SENTINEL_DATA_DIR", defaultDataDir),
		Timezone:              get("SENTINEL_TIMEZONE", defaultTimezone),
		LogLevel:              get("SENTINEL_LOG_LEVEL", defaultLogLevel),
	}

	var problems []error

	if env.DashboardPasswordHash == "" {
		problems = append(problems, errors.New("DASHBOARD_PASSWORD_HASH is required"))
	} else if !isBcryptHash(env.DashboardPasswordHash) {
		problems = append(problems, errors.New(
			"DASHBOARD_PASSWORD_HASH must be a bcrypt hash (starting $2a$, $2b$ or $2y$)"))
	}

	if _, _, err := net.SplitHostPort(env.ListenAddr); err != nil {
		problems = append(problems, fmt.Errorf(
			"SENTINEL_LISTEN_ADDR %q is not host:port: %w", env.ListenAddr, err))
	}

	loc, err := time.LoadLocation(env.Timezone)
	if err != nil {
		problems = append(problems, fmt.Errorf(
			"SENTINEL_TIMEZONE %q is not a known IANA zone: %w", env.Timezone, err))
	} else {
		env.Location = loc
	}

	if !validLogLevels[env.LogLevel] {
		problems = append(problems, fmt.Errorf(
			"SENTINEL_LOG_LEVEL %q must be one of debug, info, warn, error", env.LogLevel))
	}

	if len(problems) > 0 {
		return Env{}, fmt.Errorf("%w: %w", ErrInvalidEnv, errors.Join(problems...))
	}
	return env, nil
}

func isBcryptHash(s string) bool {
	return strings.HasPrefix(s, "$2a$") ||
		strings.HasPrefix(s, "$2b$") ||
		strings.HasPrefix(s, "$2y$")
}
