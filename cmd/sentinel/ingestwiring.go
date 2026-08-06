package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/ingest"
	"github.com/b1codes/triage-sentinel/internal/orchestrator"
	"github.com/b1codes/triage-sentinel/internal/store"
	"github.com/b1codes/triage-sentinel/internal/triage"
)

// ErrIngestEnv is returned when the environment lacks a secret ingestion needs.
var ErrIngestEnv = errors.New("incomplete ingestion environment")

// ingestStaleAfter is how long without a successful pull before /api/health
// reports ingestion stale.
const ingestStaleAfter = 30 * time.Minute

// assertIngestEnv checks the secrets M1 introduces.
//
// M0 established that each milestone asserts its own secrets at wiring time
// rather than in LoadEnv, so the binary stays runnable without credentials it
// does not use. GITHUB_TOKEN is required a milestone earlier than SPEC §14
// implies, because job-level fingerprinting reads the Actions API; read scope
// is sufficient, and M4 is the first milestone needing write.
func assertIngestEnv(env config.Env) error {
	required := []struct {
		name  string
		value string
	}{
		{"GCP_PROJECT_ID", env.GCPProjectID},
		{"PUBSUB_SUBSCRIPTION", env.PubSubSubscription},
		{"GITHUB_WEBHOOK_SECRET", env.GitHubWebhookSecret},
		{"GITHUB_TOKEN", env.GitHubToken},
	}

	var problems []error
	for _, r := range required {
		if r.value == "" {
			problems = append(problems, fmt.Errorf(
				"%s is required to run ingestion; pass --no-ingest to start without it", r.name))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %w", ErrIngestEnv, errors.Join(problems...))
	}
	return nil
}

// ingestDeps are what startIngestion needs.
type ingestDeps struct {
	Env      config.Env
	Registry config.Registry
	DB       *store.DB
	Hub      *bus.Hub
	Logger   *slog.Logger
}

// startIngestion builds the router, subscriber, and orchestrator, and starts
// both loops. Both stop when ctx is cancelled.
func startIngestion(ctx context.Context, d ingestDeps) (*ingest.Subscriber, error) {
	resolver := ingest.NewRegistryResolver(d.Registry)

	router := ingest.NewRouter(
		ingest.NewGitHubAdapter(ingest.GitHubOptions{
			Secret:   d.Env.GitHubWebhookSecret,
			Resolver: resolver,
			Jobs:     ingest.NewGitHubJobFetcher(d.Env.GitHubToken, &http.Client{Timeout: 15 * time.Second}),
		}),
		ingest.NewGCPLogAdapter(resolver),
	)

	client, err := ingest.NewPubSubClient(ctx)
	if err != nil {
		return nil, err
	}
	puller, err := ingest.NewRESTPuller(ingest.RESTOptions{
		Subscription: d.Env.PubSubSubscription,
		Client:       client,
	})
	if err != nil {
		return nil, err
	}

	writer := ingest.NewIncidentWriter(d.DB, time.Now)

	subscriber, err := ingest.NewSubscriber(ingest.SubscriberOptions{
		Puller:    puller,
		Router:    router,
		Handler:   writer,
		Malformed: writer,
		Cursor: func(ctx context.Context, at time.Time) error {
			return store.TouchIngestCursor(ctx, d.DB, "pubsub", at)
		},
		Logger: d.Logger,
	})
	if err != nil {
		return nil, err
	}

	orch, err := newOrchestrator(d)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := subscriber.Run(ctx); err != nil {
			d.Logger.Error("subscriber stopped", "error", err)
		}
	}()
	go func() {
		if err := orch.Run(ctx); err != nil {
			d.Logger.Error("orchestrator stopped", "error", err)
		}
	}()

	return subscriber, nil
}

func newOrchestrator(d ingestDeps) (*orchestrator.Orchestrator, error) {
	patterns, err := config.CompileTransientPatterns(d.Registry.Triage.TransientPatterns)
	if err != nil {
		// Unreachable in practice: validation already compiled these at load.
		// Returning rather than panicking keeps the failure legible if the
		// validation path is ever changed.
		return nil, fmt.Errorf("compiling transient patterns: %w", err)
	}

	registry := d.Registry
	return orchestrator.New(orchestrator.Deps{
		DB:  d.DB,
		Hub: d.Hub,
		Chain: triage.NewChain(triage.ChainOptions{
			TransientPatterns: patterns,
			BotEmail:          registry.Bot.Email,
		}),
		Registry: func() config.Registry { return registry },
		Logger:   d.Logger,
	})
}

// projectRows projects the registry onto the rows SyncProjects expects.
func projectRows(registry config.Registry) []store.ProjectRow {
	rows := make([]store.ProjectRow, 0, len(registry.Projects))
	for _, p := range registry.Projects {
		rows = append(rows, store.ProjectRow{
			Slug: p.Slug, Repo: p.Repo, DefaultBranch: p.DefaultBranch,
		})
	}
	return rows
}

// replayFile feeds one recorded payload through the real adapters and process
// loop, so the whole pipeline is exercisable with no GCP access at all. It is a
// development aid, and it is what makes the end-to-end test in Task 21 runnable
// on a laptop.
func replayFile(ctx context.Context, opts options, path string, stdout io.Writer) error {
	if path == "" {
		return errors.New("replay requires a path to a recorded payload file")
	}

	env, registry, err := loadConfig(opts)
	if err != nil {
		return err
	}

	db, err := store.Open(databasePath(env))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if _, err := store.Migrate(context.Background(), db); err != nil {
		return err
	}

	if err := store.SyncProjects(ctx, db, projectRows(registry), time.Now()); err != nil {
		return err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}

	logger := newLogger(env, io.Discard)
	hub := bus.NewHub(16)
	defer hub.Close()

	resolver := ingest.NewRegistryResolver(registry)
	router := ingest.NewRouter(
		ingest.NewGitHubAdapter(ingest.GitHubOptions{
			Secret: env.GitHubWebhookSecret, Resolver: resolver,
			Jobs: ingest.NewGitHubJobFetcher(env.GitHubToken, nil),
		}),
		ingest.NewGCPLogAdapter(resolver),
	)

	attributes := map[string]string{"logging.googleapis.com/timestamp": time.Now().UTC().Format(time.RFC3339)}
	if strings.Contains(path, "github") {
		attributes = map[string]string{
			"x-github-event":      inferGitHubEvent(path),
			"x-hub-signature-256": signBody(env.GitHubWebhookSecret, data),
		}
	}

	event, err := router.Route(ctx, ingest.Message{
		ID: "replay", Data: data, Attributes: attributes, PublishTime: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("routing %s: %w", path, err)
	}

	if err := ingest.NewIncidentWriter(db, time.Now).Handle(ctx, event); err != nil {
		return err
	}

	orch, err := newOrchestrator(ingestDeps{
		Env: env, Registry: registry, DB: db, Hub: hub, Logger: logger,
	})
	if err != nil {
		return err
	}
	moved, err := orch.ProcessOnce(ctx)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "replayed %s: source=%s ref=%s project=%s processed=%d\n",
		path, event.Source, event.SourceRef, event.ProjectSlug, moved)
	return nil
}

// inferGitHubEvent guesses the webhook event type from a recorded file's name,
// which is enough for a development aid.
func inferGitHubEvent(path string) string {
	switch {
	case strings.Contains(path, "workflow_run"):
		return "workflow_run"
	case strings.Contains(path, "issue"):
		return "issues"
	default:
		return "workflow_run"
	}
}

// signBody produces the signature the GitHub adapter re-verifies, so a recorded
// payload replays without disabling the check that stops a compromised relay
// injecting events.
func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
