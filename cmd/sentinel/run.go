package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/term"

	"github.com/b1codes/triage-sentinel/internal/bus"
	"github.com/b1codes/triage-sentinel/internal/config"
	"github.com/b1codes/triage-sentinel/internal/httpapi"
	"github.com/b1codes/triage-sentinel/internal/store"
	"github.com/b1codes/triage-sentinel/internal/version"
	"github.com/b1codes/triage-sentinel/internal/webassets"
)

const (
	devServerURL     = "http://127.0.0.1:5173"
	shutdownTimeout  = 15 * time.Second
	sseBufferSize    = 256
	databaseName     = "sentinel.db"
	maxPasswordBytes = 1 << 12
)

const usage = `sentinel — self-healing agent orchestrator

Usage:
  sentinel [subcommand] [flags]

Subcommands:
  serve          Run the control plane (default)
  migrate        Apply pending database migrations and exit
  validate       Validate configuration and exit
  hash-password  Read a password from stdin and print a bcrypt hash
  version        Print the build version

Flags:
`

type options struct {
	envFile string
	config  string
	listen  string
	dev     bool
}

// run is main's testable body.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runWithStdin(ctx, args, os.Stdin, stdout, stderr)
}

// runWithStdin exists so hash-password is testable without a terminal.
func runWithStdin(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	subcommand := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		subcommand, args = args[0], args[1:]
	}

	fs := flag.NewFlagSet("sentinel", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}

	var opts options
	fs.StringVar(&opts.envFile, "env-file", "", "path to a KEY=VALUE environment file (launchd uses this)")
	fs.StringVar(&opts.config, "config", "projects.yaml", "path to the project registry")
	fs.StringVar(&opts.listen, "listen", "", "override SENTINEL_LISTEN_ADDR")
	fs.BoolVar(&opts.dev, "dev", false, "proxy the dashboard to the Vite dev server instead of serving embedded assets")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // fs.Usage already wrote the help text; -h is not a failure
		}
		return err
	}

	switch subcommand {
	case "version":
		fmt.Fprintln(stdout, version.Get())
		return nil
	case "hash-password":
		return hashPassword(stdin, stdout, stderr)
	case "validate":
		return validate(opts, stdout)
	case "migrate":
		return migrate(ctx, opts, stdout)
	case "serve":
		return serve(ctx, opts, stdout, stderr)
	default:
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
		return fmt.Errorf("unknown subcommand %q", subcommand)
	}
}

// loadConfig resolves the environment and registry the same way for every
// subcommand, so `validate` genuinely validates what `serve` will use.
func loadConfig(opts options) (config.Env, config.Registry, error) {
	lookup := config.OSLookup

	if opts.envFile != "" {
		values, err := config.LoadEnvFile(opts.envFile)
		if err != nil {
			return config.Env{}, config.Registry{}, err
		}
		lookup = config.ChainLookup(values, config.OSLookup)
	}

	env, err := config.LoadEnv(lookup)
	if err != nil {
		return config.Env{}, config.Registry{}, err
	}
	if opts.listen != "" {
		env.ListenAddr = opts.listen
	}

	registry, err := config.LoadRegistry(opts.config)
	if err != nil {
		return config.Env{}, config.Registry{}, err
	}
	return env, registry, nil
}

func validate(opts options, stdout io.Writer) error {
	env, registry, err := loadConfig(opts)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "ok: configuration valid\n")
	fmt.Fprintf(stdout, "  listen:   %s\n", env.ListenAddr)
	fmt.Fprintf(stdout, "  data dir: %s\n", env.DataDir)
	fmt.Fprintf(stdout, "  timezone: %s\n", env.Location)
	fmt.Fprintf(stdout, "  projects: %d (%s)\n",
		len(registry.Projects), strings.Join(registry.Slugs(), ", "))
	return nil
}

func migrate(ctx context.Context, opts options, stdout io.Writer) error {
	env, _, err := loadConfig(opts)
	if err != nil {
		return err
	}

	db, err := store.Open(databasePath(env))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	applied, err := store.Migrate(ctx, db)
	if err != nil {
		return err
	}
	schemaVersion, err := store.SchemaVersion(ctx, db)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "applied %d migration(s); schema version %d\n", applied, schemaVersion)
	return nil
}

// hashPassword reads a password and prints its bcrypt hash.
//
// On a terminal it reads through x/term: no echo, and it returns on Enter. A
// plain io.ReadAll on a TTY would block until Ctrl-D while echoing the password
// in cleartext, which is why this must not be a one-liner. When stdin is a pipe
// it falls back to reading the stream so tests and scripts work — but an
// operator should always use the interactive path, so the password never
// appears as a shell argument or heredoc where it would land in history.
func hashPassword(stdin io.Reader, stdout, stderr io.Writer) error {
	var password string

	if f, isFile := stdin.(*os.File); isFile && term.IsTerminal(int(f.Fd())) {
		fmt.Fprint(stderr, "Password: ")
		raw, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(stderr) // ReadPassword swallows the newline the user typed
		if err != nil {
			return fmt.Errorf("reading password: %w", err)
		}
		password = string(raw)
	} else {
		raw, err := io.ReadAll(io.LimitReader(stdin, maxPasswordBytes))
		if err != nil {
			return fmt.Errorf("reading password from stdin: %w", err)
		}
		password = strings.TrimRight(string(raw), "\r\n")
	}

	if password == "" {
		return errors.New("password must not be empty")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hashing password: %w", err)
	}

	fmt.Fprintln(stdout, string(hash))
	return nil
}

func databasePath(env config.Env) string {
	return filepath.Join(env.DataDir, databaseName)
}

func newLogger(env config.Env, w io.Writer) *slog.Logger {
	levels := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: levels[env.LogLevel]}))
}

func serve(ctx context.Context, opts options, stdout, stderr io.Writer) error {
	env, registry, err := loadConfig(opts)
	if err != nil {
		return err
	}

	log := newLogger(env, stderr)
	started := time.Now()

	db, err := store.Open(databasePath(env))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Migrations run automatically: a background service that refuses to start
	// because a migration is pending is a worse failure than applying it. They
	// use context.Background() rather than ctx deliberately: store.Open already
	// creates the database file on disk, so a shutdown signal that arrives while
	// a migration transaction is mid-flight must not abort it and leave the
	// schema half-applied.
	applied, err := store.Migrate(context.Background(), db)
	if err != nil {
		return err
	}
	if applied > 0 {
		log.Info("applied migrations", "count", applied)
	}

	hub := bus.NewHub(sseBufferSize)
	defer hub.Close()

	static, err := dashboardHandler(opts, log)
	if err != nil {
		return err
	}

	srv, err := httpapi.NewServer(httpapi.Deps{
		DB:       db,
		Hub:      hub,
		Env:      env,
		Registry: registry,
		Static:   static,
		Logger:   log,
		Started:  started,
	})
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", env.ListenAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", env.ListenAddr, err)
	}

	httpServer := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		// WriteTimeout is deliberately unset. Any positive value would cut off
		// the long-lived /api/stream SSE responses mid-flight.
		WriteTimeout: 0,
		ErrorLog:     slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	reloadDone := watchReloads(ctx, srv, opts, log)

	serveErr := make(chan error, 1)
	go func() {
		log.Info("listening",
			"addr", listener.Addr().String(),
			"version", version.Get(),
			"projects", len(registry.Projects),
			"dev", opts.dev,
		)
		fmt.Fprintf(stdout, "sentinel listening on http://%s\n", listener.Addr().String())

		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		log.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
		_ = httpServer.Close()
	}

	// Closing the hub releases every SSE handler still blocked on a client
	// channel, so Shutdown's wait for in-flight requests can complete.
	hub.Close()
	<-reloadDone

	return nil
}

// dashboardHandler picks the embedded assets or the dev proxy.
func dashboardHandler(opts options, log *slog.Logger) (http.Handler, error) {
	if opts.dev {
		log.Info("serving dashboard from the vite dev server", "target", devServerURL)
		return httpapi.NewDevProxyHandler(devServerURL)
	}

	dist, err := webassets.DistFS()
	if err != nil {
		if !errors.Is(err, webassets.ErrNotBuilt) {
			return nil, err
		}
		// Not fatal: the API and SSE stream still work, and the static handler
		// serves an actionable message telling the operator to run `make web`.
		log.Warn("dashboard assets are not built; run `make web`")
	}
	return httpapi.NewStaticHandler(dist), nil
}

// watchReloads applies SIGHUP by re-reading the registry. A reload that fails
// validation is logged and discarded, leaving the running configuration in
// place (SPEC §4.1).
func watchReloads(ctx context.Context, srv *httpapi.Server, opts options, log *slog.Logger) <-chan struct{} {
	done := make(chan struct{})

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)

	go func() {
		defer close(done)
		defer signal.Stop(hup)

		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				registry, err := config.LoadRegistry(opts.config)
				if err != nil {
					log.Error("reload rejected; keeping the previous configuration",
						"error", err, "config", opts.config)
					continue
				}
				srv.SetRegistry(registry)
				log.Info("configuration reloaded", "projects", len(registry.Projects))
			}
		}
	}()

	return done
}
