// Package store owns the sentinel's SQLite database: connection policy,
// pragmas, and schema migrations.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite" // pure-Go driver; keeps CGO_ENABLED=0 builds working
)

// ErrOpen is returned when the database cannot be opened or configured.
var ErrOpen = errors.New("opening database")

const (
	driverName    = "sqlite"
	readerConns   = 4
	busyTimeoutMS = 5000
)

// DB holds the sentinel's database handles. Writes go through a pool capped at
// one connection so they serialise in the Go runtime rather than contending
// inside SQLite; reads use a separate pool. This is what makes SQLITE_BUSY
// structurally impossible rather than merely retried (SPEC §5).
type DB struct {
	writer *sql.DB
	reader *sql.DB
	path   string

	closeOnce sync.Once
	closeErr  error
}

// Open creates the database's parent directory if needed, opens the writer and
// reader pools, applies the required pragmas, and verifies they took effect.
func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("%w: creating directory for %s: %w", ErrOpen, path, err)
	}

	writer, err := openPool(path, 1)
	if err != nil {
		return nil, err
	}

	reader, err := openPool(path, readerConns)
	if err != nil {
		_ = writer.Close()
		return nil, err
	}

	db := &DB{writer: writer, reader: reader, path: path}

	if err := db.verifyPragmas(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// dsn builds a modernc.org/sqlite DSN. The driver applies each _pragma query
// parameter to every connection it opens, which is essential: pragmas set via a
// one-off Exec would apply to a single pooled connection only.
func dsn(path string) string {
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS))
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "synchronous(NORMAL)")
	return "file:" + path + "?" + q.Encode()
}

func openPool(path string, maxConns int) (*sql.DB, error) {
	pool, err := sql.Open(driverName, dsn(path))
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrOpen, path, err)
	}
	pool.SetMaxOpenConns(maxConns)
	pool.SetMaxIdleConns(maxConns)

	if err := pool.Ping(); err != nil {
		_ = pool.Close()
		return nil, fmt.Errorf("%w: pinging %s: %w", ErrOpen, path, err)
	}
	return pool, nil
}

// verifyPragmas asserts the pragmas actually took effect. A silently ignored
// journal_mode would cost the WAL's concurrent-reader guarantee, so this is
// checked rather than assumed.
func (db *DB) verifyPragmas() error {
	checks := []struct {
		pool  *sql.DB
		which string
		query string
		want  string
	}{
		{db.writer, "writer", "PRAGMA journal_mode", "wal"},
		{db.writer, "writer", "PRAGMA foreign_keys", "1"},
		{db.reader, "reader", "PRAGMA journal_mode", "wal"},
		{db.reader, "reader", "PRAGMA foreign_keys", "1"},
	}

	for _, c := range checks {
		var got string
		if err := c.pool.QueryRow(c.query).Scan(&got); err != nil {
			return fmt.Errorf("%w: %s %s: %w", ErrOpen, c.which, c.query, err)
		}
		if got != c.want {
			return fmt.Errorf("%w: %s %s = %q, want %q",
				ErrOpen, c.which, c.query, got, c.want)
		}
	}
	return nil
}

// Writer returns the single-connection write pool. All INSERT, UPDATE, DELETE,
// DDL, and transaction work must use it.
func (db *DB) Writer() *sql.DB { return db.writer }

// Reader returns the multi-connection read pool. Use it for SELECT only.
func (db *DB) Reader() *sql.DB { return db.reader }

// Path returns the database file path.
func (db *DB) Path() string { return db.path }

// SizeBytes returns the size of the main database file, excluding the WAL. It
// is reported by /api/health (SPEC §12).
func (db *DB) SizeBytes() (int64, error) {
	info, err := os.Stat(db.path)
	if err != nil {
		return 0, fmt.Errorf("checking database size: %w", err)
	}
	return info.Size(), nil
}

// Close closes both pools. It is safe to call more than once.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		var errs []error
		if err := db.reader.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing reader: %w", err))
		}
		if err := db.writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing writer: %w", err))
		}
		db.closeErr = errors.Join(errs...)
	})
	return db.closeErr
}
