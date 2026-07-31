package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTemp(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "nested", "sentinel.db"))
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
	return db
}

func TestOpenCreatesParentDirectory(t *testing.T) {
	db := openTemp(t)
	if _, err := os.Stat(filepath.Dir(db.Path())); err != nil {
		t.Errorf("parent directory not created: %v", err)
	}
}

func TestOpenAppliesPragmas(t *testing.T) {
	db := openTemp(t)

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "journal mode is WAL", query: "PRAGMA journal_mode", want: "wal"},
		{name: "foreign keys enforced", query: "PRAGMA foreign_keys", want: "1"},
		{name: "busy timeout set", query: "PRAGMA busy_timeout", want: "5000"},
		{name: "synchronous is NORMAL", query: "PRAGMA synchronous", want: "1"},
	}

	for _, tc := range tests {
		t.Run(tc.name+" (writer)", func(t *testing.T) {
			var got string
			if err := db.Writer().QueryRow(tc.query).Scan(&got); err != nil {
				t.Fatalf("%s: %v", tc.query, err)
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", tc.query, got, tc.want)
			}
		})
		t.Run(tc.name+" (reader)", func(t *testing.T) {
			var got string
			if err := db.Reader().QueryRow(tc.query).Scan(&got); err != nil {
				t.Fatalf("%s: %v", tc.query, err)
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", tc.query, got, tc.want)
			}
		})
	}
}

func TestWriterIsSingleConnection(t *testing.T) {
	db := openTemp(t)
	if got := db.Writer().Stats().MaxOpenConnections; got != 1 {
		t.Errorf("writer MaxOpenConnections = %d, want 1", got)
	}
	if got := db.Reader().Stats().MaxOpenConnections; got < 2 {
		t.Errorf("reader MaxOpenConnections = %d, want >= 2", got)
	}
}

// TestConcurrentWritesDoNotReturnBusy is the reason the writer is capped at one
// connection. With a pool of N, these goroutines would contend inside SQLite and
// surface SQLITE_BUSY; with a pool of 1 they queue in the Go runtime instead.
func TestConcurrentWritesDoNotReturnBusy(t *testing.T) {
	db := openTemp(t)

	if _, err := db.Writer().Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	const writers = 20
	const perWriter = 25

	var wg sync.WaitGroup
	errCh := make(chan error, writers*perWriter)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := db.Writer().Exec(`INSERT INTO t (v) VALUES (?)`, "x"); err != nil {
					errCh <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent write failed: %v", err)
	}

	var count int
	if err := db.Reader().QueryRow(`SELECT count(*) FROM t`).Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if want := writers * perWriter; count != want {
		t.Errorf("row count = %d, want %d", count, want)
	}
}

// TestConcurrentReadersAndWritersDoNotBlock runs writer and reader goroutines
// against the live WAL file at the same time — not sequentially like
// TestConcurrentWritesDoNotReturnBusy, which drains all writes before issuing a
// single read. WAL mode is supposed to let readers proceed against a snapshot
// while a writer commits, and busy_timeout is supposed to absorb any residual
// contention rather than surface SQLITE_BUSY. This test exercises that
// simultaneously, under -race, to prove it rather than assume it.
func TestConcurrentReadersAndWritersDoNotBlock(t *testing.T) {
	db := openTemp(t)

	if _, err := db.Writer().Exec(`CREATE TABLE rw (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	const writers = 10
	const insertsPerWriter = 25
	const readers = 10
	const readsPerReader = 50

	var wg sync.WaitGroup
	errCh := make(chan error, writers*insertsPerWriter+readers*readsPerReader)

	// Start readers and writers together under one WaitGroup so both kinds of
	// goroutines are in flight before wg.Wait() below — that's what forces
	// reads to land while writes are still happening, rather than after.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < insertsPerWriter; i++ {
				if _, err := db.Writer().Exec(`INSERT INTO rw (v) VALUES (?)`, "x"); err != nil {
					errCh <- fmt.Errorf("write: %w", err)
				}
			}
		}()
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerReader; i++ {
				var count int
				if err := db.Reader().QueryRow(`SELECT count(*) FROM rw`).Scan(&count); err != nil {
					errCh <- fmt.Errorf("read: %w", err)
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent reader/writer traffic failed: %v", err)
	}

	var count int
	if err := db.Reader().QueryRow(`SELECT count(*) FROM rw`).Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if want := writers * insertsPerWriter; count != want {
		t.Errorf("final row count = %d, want %d", count, want)
	}
}

func TestSizeBytesGrowsAfterWrite(t *testing.T) {
	db := openTemp(t)

	before, err := db.SizeBytes()
	if err != nil {
		t.Fatalf("SizeBytes() error = %v", err)
	}

	if _, err := db.Writer().Exec(`CREATE TABLE big (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	for i := 0; i < 500; i++ {
		if _, err := db.Writer().Exec(`INSERT INTO big (v) VALUES (?)`, "padding-padding-padding"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if _, err := db.Writer().Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	after, err := db.SizeBytes()
	if err != nil {
		t.Fatalf("SizeBytes() error = %v", err)
	}
	if after <= before {
		t.Errorf("SizeBytes() = %d after writes, want > %d", after, before)
	}
}

func TestOpenUnwritablePath(t *testing.T) {
	// A path whose parent is an existing regular file cannot become a directory.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("writing blocker: %v", err)
	}

	_, err := Open(filepath.Join(blocker, "sentinel.db"))
	if err == nil {
		t.Fatal("Open() error = nil, want error")
	}
	if !errors.Is(err, ErrOpen) {
		t.Errorf("errors.Is(err, ErrOpen) = false, want true (err = %v)", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sentinel.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v, want nil", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}
