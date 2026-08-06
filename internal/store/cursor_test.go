package store

import (
	"context"
	"testing"
	"time"
)

func TestIngestCursor(t *testing.T) {
	db, now := syncedDB(t)
	ctx := context.Background()

	t.Run("absent cursor reports not found", func(t *testing.T) {
		_, ok, err := LastIngestAt(ctx, db)
		if err != nil {
			t.Fatalf("LastIngestAt() error = %v, want nil", err)
		}
		if ok {
			t.Error("ok = true with no cursor rows")
		}
	})

	if err := TouchIngestCursor(ctx, db, "pubsub", now); err != nil {
		t.Fatalf("TouchIngestCursor() error = %v, want nil", err)
	}

	got, ok, err := LastIngestAt(ctx, db)
	if err != nil || !ok {
		t.Fatalf("LastIngestAt() = %v, %v, want found", ok, err)
	}
	if !got.Equal(now.UTC().Truncate(time.Second)) {
		t.Errorf("LastIngestAt() = %v, want %v", got, now.UTC())
	}

	t.Run("touch updates rather than duplicating", func(t *testing.T) {
		later := now.Add(time.Hour)
		if err := TouchIngestCursor(ctx, db, "pubsub", later); err != nil {
			t.Fatalf("TouchIngestCursor() error = %v", err)
		}
		var rows int
		if err := db.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM ingest_cursor`).Scan(&rows); err != nil {
			t.Fatalf("counting: %v", err)
		}
		if rows != 1 {
			t.Errorf("ingest_cursor rows = %d, want 1", rows)
		}
		got, _, err := LastIngestAt(ctx, db)
		if err != nil {
			t.Fatalf("LastIngestAt() error = %v", err)
		}
		if !got.Equal(later.UTC().Truncate(time.Second)) {
			t.Errorf("LastIngestAt() = %v, want %v", got, later.UTC())
		}
	})
}
