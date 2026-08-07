package store

import (
	"encoding/json"
	"testing"
	"time"
)

func observation(incidentID int64) FingerprintObservation {
	return FingerprintObservation{
		Fingerprint: "fp-abc",
		ProjectSlug: "api",
		Strategy:    "source_roots",
		Frames:      []string{"src/index.js", "src/handler.js"},
		IncidentID:  incidentID,
		Window:      6 * time.Hour,
	}
}

func TestObserveFingerprintOpensThenSuppresses(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	first, err := ObserveFingerprint(ctx, db, observation(id), now)
	if err != nil {
		t.Fatalf("ObserveFingerprint() error = %v, want nil", err)
	}
	if first.Suppressed {
		t.Error("Suppressed = true on the first sighting, want false")
	}
	if first.TotalOccurrences != 1 {
		t.Errorf("TotalOccurrences = %d, want 1", first.TotalOccurrences)
	}
	if want := now.Add(6 * time.Hour); !first.SuppressUntil.Equal(want) {
		t.Errorf("SuppressUntil = %v, want %v", first.SuppressUntil, want)
	}

	second, err := ObserveFingerprint(ctx, db, observation(id), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ObserveFingerprint() error = %v, want nil", err)
	}
	if !second.Suppressed {
		t.Error("Suppressed = false inside the window, want true")
	}
	if second.TotalOccurrences != 2 {
		t.Errorf("TotalOccurrences = %d, want 2", second.TotalOccurrences)
	}
	if second.FirstIncidentID != id {
		t.Errorf("FirstIncidentID = %d, want %d; suppressed events attach to the original", second.FirstIncidentID, id)
	}
}

func TestObserveFingerprintReopensAfterWindow(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	if _, err := ObserveFingerprint(ctx, db, observation(id), now); err != nil {
		t.Fatalf("ObserveFingerprint() error = %v", err)
	}

	after := now.Add(6*time.Hour + time.Second)
	out, err := ObserveFingerprint(ctx, db, observation(id), after)
	if err != nil {
		t.Fatalf("ObserveFingerprint() error = %v", err)
	}
	if out.Suppressed {
		t.Error("Suppressed = true past the window, want false; the window must reopen")
	}
	if want := after.Add(6 * time.Hour); !out.SuppressUntil.Equal(want) {
		t.Errorf("SuppressUntil = %v, want %v", out.SuppressUntil, want)
	}
	if out.TotalOccurrences != 2 {
		t.Errorf("TotalOccurrences = %d, want 2; the lifetime count must not reset", out.TotalOccurrences)
	}
}

func TestObserveFingerprintRecordsEvidence(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	if _, err := ObserveFingerprint(ctx, db, observation(id), now); err != nil {
		t.Fatalf("ObserveFingerprint() error = %v", err)
	}

	rec, ok, err := GetFingerprint(ctx, db, "fp-abc")
	if err != nil || !ok {
		t.Fatalf("GetFingerprint() = %v, %v, want found", ok, err)
	}
	if rec.Strategy != "source_roots" {
		t.Errorf("Strategy = %q, want %q", rec.Strategy, "source_roots")
	}
	if len(rec.Frames) != 2 || rec.Frames[0] != "src/index.js" {
		t.Errorf("Frames = %v, want the recorded frames", rec.Frames)
	}

	t.Run("frames are stored as a JSON array", func(t *testing.T) {
		var raw string
		if err := db.Reader().QueryRowContext(ctx,
			`SELECT frames_json FROM fingerprints WHERE fingerprint = 'fp-abc'`).Scan(&raw); err != nil {
			t.Fatalf("reading frames_json: %v", err)
		}
		var decoded []string
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			t.Errorf("frames_json %q is not a JSON array: %v", raw, err)
		}
	})
}

func TestObserveFingerprintStormCollapses(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	const storm = 500
	opened := 0
	for i := range storm {
		out, err := ObserveFingerprint(ctx, db, observation(id), now.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("ObserveFingerprint(%d) error = %v", i, err)
		}
		if !out.Suppressed {
			opened++
		}
	}

	if opened != 1 {
		t.Errorf("opened %d windows, want exactly 1; a storm must collapse to one incident", opened)
	}

	rec, _, err := GetFingerprint(ctx, db, "fp-abc")
	if err != nil {
		t.Fatalf("GetFingerprint() error = %v", err)
	}
	if rec.TotalOccurrences != storm {
		t.Errorf("TotalOccurrences = %d, want %d; the storm must remain visible while silent", rec.TotalOccurrences, storm)
	}
}
