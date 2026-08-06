package store

import (
	"context"
	"testing"
	"time"
)

func seedMany(t *testing.T, db *DB, ctx context.Context, now time.Time) {
	t.Helper()
	seeds := []struct {
		ref, source, state string
	}{
		{"a", "gcplog", "received"},
		{"b", "gcplog", "triaging"},
		{"c", "github", "triaging"},
		{"d", "github", "filtered"},
	}
	for i, s := range seeds {
		p := sampleParams()
		p.SourceRef, p.Source, p.State = s.ref, s.source, s.state
		p.OccurredAt = now.Add(time.Duration(i) * time.Minute)
		if _, err := IngestIncident(ctx, db, p, now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("seeding %s: %v", s.ref, err)
		}
	}
}

func TestListIncidentsFiltersAndPaginates(t *testing.T) {
	db, ctx, now := ingestFixture(t)
	seedMany(t, db, ctx, now)

	tests := []struct {
		name      string
		filter    IncidentFilter
		wantPage  int
		wantTotal int
	}{
		{name: "no filter returns all", filter: IncidentFilter{}, wantPage: 4, wantTotal: 4},
		{name: "by state", filter: IncidentFilter{States: []string{"triaging"}}, wantPage: 2, wantTotal: 2},
		{name: "by source", filter: IncidentFilter{Sources: []string{"github"}}, wantPage: 2, wantTotal: 2},
		{name: "by several states", filter: IncidentFilter{States: []string{"received", "filtered"}}, wantPage: 2, wantTotal: 2},
		{name: "limit caps the page but not the total", filter: IncidentFilter{Limit: 1}, wantPage: 1, wantTotal: 4},
		{name: "offset walks the page", filter: IncidentFilter{Limit: 2, Offset: 2}, wantPage: 2, wantTotal: 4},
		{name: "offset past the end is empty not an error", filter: IncidentFilter{Limit: 2, Offset: 99}, wantPage: 0, wantTotal: 4},
		{name: "unknown state matches nothing", filter: IncidentFilter{States: []string{"nope"}}, wantPage: 0, wantTotal: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, total, err := ListIncidents(ctx, db, tc.filter)
			if err != nil {
				t.Fatalf("ListIncidents() error = %v, want nil", err)
			}
			if len(got) != tc.wantPage {
				t.Errorf("len(page) = %d, want %d", len(got), tc.wantPage)
			}
			if total != tc.wantTotal {
				t.Errorf("total = %d, want %d", total, tc.wantTotal)
			}
		})
	}
}

func TestListIncidentsOrdersNewestFirst(t *testing.T) {
	db, ctx, now := ingestFixture(t)
	seedMany(t, db, ctx, now)

	got, _, err := ListIncidents(ctx, db, IncidentFilter{})
	if err != nil {
		t.Fatalf("ListIncidents() error = %v", err)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].CreatedAt.Before(got[i].CreatedAt) {
			t.Fatalf("results are not newest-first at index %d", i)
		}
	}
}

func TestGetIncidentRoundTrips(t *testing.T) {
	db, ctx, now, id := seededIncident(t)

	got, ok, err := GetIncident(ctx, db, id)
	if err != nil || !ok {
		t.Fatalf("GetIncident() = %v, %v, want found", ok, err)
	}
	if got.Title != sampleParams().Title {
		t.Errorf("Title = %q, want %q", got.Title, sampleParams().Title)
	}
	if got.Metadata["severity"] != "ERROR" {
		t.Errorf("Metadata = %v, want severity ERROR", got.Metadata)
	}
	if got.OccurredAt.Location() != time.UTC {
		t.Errorf("OccurredAt location = %v, want UTC", got.OccurredAt.Location())
	}
	if got.ClosedAt != nil {
		t.Errorf("ClosedAt = %v, want nil for an open incident", got.ClosedAt)
	}
	_ = now

	t.Run("absent id reports false without erroring", func(t *testing.T) {
		_, ok, err := GetIncident(ctx, db, 999999)
		if err != nil {
			t.Fatalf("GetIncident() error = %v, want nil", err)
		}
		if ok {
			t.Error("ok = true for a missing incident")
		}
	})
}

func TestCountByState(t *testing.T) {
	db, ctx, now := ingestFixture(t)
	seedMany(t, db, ctx, now)

	counts, err := CountByState(ctx, db)
	if err != nil {
		t.Fatalf("CountByState() error = %v, want nil", err)
	}
	if counts["triaging"] != 2 {
		t.Errorf("counts[triaging] = %d, want 2", counts["triaging"])
	}
	if counts["received"] != 1 {
		t.Errorf("counts[received] = %d, want 1", counts["received"])
	}
	if _, present := counts["merged"]; present {
		t.Error("counts includes a state with no rows; only observed states belong here")
	}
}

func TestMarkFingerprint(t *testing.T) {
	db, ctx, _, id := seededIncident(t)

	if err := MarkFingerprint(ctx, db, id, "fp-xyz"); err != nil {
		t.Fatalf("MarkFingerprint() error = %v, want nil", err)
	}
	got, _, err := GetIncident(ctx, db, id)
	if err != nil {
		t.Fatalf("GetIncident() error = %v", err)
	}
	if got.Fingerprint != "fp-xyz" {
		t.Errorf("Fingerprint = %q, want %q", got.Fingerprint, "fp-xyz")
	}
}
