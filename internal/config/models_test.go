package config

import (
	"math"
	"testing"
)

func TestLookupModel(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantOK  bool
		wantIn  float64
		wantOut float64
	}{
		{name: "opus 5", id: "claude-opus-5", wantOK: true, wantIn: 5.00, wantOut: 25.00},
		{name: "sonnet 5", id: "claude-sonnet-5", wantOK: true, wantIn: 3.00, wantOut: 15.00},
		{name: "haiku 4.5", id: "claude-haiku-4-5", wantOK: true, wantIn: 1.00, wantOut: 5.00},
		{name: "unknown", id: "claude-imaginary-9", wantOK: false},
		{name: "empty", id: "", wantOK: false},
		{name: "date suffixed alias is not known", id: "claude-opus-5-20260101", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			price, ok := LookupModel(tc.id)
			if ok != tc.wantOK {
				t.Fatalf("LookupModel(%q) ok = %v, want %v", tc.id, ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if price.InputPerMTok != tc.wantIn {
				t.Errorf("InputPerMTok = %v, want %v", price.InputPerMTok, tc.wantIn)
			}
			if price.OutputPerMTok != tc.wantOut {
				t.Errorf("OutputPerMTok = %v, want %v", price.OutputPerMTok, tc.wantOut)
			}
		})
	}
}

func TestKnownModelIDsIsSortedAndNonEmpty(t *testing.T) {
	ids := KnownModelIDs()
	if len(ids) == 0 {
		t.Fatal("KnownModelIDs() is empty")
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			t.Errorf("KnownModelIDs() not sorted: %q >= %q", ids[i-1], ids[i])
		}
	}
}

func TestKnownModelIDsCannotBeMutatedByCaller(t *testing.T) {
	first := KnownModelIDs()
	first[0] = "clobbered"
	second := KnownModelIDs()
	if second[0] == "clobbered" {
		t.Error("KnownModelIDs() returns a shared slice; callers can corrupt the table")
	}
}

func TestCostUSD(t *testing.T) {
	// Opus 5: $5.00 per Mtok in, $25.00 per Mtok out.
	// Cache reads bill at 0.10x input; cache writes at 1.25x input (SPEC §7.2).
	price, ok := LookupModel("claude-opus-5")
	if !ok {
		t.Fatal("claude-opus-5 missing from price table")
	}

	tests := []struct {
		name       string
		in         int64
		out        int64
		cacheRead  int64
		cacheWrite int64
		want       float64
	}{
		{name: "zero", want: 0},
		{name: "one Mtok input", in: 1_000_000, want: 5.00},
		{name: "one Mtok output", out: 1_000_000, want: 25.00},
		{name: "one Mtok cache read", cacheRead: 1_000_000, want: 0.50},
		{name: "one Mtok cache write", cacheWrite: 1_000_000, want: 6.25},
		{
			name: "combined",
			in:   200_000, out: 40_000, cacheRead: 800_000, cacheWrite: 100_000,
			// 0.2*5 + 0.04*25 + 0.8*0.5 + 0.1*6.25 = 1 + 1 + 0.4 + 0.625
			want: 3.025,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := price.CostUSD(tc.in, tc.out, tc.cacheRead, tc.cacheWrite)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("CostUSD() = %v, want %v", got, tc.want)
			}
		})
	}
}
