package triage

import "testing"

func TestComputeFingerprintLadder(t *testing.T) {
	appFrame := "at handler (src/index.js)"
	depFrame := "at Layer.handle (node_modules/express/lib/router/layer.js)"

	tests := []struct {
		name         string
		in           FingerprintInput
		wantStrategy Strategy
		wantFrames   int
	}{
		{
			name: "declared source roots win",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
				Frames:      []string{depFrame, appFrame},
				SourceRoots: []string{"src/"},
			},
			wantStrategy: StrategySourceRoots,
			wantFrames:   1,
		},
		{
			name: "no roots declared falls back to the denylist",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
				Frames: []string{depFrame, appFrame},
			},
			wantStrategy: StrategyDenylist,
			wantFrames:   1,
		},
		{
			name: "all frames are dependencies so all are used",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
				Frames: []string{depFrame, depFrame + "2"},
			},
			wantStrategy: StrategyAllFrames,
			wantFrames:   2,
		},
		{
			name: "roots declared but none match falls through to all frames",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
				Frames:      []string{depFrame},
				SourceRoots: []string{"cmd/"},
			},
			wantStrategy: StrategyAllFrames,
			wantFrames:   1,
		},
		{
			name: "no frames at all",
			in: FingerprintInput{
				ProjectSlug: "api", ErrorClass: "TypeError",
			},
			wantStrategy: StrategyNoFrames,
			wantFrames:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeFingerprint(tc.in)
			if got.Strategy != tc.wantStrategy {
				t.Errorf("Strategy = %q, want %q", got.Strategy, tc.wantStrategy)
			}
			if len(got.Frames) != tc.wantFrames {
				t.Errorf("len(Frames) = %d, want %d (frames = %v)", len(got.Frames), tc.wantFrames, got.Frames)
			}
			if got.Hash == "" {
				t.Error("Hash is empty")
			}
		})
	}
}

// TestFingerprintNeverOverCollapses is the regression test for design §4.4's
// asymmetry. Over-collapse silently suppresses a real failure and nothing in the
// system catches it, so this is the single most important test in the package.
func TestFingerprintNeverOverCollapses(t *testing.T) {
	t.Run("two distinct dependency-only bugs stay distinct", func(t *testing.T) {
		a := ComputeFingerprint(FingerprintInput{
			ProjectSlug: "api", ErrorClass: "TypeError",
			Frames: []string{"at Layer.handle (node_modules/express/lib/router/layer.js)"},
		})
		b := ComputeFingerprint(FingerprintInput{
			ProjectSlug: "api", ErrorClass: "TypeError",
			Frames: []string{"at Pool.query (node_modules/pg/lib/pool.js)"},
		})

		if a.Hash == b.Hash {
			t.Fatal("two distinct bugs share a fingerprint; excluding all frames left an empty frame set and collapsed them")
		}
		if a.Strategy != StrategyAllFrames || b.Strategy != StrategyAllFrames {
			t.Errorf("strategies = %q/%q, want all_frames for both", a.Strategy, b.Strategy)
		}
	})

	t.Run("same class different project stays distinct", func(t *testing.T) {
		a := ComputeFingerprint(FingerprintInput{ProjectSlug: "api", ErrorClass: "TypeError"})
		b := ComputeFingerprint(FingerprintInput{ProjectSlug: "worker", ErrorClass: "TypeError"})
		if a.Hash == b.Hash {
			t.Error("two projects share a fingerprint; project_slug must be part of the hash")
		}
	})

	t.Run("same frames different class stays distinct", func(t *testing.T) {
		frames := []string{"at handler (src/index.js)"}
		a := ComputeFingerprint(FingerprintInput{ProjectSlug: "api", ErrorClass: "TypeError", Frames: frames})
		b := ComputeFingerprint(FingerprintInput{ProjectSlug: "api", ErrorClass: "RangeError", Frames: frames})
		if a.Hash == b.Hash {
			t.Error("two error classes share a fingerprint")
		}
	})
}

func TestComputeFingerprintIsDeterministic(t *testing.T) {
	in := FingerprintInput{
		ProjectSlug: "api", ErrorClass: "TypeError",
		Frames: []string{"at handler (src/index.js)"}, SourceRoots: []string{"src/"},
	}
	if ComputeFingerprint(in).Hash != ComputeFingerprint(in).Hash {
		t.Error("ComputeFingerprint is not deterministic")
	}
}

func TestWorkflowFingerprint(t *testing.T) {
	a := WorkflowFingerprint("api", "ci.yml", []string{"test", "Run unit tests"})
	b := WorkflowFingerprint("api", "ci.yml", []string{"lint", "Run staticcheck"})

	if a.Hash == b.Hash {
		t.Fatal("two failing jobs in one workflow share a fingerprint; workflow name alone would collapse every ci.yml failure into one incident")
	}
	if a.Strategy != StrategyWorkflow {
		t.Errorf("Strategy = %q, want %q", a.Strategy, StrategyWorkflow)
	}
}
