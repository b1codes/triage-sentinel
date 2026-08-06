package triage

import (
	"regexp"
	"testing"
)

func testChain(t *testing.T) *Chain {
	t.Helper()
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)connection reset by peer`),
		regexp.MustCompile(`(?i)ECONNRESET`),
		regexp.MustCompile(`(?i)the operation was canceled`),
	}
	return NewChain(ChainOptions{
		TransientPatterns: patterns,
		BotEmail:          "sentinel@example.invalid",
	})
}

func TestChainEvaluate(t *testing.T) {
	tests := []struct {
		name        string
		subject     Subject
		wantVerdict Verdict
		wantFilter  string
	}{
		{
			name:        "clean subject passes",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", Body: "at handler (src/a.js)"},
			wantVerdict: VerdictPass,
		},
		{
			name:        "quarantined project is filtered",
			subject:     Subject{ProjectSlug: "api", Quarantined: true, Title: "TypeError"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "Quarantined",
		},
		{
			name:        "transient match in the body",
			subject:     Subject{ProjectSlug: "api", Title: "job failed", Body: "read tcp: connection reset by peer"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "Transient",
		},
		{
			name:        "transient match in the title",
			subject:     Subject{ProjectSlug: "api", Title: "ECONNRESET talking to upstream"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "Transient",
		},
		{
			name:        "cancelled job is transient",
			subject:     Subject{ProjectSlug: "api", Title: "The operation was canceled."},
			wantVerdict: VerdictFiltered,
			wantFilter:  "Transient",
		},
		{
			name:        "our own commit is self-inflicted",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", AuthorEmail: "sentinel@example.invalid"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "SelfInflicted",
		},
		{
			name:        "self-inflicted match is case-insensitive",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", AuthorEmail: "Sentinel@Example.Invalid"},
			wantVerdict: VerdictFiltered,
			wantFilter:  "SelfInflicted",
		},
		{
			name:        "a human commit is not self-inflicted",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", AuthorEmail: "person@example.com"},
			wantVerdict: VerdictPass,
		},
		{
			name:        "suppressed fingerprint",
			subject:     Subject{ProjectSlug: "api", Title: "TypeError", Suppressed: true},
			wantVerdict: VerdictSuppressed,
			wantFilter:  "Fingerprint",
		},
	}

	chain := testChain(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chain.Evaluate(tc.subject)
			if got.Verdict != tc.wantVerdict {
				t.Errorf("Verdict = %q, want %q (reason %q)", got.Verdict, tc.wantVerdict, got.Reason)
			}
			if tc.wantFilter != "" && got.Filter != tc.wantFilter {
				t.Errorf("Filter = %q, want %q", got.Filter, tc.wantFilter)
			}
			if got.Verdict != VerdictPass && got.Reason == "" {
				t.Error("a rejection carries no reason; the dashboard needs one")
			}
		})
	}
}

func TestChainShortCircuitsInOrder(t *testing.T) {
	// A quarantined project whose body is also transient must report
	// Quarantined: the first matching filter wins, and evaluation stops.
	got := testChain(t).Evaluate(Subject{
		ProjectSlug: "api",
		Quarantined: true,
		Body:        "connection reset by peer",
		Suppressed:  true,
	})
	if got.Filter != "Quarantined" {
		t.Errorf("Filter = %q, want %q; the chain must short-circuit on the first match", got.Filter, "Quarantined")
	}
}

func TestChainOrderMatchesSpec(t *testing.T) {
	// Unroutable and Duplicate are enforced at the write boundary (design §3.2),
	// so they are deliberately absent here.
	want := []string{"Quarantined", "Transient", "SelfInflicted", "Fingerprint", "BuildSanity"}
	got := testChain(t).FilterNames()

	if len(got) != len(want) {
		t.Fatalf("FilterNames() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("FilterNames()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildSanityIsANoOpUntilM3(t *testing.T) {
	// BuildSanity needs a checkout and subprocess supervision, which arrive in
	// M3. It holds its chain position so M3 is a body swap with no call-site
	// churn. If this test starts failing, BuildSanity was implemented — update
	// it rather than deleting it.
	got := testChain(t).Evaluate(Subject{ProjectSlug: "api", Title: "anything at all"})
	if got.Verdict != VerdictPass {
		t.Errorf("Verdict = %q, want pass; BuildSanity must not reject in M1", got.Verdict)
	}
}

func TestChainWithNoPatternsStillPasses(t *testing.T) {
	chain := NewChain(ChainOptions{BotEmail: "sentinel@example.invalid"})
	if got := chain.Evaluate(Subject{ProjectSlug: "api", Title: "ECONNRESET"}); got.Verdict != VerdictPass {
		t.Errorf("Verdict = %q, want pass when no transient patterns are configured", got.Verdict)
	}
}

func TestChainWithNoBotEmailDoesNotFilterEveryone(t *testing.T) {
	// An empty BotEmail must never match an empty AuthorEmail, or every event
	// without commit attribution would be discarded as self-inflicted.
	chain := NewChain(ChainOptions{})
	if got := chain.Evaluate(Subject{ProjectSlug: "api", Title: "TypeError"}); got.Verdict != VerdictPass {
		t.Errorf("Verdict = %q, want pass; an unset bot email must not filter unattributed events", got.Verdict)
	}
}
