package triage

import (
	"regexp"
	"strings"
)

// Verdict is the outcome of the Tier 0 chain.
type Verdict string

// The three Tier 0 outcomes. Suppressed is distinct from Filtered because a
// suppressed event is a recurrence of a known problem, while a filtered one is
// noise — the dashboard and the budget digest treat them differently.
const (
	VerdictPass       Verdict = "pass"
	VerdictFiltered   Verdict = "filtered"
	VerdictSuppressed Verdict = "suppressed"
)

// Subject is everything a Tier 0 filter may consider. Every filter is a pure
// function of this struct: suppression needs a database round trip, so the
// caller performs it and passes the result in as Suppressed. That keeps the
// whole chain exhaustively table-testable with no fixtures.
type Subject struct {
	ProjectSlug string
	Kind        string
	Title       string
	Body        string

	// AuthorEmail is the commit author when the source carries one. Empty
	// means unattributed, which must never match the bot identity.
	AuthorEmail string

	Quarantined bool
	Suppressed  bool
}

// Decision is a chain outcome together with the filter that produced it.
type Decision struct {
	Verdict Verdict
	Filter  string
	Reason  string
}

// ChainOptions configures the filters.
type ChainOptions struct {
	// TransientPatterns come from the registry, already compiled by
	// config.CompileTransientPatterns so a malformed regex refuses startup.
	TransientPatterns []*regexp.Regexp

	// BotEmail is the sentinel's own git identity. Matching it closes the
	// self-repair loop (SPEC §4.3.1, §4.9).
	BotEmail string
}

// filter is one named, short-circuiting check.
type filter struct {
	name  string
	check func(Subject) (Verdict, string)
}

// Chain is the ordered Tier 0 filter set.
type Chain struct {
	filters []filter
}

// NewChain builds the Tier 0 chain in SPEC §4.3.1 order.
//
// Unroutable and Duplicate are deliberately absent: both are enforced at the
// write boundary, by routing and by the incidents(source, source_ref) unique
// index respectively (design §3.2). Including them here would mean re-querying
// for facts the insert already established, and would race a concurrent
// duplicate delivery.
func NewChain(opts ChainOptions) *Chain {
	botEmail := strings.ToLower(strings.TrimSpace(opts.BotEmail))

	return &Chain{filters: []filter{
		{
			name: "Quarantined",
			check: func(s Subject) (Verdict, string) {
				if s.Quarantined {
					return VerdictFiltered, "project is quarantined"
				}
				return VerdictPass, ""
			},
		},
		{
			name: "Transient",
			check: func(s Subject) (Verdict, string) {
				for _, re := range opts.TransientPatterns {
					if re.MatchString(s.Title) || re.MatchString(s.Body) {
						return VerdictFiltered, "matched transient pattern " + re.String()
					}
				}
				return VerdictPass, ""
			},
		},
		{
			name: "SelfInflicted",
			check: func(s Subject) (Verdict, string) {
				author := strings.ToLower(strings.TrimSpace(s.AuthorEmail))
				// An unset bot email must never match an unattributed event,
				// or every event without commit attribution would vanish.
				if botEmail == "" || author == "" {
					return VerdictPass, ""
				}
				if author == botEmail {
					return VerdictFiltered, "authored by the sentinel's own bot identity"
				}
				return VerdictPass, ""
			},
		},
		{
			name: "Fingerprint",
			check: func(s Subject) (Verdict, string) {
				if s.Suppressed {
					return VerdictSuppressed, "fingerprint seen within its suppression window"
				}
				return VerdictPass, ""
			},
		},
		{
			name: "BuildSanity",
			check: func(Subject) (Verdict, string) {
				// No-op until M3. This filter runs a project's
				// commands.healthcheck, which needs a checkout and subprocess
				// supervision that the workspace and runner packages provide
				// from M3 (design §2.3). It holds its chain position so that
				// milestone is a body swap with no call-site churn.
				return VerdictPass, ""
			},
		},
	}}
}

// Evaluate runs the chain, stopping at the first rejection.
func (c *Chain) Evaluate(s Subject) Decision {
	for _, f := range c.filters {
		verdict, reason := f.check(s)
		if verdict != VerdictPass {
			return Decision{Verdict: verdict, Filter: f.name, Reason: reason}
		}
	}
	return Decision{Verdict: VerdictPass}
}

// FilterNames returns the chain's filters in evaluation order.
func (c *Chain) FilterNames() []string {
	names := make([]string, 0, len(c.filters))
	for _, f := range c.filters {
		names = append(names, f.name)
	}
	return names
}
