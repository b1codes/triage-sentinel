package config

import "sort"

// Cache multipliers applied to a model's input rate (SPEC §7.2).
const (
	cacheReadMultiplier  = 0.10
	cacheWriteMultiplier = 1.25
)

// ModelPrice is the billing rate for one model, in US dollars per million
// tokens.
type ModelPrice struct {
	InputPerMTok  float64
	OutputPerMTok float64
}

// CostUSD returns the dollar cost of one interaction. Cache reads bill at
// 0.10x the input rate and cache writes at 1.25x (SPEC §7.2). This is the
// single cost formula in the system: the budget ledger and the incremental
// per-run cost cap both call it, so they cannot disagree.
func (p ModelPrice) CostUSD(inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens int64) float64 {
	const perMTok = 1_000_000.0
	return (float64(inputTokens)/perMTok)*p.InputPerMTok +
		(float64(outputTokens)/perMTok)*p.OutputPerMTok +
		(float64(cacheReadTokens)/perMTok)*p.InputPerMTok*cacheReadMultiplier +
		(float64(cacheWriteTokens)/perMTok)*p.InputPerMTok*cacheWriteMultiplier
}

// knownModels is a local mirror of published Anthropic pricing. It is
// deliberately unexported and never mutated: a model absent from this table
// is a startup validation error rather than a runtime surprise, because the
// system must refuse to spend money it cannot price (SPEC §7.2).
//
// Model IDs are exact and carry no date suffix. Keeping this table current is
// an operator responsibility; a stale table under-reports spend, which is the
// dangerous direction.
var knownModels = map[string]ModelPrice{
	"claude-opus-5":    {InputPerMTok: 5.00, OutputPerMTok: 25.00},
	"claude-sonnet-5":  {InputPerMTok: 3.00, OutputPerMTok: 15.00},
	"claude-haiku-4-5": {InputPerMTok: 1.00, OutputPerMTok: 5.00},
}

// LookupModel returns the price for a model ID, and false if the ID is not in
// the price table.
func LookupModel(id string) (ModelPrice, bool) {
	p, ok := knownModels[id]
	return p, ok
}

// KnownModelIDs returns the priced model IDs in sorted order. The returned
// slice is a fresh copy, so callers cannot corrupt the table.
func KnownModelIDs() []string {
	ids := make([]string, 0, len(knownModels))
	for id := range knownModels {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
