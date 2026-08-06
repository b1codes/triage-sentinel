package triage

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Strategy names how a fingerprint's frames were selected. It is persisted on
// the fingerprint so grouping quality can be tuned from evidence rather than
// guessed at (design §4.4.3).
type Strategy string

// The frame-selection strategies, in ladder order.
const (
	StrategySourceRoots Strategy = "source_roots"
	StrategyDenylist    Strategy = "denylist"
	StrategyAllFrames   Strategy = "all_frames"
	StrategyWorkflow    Strategy = "workflow"
	StrategyNoFrames    Strategy = "no_frames"
)

// dependencyDirs are the open-ended denylist used when a project declares no
// source roots. It is a best effort by construction — no list can enumerate
// every ecosystem's vendor directory — which is exactly why the ladder never
// lets an empty selection through.
var dependencyDirs = []string{
	"vendor/", "node_modules/", "site-packages/", "dist-packages/",
	".venv/", "venv/", ".cargo/registry/", "go/pkg/mod/", ".gem/", ".m2/",
	".gradle/caches/", ".nuget/packages/", "bundle/",
}

// FingerprintInput is everything needed to group one failure.
type FingerprintInput struct {
	ProjectSlug string
	ErrorClass  string
	Frames      []string // already normalised
	SourceRoots []string // empty selects the denylist strategy
}

// FingerprintResult is a hash together with the evidence that produced it.
type FingerprintResult struct {
	Hash     string
	Strategy Strategy
	Frames   []string
}

// ComputeFingerprint groups a failure, recording which strategy selected its
// frames.
//
// The ladder is ordered by confidence and, critically, never yields an empty
// frame set. An empty set would hash to sha256(slug, class, "") and collapse
// every same-class failure in the project into one fingerprint — the
// over-collapse failure mode, which silently suppresses real failures and which
// nothing else in the system catches. Falling back to all frames can only split
// fingerprints apart, never merge them, so it is always the safe direction
// (design §4.4.1).
func ComputeFingerprint(in FingerprintInput) FingerprintResult {
	strategy, frames := selectFrames(in)
	return FingerprintResult{
		Hash:     hashParts(in.ProjectSlug, in.ErrorClass, strings.Join(frames, "\n")),
		Strategy: strategy,
		Frames:   frames,
	}
}

func selectFrames(in FingerprintInput) (Strategy, []string) {
	if len(in.Frames) == 0 {
		return StrategyNoFrames, nil
	}

	if len(in.SourceRoots) > 0 {
		if own := filterFrames(in.Frames, func(f string) bool {
			return matchesAnyRoot(f, in.SourceRoots)
		}); len(own) > 0 {
			return StrategySourceRoots, own
		}
		// Declared roots matched nothing. Fall through rather than return
		// empty: a mis-declared root must not silently collapse the project.
		return StrategyAllFrames, capFrames(in.Frames)
	}

	if own := filterFrames(in.Frames, func(f string) bool {
		return !isDependencyFrame(f)
	}); len(own) > 0 {
		return StrategyDenylist, own
	}

	return StrategyAllFrames, capFrames(in.Frames)
}

func filterFrames(frames []string, keep func(string) bool) []string {
	var out []string
	for _, f := range frames {
		if keep(f) {
			out = append(out, f)
		}
		if len(out) == maxFrames {
			break
		}
	}
	return out
}

func capFrames(frames []string) []string {
	if len(frames) > maxFrames {
		return frames[:maxFrames]
	}
	return frames
}

// matchesAnyRoot reports whether a frame's path falls under a declared root.
// The frame text embeds the path rather than being one, so this is a substring
// test against a normalised, slash-prefixed root.
func matchesAnyRoot(frame string, roots []string) bool {
	for _, root := range roots {
		cleaned := strings.Trim(strings.TrimSpace(root), "/")
		if cleaned == "" {
			continue
		}
		if strings.Contains(frame, cleaned+"/") {
			return true
		}
	}
	return false
}

func isDependencyFrame(frame string) bool {
	for _, dir := range dependencyDirs {
		if strings.Contains(frame, dir) {
			return true
		}
	}
	return false
}

// WorkflowFingerprint groups a CI failure by its failing job and step rather
// than by a stack trace, which CI failures do not have.
//
// jobSteps must identify the failing job and step. Grouping on the workflow
// name alone is not acceptable: it would collapse every failure of ci.yml into
// a single incident, which is the over-collapse mode this design exists to
// prevent (design §4.4.2).
func WorkflowFingerprint(projectSlug, workflow string, jobSteps []string) FingerprintResult {
	frames := make([]string, 0, len(jobSteps))
	for _, s := range jobSteps {
		if normalised := Normalize(s); normalised != "" {
			frames = append(frames, normalised)
		}
	}
	return FingerprintResult{
		Hash:     hashParts(projectSlug, "workflow:"+workflow, strings.Join(frames, "\n")),
		Strategy: StrategyWorkflow,
		Frames:   frames,
	}
}

// hashParts joins its parts with a separator that cannot occur in any of them,
// so ("ab", "c") and ("a", "bc") cannot produce the same digest.
func hashParts(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
