// Package triage implements Tier 0 — the local, zero-cost filters and the
// fingerprinting that collapses an error storm into a single incident.
package triage

import (
	"regexp"
	"strings"
)

// Volatile detail that differs between two occurrences of the same bug. If any
// of this survived into a fingerprint, every occurrence would hash differently
// and suppression would never engage.
var (
	reTimestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?`)
	reUUID      = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	reHexAddr   = regexp.MustCompile(`0x[0-9a-fA-F]+`)
	reLineCol   = regexp.MustCompile(`(\.[a-zA-Z0-9]+):\d+(?::\d+)?`)
	reBareInt   = regexp.MustCompile(`\b\d+\b`)
	reSpaces    = regexp.MustCompile(`\s+`)
)

// absolutePrefixes are checkout-root markers. Everything up to and including
// one of these is machine-specific: the same bug on a laptop, a CI runner and a
// container would otherwise produce three different fingerprints.
var absolutePrefixes = []string{
	"/home/runner/work/", "/github/workspace/", "/workspace/", "/app/",
	"/usr/src/app/", "/var/task/", "/srv/", "/opt/app/",
}

// Normalize erases volatile detail from one line so two occurrences of the same
// bug produce identical text. Order matters: timestamps and UUIDs are removed
// before bare integers, or their digits would be eaten piecemeal and the
// resulting text would depend on which pattern happened to run first.
func Normalize(line string) string {
	if line == "" {
		return ""
	}

	out := reTimestamp.ReplaceAllString(line, "TIMESTAMP")
	out = reUUID.ReplaceAllString(out, "UUID")
	out = reHexAddr.ReplaceAllString(out, "0xADDR")
	out = stripAbsolutePrefix(out)
	out = reLineCol.ReplaceAllString(out, "$1")
	out = reBareInt.ReplaceAllString(out, "N")
	out = reSpaces.ReplaceAllString(out, " ")

	return strings.TrimSpace(out)
}

// stripAbsolutePrefix reduces an absolute checkout path to a repo-relative one.
// A path with no recognised marker is left alone rather than guessed at — a
// wrong guess here would merge unrelated paths, and merging is the direction
// with no backstop.
func stripAbsolutePrefix(s string) string {
	for _, prefix := range absolutePrefixes {
		if idx := indexAbsolute(s, prefix); idx >= 0 {
			return s[:idx] + s[idx+len(prefix):]
		}
	}
	// A generic /Users/<name>/code/<repo>/ or /root/<repo>/ style path: keep
	// everything from the last recognised source directory onward. This applies
	// only when the enclosing path token is genuinely absolute — a relative
	// "src/app/main.go" is already repo-relative and must be left alone.
	for _, marker := range []string{"/src/", "/internal/", "/cmd/", "/lib/", "/app/", "/pkg/"} {
		idx := strings.LastIndex(s, marker)
		if idx <= 0 {
			continue
		}
		start := strings.LastIndexAny(s[:idx+1], " \t(") + 1
		if s[start] != '/' {
			continue
		}
		return s[:start] + s[idx+1:]
	}
	return s
}

// indexAbsolute finds prefix in s only where it begins an absolute path: at the
// start of the string, or after a character that cannot be part of a path.
//
// The guard is load-bearing. Matching anywhere would let the relative path
// "src/app/main.go" hit the "/app/" checkout marker and become "srcmain.go",
// and a mangled path merges frames that belong to different code — the
// over-collapse direction, which has no backstop.
func indexAbsolute(s, prefix string) int {
	for from := 0; from < len(s); {
		rel := strings.Index(s[from:], prefix)
		if rel < 0 {
			return -1
		}
		idx := from + rel
		if idx == 0 || !isPathByte(s[idx-1]) {
			return idx
		}
		from = idx + 1
	}
	return -1
}

// isPathByte reports whether b can appear inside a path segment, which is how
// indexAbsolute tells "/app/" starting a path from "/app/" sitting inside one.
func isPathByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	}
	return b == '.' || b == '-' || b == '_' || b == '~'
}

// reFrameLine matches a stack frame in the common shapes: a leading "at " (Node,
// Java), a leading "File " (Python), or an indented path:line pair (Go).
var reFrameLine = regexp.MustCompile(`^\s*(at\s+|File\s+|\S+\.(go|js|ts|py|rb|java|kt|rs|php):\d+)`)

// maxFrames is how many frames a fingerprint considers (SPEC §4.3.2).
const maxFrames = 5

// ExtractFrames returns up to maxFrames normalised stack frames from a message
// body, in the order they appear.
func ExtractFrames(body string) []string {
	if body == "" {
		return nil
	}

	var frames []string
	for _, line := range strings.Split(body, "\n") {
		if !reFrameLine.MatchString(line) {
			continue
		}
		if normalised := Normalize(line); normalised != "" {
			frames = append(frames, normalised)
		}
		if len(frames) == maxFrames {
			break
		}
	}
	return frames
}

// reTypedError matches a leading exception class such as "TypeError:" or
// "ValueError:". Go panics are matched separately because "panic:" is lowercase.
var reTypedError = regexp.MustCompile(`^\s*([A-Z][A-Za-z0-9_]*(?:Error|Exception|Fault))\b`)

// ErrorClass identifies the kind of failure. It prefers a typed exception from
// the body, then a Go panic, then the normalised title.
func ErrorClass(title, body string) string {
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if m := reTypedError.FindStringSubmatch(trimmed); m != nil {
			return m[1]
		}
		if strings.HasPrefix(trimmed, "panic:") {
			return "panic"
		}
		break // only the first non-empty line can carry the class
	}

	if normalised := Normalize(title); normalised != "" {
		return normalised
	}
	return "unknown"
}
