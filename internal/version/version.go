// Package version exposes the build version stamped in at link time.
package version

import "strings"

// Version is overridden at build time with -ldflags "-X
// github.com/b1codes/triage-sentinel/internal/version.Version=<value>".
var Version string

// Get returns the build version, or "dev" when no version was stamped in.
func Get() string {
	if strings.TrimSpace(Version) == "" {
		return "dev"
	}
	return Version
}
