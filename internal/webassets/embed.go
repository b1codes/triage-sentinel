// Package webassets embeds the built dashboard SPA so the binary serves it with
// no Node process at runtime (SPEC §9).
package webassets

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
)

// ErrNotBuilt is returned when the embedded tree contains no index.html, which
// means `make web` has not run.
var ErrNotBuilt = errors.New("dashboard assets not built")

// The all: prefix makes embed include dot-files, so the committed
// dist/.gitkeep satisfies the pattern on a fresh clone. Without a committed
// placeholder this directive would fail to compile before the first frontend
// build, and the binary could not be built without Node installed.
//
//go:embed all:dist
var assets embed.FS

// DistFS returns the built asset tree rooted at dist. It returns ErrNotBuilt
// when index.html is absent.
func DistFS() (fs.FS, error) {
	sub, err := fs.Sub(assets, "dist")
	if err != nil {
		return nil, fmt.Errorf("rooting embedded assets at dist: %w", err)
	}

	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return sub, fmt.Errorf("%w: run `make web`", ErrNotBuilt)
	}
	return sub, nil
}
