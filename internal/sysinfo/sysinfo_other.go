//go:build !darwin

package sysinfo

import (
	"errors"
	"fmt"
	"runtime"
	"syscall"
)

var errProbeUnavailable = errors.New("resource probe unavailable")

func processRSSBytes() (int64, error) {
	return 0, fmt.Errorf("%w: rss probe not implemented for %s", errProbeUnavailable, runtime.GOOS)
}

func freeRAMBytes() (int64, error) {
	return 0, fmt.Errorf("%w: free ram probe not implemented for %s", errProbeUnavailable, runtime.GOOS)
}

// FreeDiskBytes reports bytes available to an unprivileged user on the
// filesystem containing path.
func FreeDiskBytes(path string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}
