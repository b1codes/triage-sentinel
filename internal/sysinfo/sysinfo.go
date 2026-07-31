// Package sysinfo reports host and process resource usage. It backs the
// /api/health endpoint (SPEC §12) and the pre-spawn free-RAM and free-disk
// floors (SPEC §4.12).
package sysinfo

import "runtime"

// Snapshot is a point-in-time view of process and host memory.
type Snapshot struct {
	// RSSBytes is the process resident set size, or 0 when unavailable.
	RSSBytes int64
	// FreeRAMBytes is host memory available for allocation, or 0 when
	// unavailable.
	FreeRAMBytes int64
	// Goroutines is the current goroutine count.
	Goroutines int
}

// Sample collects a Snapshot. It never fails: a probe that cannot run reports
// 0 for its field, because a health endpoint that errors because a probe
// errored is less useful than one reporting an unknown value.
func Sample() Snapshot {
	rss, err := processRSSBytes()
	if err != nil {
		rss = 0
	}
	free, err := freeRAMBytes()
	if err != nil {
		free = 0
	}
	return Snapshot{
		RSSBytes:     rss,
		FreeRAMBytes: free,
		Goroutines:   runtime.NumGoroutine(),
	}
}
