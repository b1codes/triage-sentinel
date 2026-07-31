package sysinfo

import (
	"runtime"
	"testing"
)

func TestSampleReportsGoroutines(t *testing.T) {
	got := Sample()
	if got.Goroutines < 1 {
		t.Errorf("Goroutines = %d, want >= 1", got.Goroutines)
	}
}

func TestSampleReportsMemoryOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skipf("memory probes are darwin-specific; GOOS = %s", runtime.GOOS)
	}

	got := Sample()
	if got.RSSBytes <= 0 {
		t.Errorf("RSSBytes = %d, want > 0", got.RSSBytes)
	}
	if got.FreeRAMBytes <= 0 {
		t.Errorf("FreeRAMBytes = %d, want > 0", got.FreeRAMBytes)
	}
}

func TestSampleNeverPanics(t *testing.T) {
	// Sample must be safe on every platform: an unavailable probe reports 0
	// rather than failing, because a health endpoint that errors because a
	// probe errored is worse than one reporting an unknown.
	for i := 0; i < 3; i++ {
		_ = Sample()
	}
}

func TestFreeDiskBytes(t *testing.T) {
	got, err := FreeDiskBytes(t.TempDir())
	if err != nil {
		t.Fatalf("FreeDiskBytes() error = %v, want nil", err)
	}
	if got <= 0 {
		t.Errorf("FreeDiskBytes() = %d, want > 0", got)
	}
}

func TestFreeDiskBytesMissingPath(t *testing.T) {
	if _, err := FreeDiskBytes("/definitely/does/not/exist/anywhere"); err == nil {
		t.Error("FreeDiskBytes() error = nil, want error")
	}
}
