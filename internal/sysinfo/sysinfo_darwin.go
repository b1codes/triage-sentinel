//go:build darwin

package sysinfo

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

var errProbeUnavailable = errors.New("resource probe unavailable")

// pageSizePattern extracts the page size from vm_stat's header line:
//
//	Mach Virtual Memory Statistics: (page size of 16384 bytes)
var pageSizePattern = regexp.MustCompile(`page size of (\d+) bytes`)

// processRSSBytes reads this process's resident set size. There is no portable
// pure-Go way to read RSS on darwin, so this shells out to ps; the output
// parsing is a separate pure function so it can be unit tested.
func processRSSBytes() (int64, error) {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0, fmt.Errorf("%w: running ps: %w", errProbeUnavailable, err)
	}
	return parsePSRSS(string(out))
}

// parsePSRSS converts `ps -o rss=` output (kilobytes) to bytes.
func parsePSRSS(out string) (int64, error) {
	field := strings.TrimSpace(out)
	if i := strings.IndexAny(field, "\r\n"); i >= 0 {
		field = strings.TrimSpace(field[:i])
	}
	if field == "" {
		return 0, fmt.Errorf("%w: ps produced no rss value", errProbeUnavailable)
	}

	kb, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: parsing ps rss %q: %w", errProbeUnavailable, field, err)
	}
	return kb * 1024, nil
}

// freeRAMBytes reports memory available for allocation.
func freeRAMBytes() (int64, error) {
	out, err := exec.Command("vm_stat").Output()
	if err != nil {
		return 0, fmt.Errorf("%w: running vm_stat: %w", errProbeUnavailable, err)
	}
	return parseVMStatFree(string(out))
}

// parseVMStatFree sums the vm_stat page classes that are available for
// allocation — free, inactive, and speculative — and multiplies by the page
// size from the header. Wired and active pages are in use and excluded.
func parseVMStatFree(out string) (int64, error) {
	match := pageSizePattern.FindStringSubmatch(out)
	if match == nil {
		return 0, fmt.Errorf("%w: vm_stat header has no page size", errProbeUnavailable)
	}
	pageSize, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: parsing vm_stat page size %q: %w",
			errProbeUnavailable, match[1], err)
	}

	wanted := map[string]bool{
		"Pages free":        true,
		"Pages inactive":    true,
		"Pages speculative": true,
	}

	var pages int64
	var sawFree bool

	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		label, value, found := strings.Cut(scanner.Text(), ":")
		if !found {
			continue
		}
		label = strings.TrimSpace(label)
		if !wanted[label] {
			continue
		}

		value = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), "."))
		count, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: parsing vm_stat %q value %q: %w",
				errProbeUnavailable, label, value, err)
		}

		pages += count
		if label == "Pages free" {
			sawFree = true
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("%w: scanning vm_stat output: %w", errProbeUnavailable, err)
	}
	if !sawFree {
		return 0, fmt.Errorf("%w: vm_stat output has no \"Pages free\" line", errProbeUnavailable)
	}

	return pages * pageSize, nil
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
