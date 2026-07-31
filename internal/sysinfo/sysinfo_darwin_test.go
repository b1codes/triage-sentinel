//go:build darwin

package sysinfo

import "testing"

// Captured from `vm_stat` on macOS 26.
const vmStatFixture = `Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                               50000.
Pages active:                            300000.
Pages inactive:                           20000.
Pages speculative:                         5000.
Pages throttled:                              0.
Pages wired down:                        150000.
Pages purgeable:                           1000.
"Translation faults":                  123456789.
`

func TestParseVMStatFree(t *testing.T) {
	// (50000 free + 20000 inactive + 5000 speculative) * 16384
	const want = int64(75000) * 16384

	got, err := parseVMStatFree(vmStatFixture)
	if err != nil {
		t.Fatalf("parseVMStatFree() error = %v, want nil", err)
	}
	if got != want {
		t.Errorf("parseVMStatFree() = %d, want %d", got, want)
	}
}

func TestParseVMStatFreeErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "empty", input: ""},
		{name: "no page size in header", input: "Mach Virtual Memory Statistics:\nPages free: 1.\n"},
		{name: "no free pages line", input: "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"},
		{name: "unparseable count", input: "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: lots.\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseVMStatFree(tc.input); err == nil {
				t.Error("parseVMStatFree() error = nil, want error")
			}
		})
	}
}

func TestParsePSRSS(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
	}{
		{name: "padded kilobytes", input: "  123456\n", want: 123456 * 1024},
		{name: "no padding", input: "8192", want: 8192 * 1024},
		{name: "trailing blank line", input: "4096\n\n", want: 4096 * 1024},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePSRSS(tc.input)
			if err != nil {
				t.Fatalf("parsePSRSS(%q) error = %v, want nil", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("parsePSRSS(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestParsePSRSSErrors(t *testing.T) {
	for _, input := range []string{"", "   ", "not-a-number\n"} {
		t.Run(input, func(t *testing.T) {
			if _, err := parsePSRSS(input); err == nil {
				t.Errorf("parsePSRSS(%q) error = nil, want error", input)
			}
		})
	}
}
