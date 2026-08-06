package triage

import "testing"

func TestNormalize(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "strips a line and column suffix",
			in:   "at handler (src/index.js:12:34)",
			want: "at handler (src/index.js)",
		},
		{
			name: "strips a bare line number",
			in:   "src/app/main.go:118",
			want: "src/app/main.go",
		},
		{
			name: "strips an absolute prefix down to a repo-relative path",
			in:   "/Users/someone/code/api/src/index.js:12",
			want: "src/index.js",
		},
		{
			// Regression: a checkout marker such as "/app/" must only match
			// where an absolute path begins. Matching it mid-path rewrote
			// "src/app/main.go" to "srcmain.go", and a mangled path merges
			// frames from unrelated code — the over-collapse direction.
			name: "leaves a relative path that contains a marker directory alone",
			in:   "pkg/src/thing.go:9",
			want: "pkg/src/thing.go",
		},
		{
			name: "strips a hex memory address",
			in:   "panic at 0x00c0000b4180",
			want: "panic at 0xADDR",
		},
		{
			name: "strips a uuid",
			in:   "request 3f2504e0-4f89-11d3-9a0c-0305e82c3301 failed",
			want: "request UUID failed",
		},
		{
			name: "strips an rfc3339 timestamp",
			in:   "2026-08-02T11:04:05Z handler failed",
			want: "TIMESTAMP handler failed",
		},
		{
			name: "strips a bare integer that is not part of a path",
			in:   "retry attempt 4718 of 5000",
			want: "retry attempt N of N",
		},
		{
			name: "collapses runs of whitespace",
			in:   "at   handler    (src/a.js)",
			want: "at handler (src/a.js)",
		},
		{
			name: "leaves an already-clean frame untouched",
			in:   "at handler (src/index.js)",
			want: "at handler (src/index.js)",
		},
		{
			name: "empty input stays empty",
			in:   "",
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Normalize(tc.in); got != tc.want {
				t.Errorf("Normalize(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeIsStableAcrossRuns(t *testing.T) {
	// Two occurrences of one bug differ only in volatile detail. If normalisation
	// does not erase that difference, every occurrence becomes its own
	// fingerprint and suppression never engages.
	a := "2026-08-02T11:04:05Z at handler (/home/runner/work/api/src/index.js:12:9) req=3f2504e0-4f89-11d3-9a0c-0305e82c3301"
	b := "2026-08-02T14:55:01Z at handler (/home/runner/work/api/src/index.js:12:40) req=a1b2c3d4-1111-2222-3333-444455556666"

	if Normalize(a) != Normalize(b) {
		t.Errorf("two occurrences of one bug normalised differently:\n a = %q\n b = %q", Normalize(a), Normalize(b))
	}
}

func TestExtractFrames(t *testing.T) {
	const nodeTrace = `TypeError: Cannot read properties of undefined
    at handler (/app/src/index.js:12:9)
    at Layer.handle (/app/node_modules/express/lib/router/layer.js:95:5)
    at next (/app/node_modules/express/lib/router/route.js:137:13)`

	frames := ExtractFrames(nodeTrace)
	if len(frames) != 3 {
		t.Fatalf("len(frames) = %d, want 3; got %v", len(frames), frames)
	}
	if frames[0] != "at handler (src/index.js)" {
		t.Errorf("frames[0] = %q, want the normalised first frame", frames[0])
	}

	t.Run("a message with no frames yields none", func(t *testing.T) {
		if got := ExtractFrames("database connection refused"); len(got) != 0 {
			t.Errorf("ExtractFrames() = %v, want empty", got)
		}
	})
}

func TestErrorClass(t *testing.T) {
	tests := []struct {
		name  string
		title string
		body  string
		want  string
	}{
		{name: "typed exception from body", title: "", body: "TypeError: x is undefined\n    at a (src/a.js:1)", want: "TypeError"},
		{name: "go panic", title: "", body: "panic: runtime error: index out of range", want: "panic"},
		{name: "python exception", title: "", body: "ValueError: invalid literal for int()", want: "ValueError"},
		{name: "falls back to the normalised title", title: "Deploy failed for api", body: "", want: "Deploy failed for api"},
		{name: "empty everywhere", title: "", body: "", want: "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorClass(tc.title, tc.body); got != tc.want {
				t.Errorf("ErrorClass(%q, %q) = %q, want %q", tc.title, tc.body, got, tc.want)
			}
		})
	}
}
