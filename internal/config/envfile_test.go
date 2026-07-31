package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeEnvFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing env file: %v", err)
	}
	return path
}

func TestLoadEnvFile(t *testing.T) {
	path := writeEnvFile(t, `
# A comment
SENTINEL_LOG_LEVEL=debug

  SENTINEL_TIMEZONE = America/Toronto
QUOTED_DOUBLE="has spaces"
QUOTED_SINGLE='also spaces'
EMPTY=
WITH_EQUALS=postgres://u:p@host/db?sslmode=require
export EXPORTED=yes
`)

	got, err := LoadEnvFile(path)
	if err != nil {
		t.Fatalf("LoadEnvFile() error = %v, want nil", err)
	}

	tests := []struct {
		key  string
		want string
	}{
		{key: "SENTINEL_LOG_LEVEL", want: "debug"},
		{key: "SENTINEL_TIMEZONE", want: "America/Toronto"},
		{key: "QUOTED_DOUBLE", want: "has spaces"},
		{key: "QUOTED_SINGLE", want: "also spaces"},
		{key: "EMPTY", want: ""},
		{key: "WITH_EQUALS", want: "postgres://u:p@host/db?sslmode=require"},
		{key: "EXPORTED", want: "yes"},
	}
	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			if got[tc.key] != tc.want {
				t.Errorf("%s = %q, want %q", tc.key, got[tc.key], tc.want)
			}
		})
	}

	if _, ok := got["# A comment"]; ok {
		t.Error("a comment line was parsed as a variable")
	}
}

func TestLoadEnvFileErrors(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		if _, err := LoadEnvFile(filepath.Join(t.TempDir(), "nope")); err == nil {
			t.Error("LoadEnvFile() error = nil, want error")
		}
	})

	t.Run("line without equals", func(t *testing.T) {
		path := writeEnvFile(t, "THIS_IS_NOT_A_PAIR\n")
		if _, err := LoadEnvFile(path); err == nil {
			t.Error("LoadEnvFile() error = nil, want error for a line with no '='")
		}
	})
}

func TestChainLookupPrefersProcessEnvironment(t *testing.T) {
	file := map[string]string{"SENTINEL_LOG_LEVEL": "debug", "ONLY_IN_FILE": "yes"}
	process := func(key string) (string, bool) {
		if key == "SENTINEL_LOG_LEVEL" {
			return "warn", true
		}
		return "", false
	}

	lookup := ChainLookup(file, process)

	if v, ok := lookup("SENTINEL_LOG_LEVEL"); !ok || v != "warn" {
		t.Errorf("lookup(SENTINEL_LOG_LEVEL) = %q, %v; want warn, true (process env must win)", v, ok)
	}
	if v, ok := lookup("ONLY_IN_FILE"); !ok || v != "yes" {
		t.Errorf("lookup(ONLY_IN_FILE) = %q, %v; want yes, true", v, ok)
	}
	if _, ok := lookup("NOWHERE"); ok {
		t.Error("lookup(NOWHERE) ok = true, want false")
	}
}

func TestChainLookupWithNilFileMap(t *testing.T) {
	lookup := ChainLookup(nil, func(string) (string, bool) { return "v", true })
	if v, ok := lookup("ANY"); !ok || v != "v" {
		t.Errorf("lookup(ANY) = %q, %v; want v, true", v, ok)
	}
}
