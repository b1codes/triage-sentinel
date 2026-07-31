package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadEnvFile parses a KEY=VALUE file. It exists because launchd does not read
// .env: the plist passes --env-file so every secret stays in one chmod 600 file
// instead of a world-readable copy inside the plist (SPEC §14).
//
// Comments (#), blank lines, an optional `export ` prefix, and surrounding
// single or double quotes are handled. A line with no '=' is an error rather
// than skipped, so a typo does not silently drop a credential.
func LoadEnvFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening env file %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")

		key, value, found := strings.Cut(line, "=")
		if !found {
			return nil, fmt.Errorf("%s:%d: expected KEY=VALUE, got %q", path, lineNo, line)
		}

		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("%s:%d: empty key", path, lineNo)
		}
		values[key] = unquote(strings.TrimSpace(value))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading env file %s: %w", path, err)
	}
	return values, nil
}

func unquote(v string) string {
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			return v[1 : len(v)-1]
		}
	}
	return v
}

// ChainLookup builds a lookup function that consults the process environment
// first and the file map second, so an operator can override a single value
// ad hoc without editing the file.
func ChainLookup(file map[string]string, fallback func(string) (string, bool)) func(string) (string, bool) {
	return func(key string) (string, bool) {
		if fallback != nil {
			if v, ok := fallback(key); ok && strings.TrimSpace(v) != "" {
				return v, true
			}
		}
		v, ok := file[key]
		return v, ok
	}
}
