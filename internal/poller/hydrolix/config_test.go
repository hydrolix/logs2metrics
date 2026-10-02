package hydrolix

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// sql_file is resolved relative to the config file's directory, so a failure to
// read it must name the path that was actually searched. Reporting the value as
// written sends the reader looking in the wrong place.
func TestLoadConfigMissingSQLFileNamesSearchedPath(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "queries.yaml")
	config := "queries:\n  - name: demo\n    sql_file: sub/missing.sql\n"
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := LoadConfig(configPath, nil)
	if err == nil {
		t.Fatal("expected an error for an unreadable sql_file, got nil")
	}

	searched := filepath.Join(dir, "sub", "missing.sql")
	if !strings.Contains(err.Error(), searched) {
		t.Errorf("error should name the path searched (%s), got: %v", searched, err)
	}
}

// captureSlog routes the default slog logger into a buffer for the test.
func captureSlog(t *testing.T) *strings.Builder {
	t.Helper()
	var buf strings.Builder
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &buf
}

// counter and rate metrics over-count with the sliding window: every minute
// bucket is re-read about 20 times and Inc adds each time. A config using
// them must say so once per affected metric at startup, naming the query and
// column, so the misconfiguration is visible before the numbers are trusted.
// (HDX-12487)
func TestCounterAndRateTypesWarnAtStartup(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "queries.yaml")
	config := `queries:
  - name: q1
    sql: select 1
    metrics:
      - {column: colA, name: m.a, type: counter}
      - {column: colB, name: m.b, type: rate}
      - {column: colC, name: m.c, type: gauge}
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	buf := captureSlog(t)
	if _, err := LoadConfig(configPath, nil); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	logs := buf.String()
	if got := strings.Count(logs, "level=WARN"); got != 2 {
		t.Fatalf("want exactly 2 WARN lines (one per counter/rate metric), got %d:\n%s", got, logs)
	}
	for _, want := range []string{"colA", "colB", "q1"} {
		if !strings.Contains(logs, want) {
			t.Errorf("warnings should name %q, got:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, "colC") {
		t.Errorf("the gauge metric must not be warned about, got:\n%s", logs)
	}
}

// A gauge-only config is the documented correct shape and logs nothing new.
func TestGaugeOnlyConfigLogsNoWarning(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "queries.yaml")
	config := `queries:
  - name: q1
    sql: select 1
    metrics:
      - {column: colC, name: m.c, type: gauge}
      - {column: colD, name: m.d}
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	buf := captureSlog(t)
	if _, err := LoadConfig(configPath, nil); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if logs := buf.String(); strings.Contains(logs, "level=WARN") {
		t.Fatalf("gauge-only config must not warn, got:\n%s", logs)
	}
}
