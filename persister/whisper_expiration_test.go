package persister

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeExpirationConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "storage-expiration.conf")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadWhisperExpiration(t *testing.T) {
	rules, err := ReadWhisperExpiration(writeExpirationConfig(t, `
[keep-important]
pattern = ^jobs\.important\.
expiration = 0s

[short-lived]
pattern = ^jobs\.
expiration = 24h
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 2 {
		t.Fatalf("rules = %d, want 2", len(rules))
	}
	if got := rules.Match("jobs.important.queue", time.Hour); got != 0 {
		t.Fatalf("important expiration = %v, want 0", got)
	}
	if got := rules.Match("jobs.worker.queue", time.Hour); got != 24*time.Hour {
		t.Fatalf("jobs expiration = %v, want 24h", got)
	}
	if got := rules.Match("service.requests", time.Hour); got != time.Hour {
		t.Fatalf("fallback expiration = %v, want 1h", got)
	}
	if !rules.Enabled() {
		t.Fatal("rules should enable expiration")
	}
}

func TestReadWhisperExpirationRejectsInvalidRules(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing pattern", body: "[rule]\nexpiration = 1h"},
		{name: "missing expiration", body: "[rule]\npattern = .*"},
		{name: "invalid pattern", body: "[rule]\npattern = [\nexpiration = 1h"},
		{name: "invalid duration", body: "[rule]\npattern = .*\nexpiration = tomorrow"},
		{name: "negative duration", body: "[rule]\npattern = .*\nexpiration = -1h"},
		{name: "unknown setting", body: "[rule]\npattern = .*\nexpiration = 1h\npriority = 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ReadWhisperExpiration(writeExpirationConfig(t, tt.body)); err == nil {
				t.Fatal("invalid expiration rule accepted")
			}
		})
	}
}

func TestWhisperExpirationRulesDisabled(t *testing.T) {
	rules, err := ReadWhisperExpiration(writeExpirationConfig(t, "[keep]\npattern = .*\nexpiration = 0"))
	if err != nil {
		t.Fatal(err)
	}
	if rules.Enabled() {
		t.Fatal("zero-only rules should not enable expiration")
	}
}
