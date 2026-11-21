package downsize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeEndpoint(t *testing.T) {
	for in, want := range map[string]string{
		"host-a":                   "http://host-a:4242",
		"host-a.example.org":       "http://host-a.example.org:4242",
		"host-a:5000":              "http://host-a:5000",
		"10.0.0.1":                 "http://10.0.0.1:4242",
		"http://host-a":            "http://host-a:4242",
		"http://host-a:4242":       "http://host-a:4242",
		"http://host-a:4242/":      "http://host-a:4242",
		"https://host-a":           "https://host-a:4242",
		"[2001:db8::1]":            "http://[2001:db8::1]:4242",
		"http://10.211.63.25:4242": "http://10.211.63.25:4242",
	} {
		got, err := normalizeEndpoint(in)
		if err != nil || got != want {
			t.Errorf("normalizeEndpoint(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "http://", "http://host-a/path", "ftp://host-a", "http://u@host-a"} {
		if got, err := normalizeEndpoint(in); err == nil {
			t.Errorf("normalizeEndpoint(%q) = %q, want error", in, got)
		}
	}
}

func stubLookup(t *testing.T, table map[string][]string) {
	t.Helper()
	old := lookupHost
	t.Cleanup(func() { lookupHost = old })
	lookupHost = func(_ context.Context, host string) ([]string, error) {
		if a, ok := table[host]; ok {
			return a, nil
		}
		return nil, errors.New("no such host")
	}
}

func TestResolveEndpoint(t *testing.T) {
	stubLookup(t, map[string][]string{"host-a": {"2001:db8::5", "10.0.0.5"}, "host-b": {"10.0.0.6"}})
	for in, want := range map[string]string{
		"http://host-a:4242":   "http://10.0.0.5:4242", // IPv4 preferred
		"http://host-b:5000":   "http://10.0.0.6:5000",
		"http://10.9.9.9:4242": "http://10.9.9.9:4242", // addresses pass through
		"https://host-a:4242":  "https://host-a:4242",  // certificates name hosts
	} {
		got, err := resolveEndpoint(context.Background(), in)
		if err != nil || got != want {
			t.Errorf("resolveEndpoint(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := resolveEndpoint(context.Background(), "http://missing:4242"); err == nil || !strings.Contains(err.Error(), `"missing"`) {
		t.Fatalf("unresolvable host error must name it: %v", err)
	}
}

func TestSyncHostnameFlagsAndResume(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	secret := syncTestJWTSecret(t)
	c, err := parseSyncCommand([]string{"--source", "host-a", "--source", "host-b:5000,http://host-c:4242/", "--to", "host-t", "--state", state, "--jwt-secret", secret})
	if err != nil {
		t.Fatal(err)
	}
	wantFrom := []string{"http://host-a:4242", "http://host-b:5000", "http://host-c:4242"}
	if !reflect.DeepEqual(c.sync.from, wantFrom) || c.sync.to != "http://host-t:4242" {
		t.Fatalf("from=%v to=%v", c.sync.from, c.sync.to)
	}
	if err := os.MkdirAll(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(state, "config.json"))
	if !strings.Contains(string(raw), "http://host-t:4242") || !strings.Contains(string(raw), "http://host-a:4242,http://host-b:5000,http://host-c:4242") {
		t.Fatalf("saved config holds typed, normalized URLs: %s", raw)
	}
	// New state resumes, and an old state with full URLs resumes too.
	r, err := parseSyncCommand([]string{"--resume", state})
	if err != nil || !reflect.DeepEqual(r.sync.from, wantFrom) || r.sync.to != "http://host-t:4242" {
		t.Fatalf("resume new: %+v %v", r.sync, err)
	}
	old := strings.ReplaceAll(string(raw), "http://host-", "http://old-host-")
	oldState := filepath.Join(t.TempDir(), "old")
	if err := os.MkdirAll(oldState, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldState, "config.json"), []byte(old), 0600); err != nil {
		t.Fatal(err)
	}
	r, err = parseSyncCommand([]string{"--resume", oldState})
	if err != nil || r.sync.to != "http://old-host-t:4242" || len(r.sync.from) != 3 {
		t.Fatalf("resume old: %+v %v", r.sync, err)
	}
}

func TestSyncMainResolvesSources(t *testing.T) {
	stubLookup(t, map[string][]string{"host-a": {"10.0.0.5"}, "host-t": {"10.0.0.9"}})
	args := []string{"--source", "host-a", "--source", "missing", "--to", "host-t", "--state", filepath.Join(t.TempDir(), "s"), "--jwt-secret", syncTestJWTSecret(t)}
	err := SyncMain(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), `"missing"`) {
		t.Fatalf("want unresolvable host error naming it, got %v", err)
	}
}

func TestLocalBuckydEndpoint(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for body, want := range map[string]string{
		"[buckyd]\nbind = \":4343\"\n":         "http://127.0.0.1:4343",
		"[buckyd]\nbind = \"0.0.0.0:4242\"\n":  "http://127.0.0.1:4242",
		"[buckyd]\nbind = \"10.1.2.3:4242\"\n": "http://10.1.2.3:4242",
	} {
		p := write("c.conf", body)
		if got, from := localBuckydEndpoint(p, "/nonexistent"); got != want || from != p {
			t.Errorf("%q: got %q from %q, want %q", body, got, from, want)
		}
	}
	if got, _ := localBuckydEndpoint(write("none.conf", "[common]\n"), "/nonexistent"); got != "" {
		t.Fatalf("no bind must give no endpoint, got %q", got)
	}
	// Omitted --to defaults to the local buckyd.
	cfg := write("d.conf", "[buckyd]\nbind = \":4242\"\n")
	c, err := parseSyncCommand([]string{"--source", "host-a", "--state", filepath.Join(dir, "st"), "--config", cfg, "--jwt-secret", syncTestJWTSecret(t)})
	if err != nil || c.sync.to != "http://127.0.0.1:4242" {
		t.Fatalf("default target: %v %v", c.sync.to, err)
	}
}
