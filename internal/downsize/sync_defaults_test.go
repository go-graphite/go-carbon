package downsize

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSyncDefaultsNoFlags(t *testing.T) {
	t.Parallel()
	cfg := filepath.Join(t.TempDir(), "go-carbon.conf")
	if err := os.WriteFile(cfg, []byte("[buckyd]\nauth-jwt-secret-file = \"/some/where/secret\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := parseSyncCommand(syncCLIArgs(t, "--config", cfg))
	if err != nil {
		t.Fatal(err)
	}
	if c.copy.streams != 64 || c.copy.progressInterval != 60*time.Second || !c.ready.noReady || c.jwtSecret != "/some/where/secret" || c.explicit["streams"] {
		t.Fatalf("defaults: streams=%d progress=%s noReady=%v jwt=%s", c.copy.streams, c.copy.progressInterval, c.ready.noReady, c.jwtSecret)
	}
	c, err = parseSyncCommand(syncCLIArgs(t, "--config", filepath.Join(t.TempDir(), "missing.conf")))
	if err != nil || c.jwtSecret != hostJWTSecretPath {
		t.Fatalf("fallback: %v %q", err, c.jwtSecret)
	}
	empty := filepath.Join(t.TempDir(), "empty.conf")
	if err := os.WriteFile(empty, []byte("[common]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := defaultJWTSecretPath(empty); got != hostJWTSecretPath {
		t.Fatal(got)
	}
}

func TestSyncReadyFlagOptsIn(t *testing.T) {
	t.Parallel()
	c, err := parseSyncCommand(syncCLIArgs(t, "--ready"))
	if err != nil || c.ready.noReady {
		t.Fatalf("--ready: %v noReady=%v", err, c.ready.noReady)
	}
	if _, err := parseSyncCommand(syncCLIArgs(t, "--no-ready")); err == nil {
		t.Fatal("--no-ready should be removed")
	}
}

func TestSyncResumeLegacyNoReadyFlag(t *testing.T) {
	t.Parallel()
	for _, saved := range []string{"true", "false"} {
		c, err := parseSyncCommand(syncCLIArgs(t, "--streams", "2", "--jwt-secret", "/test/secret"))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.saveConfig(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(c.sync.state, "config.json")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var cfg map[string]any
		if err := json.Unmarshal(data, &cfg); err != nil {
			t.Fatal(err)
		}
		flags := cfg["flags"]
		if flags == nil {
			flags = cfg["Flags"]
		}
		m := flags.(map[string]any)
		delete(m, "ready")
		m["no-ready"] = saved
		data, _ = json.Marshal(cfg)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		r, err := parseSyncCommand([]string{"--resume", c.sync.state})
		if err != nil {
			t.Fatalf("no-ready=%s: %v", saved, err)
		}
		if want := saved == "true"; r.ready.noReady != want {
			t.Fatalf("no-ready=%s: noReady=%v", saved, r.ready.noReady)
		}
	}
}
