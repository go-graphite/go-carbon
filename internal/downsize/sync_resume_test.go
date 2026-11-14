package downsize

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSyncResumeSavedConfigContinuesJournal(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	c, err := parseSyncCommand([]string{"--source", syncD1, "--source", syncD2, "--to", syncT, "--state", state, "--window", "gap:1799999400:1800000000", "--no-full-history", "--streams", "2", "--jwt-secret", "/test/secret", "--include-short-retention"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	b := newFakeSync()
	b.put(syncD1, "a", syncTestSchema)
	c.sync.now = func() time.Time { return time.Unix(syncTestNow, 0) }
	if err := c.run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	copies := len(b.copies)
	resumed, err := parseSyncCommand([]string{"--resume", state, "--streams", "3", "--rate", "1048576"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resumed.sync.from, c.sync.from) || resumed.sync.to != c.sync.to || !reflect.DeepEqual(resumed.sync.windows, c.sync.windows) || resumed.sync.fullHistory || !resumed.sync.includeShort || resumed.jwtSecret != c.jwtSecret || resumed.copy.streams != 3 || resumed.rate != 1048576 {
		t.Fatalf("configuration changed: %+v", resumed)
	}
	resumed.sync.now = c.sync.now
	if err := resumed.run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if len(b.copies) != copies {
		t.Fatal("resume repeated completed copies")
	}
	if err := resumed.saveConfig(); err != nil {
		t.Fatal(err)
	}
	again, err := parseSyncCommand([]string{"--resume", state})
	if err != nil || again.copy.streams != 3 || again.rate != 1048576 {
		t.Fatalf("operational override not saved: %+v %v", again, err)
	}
}

func TestSyncResumeRefusesScopeOverrides(t *testing.T) {
	c, err := parseSyncCommand(syncCLIArgs(t, "--no-copy"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"--to", syncD2}, {"--source", syncD2}, {"--window", "100:200"}, {"--ring", "other.json"}, {"--no-copy=false"}, {"--jwt-secret", "/other"}} {
		_, err := parseSyncCommand(append([]string{"--resume", c.sync.state}, args...))
		if err == nil || !strings.Contains(err.Error(), "cannot change scope") {
			t.Fatalf("%v: %v", args, err)
		}
	}
}

func TestSyncResumeLegacyUpgradeAndRejectsWrongArgv(t *testing.T) {
	c, err := parseSyncCommand(syncCLIArgs(t, "--streams", "2"))
	if err != nil {
		t.Fatal(err)
	}
	b := newFakeSync()
	if err := c.run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if _, err := parseSyncCommand([]string{"--resume", c.sync.state}); err == nil || !strings.Contains(err.Error(), "original") {
		t.Fatal(err)
	}
	wrong, err := parseSyncCommand([]string{"--resume", c.sync.state, "--source", syncD2, "--to", syncT})
	if err != nil {
		t.Fatal(err)
	}
	if err := wrong.saveConfig(); err == nil || !strings.Contains(err.Error(), "durable journal") {
		t.Fatal("saved incorrect legacy scope", err)
	}
	if _, err := os.Stat(filepath.Join(c.sync.state, "config.json")); !os.IsNotExist(err) {
		t.Fatal("wrong argv poisoned config", err)
	}
	upgraded, err := parseSyncCommand([]string{"--resume", c.sync.state, "--from", syncD1, "--to", syncT, "--streams", "2"})
	if err != nil {
		t.Fatal(err)
	}
	if err := upgraded.saveConfig(); err != nil {
		t.Fatal(err)
	}
	if err := upgraded.run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	resumed, err := parseSyncCommand([]string{"--resume", c.sync.state})
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.run(context.Background(), b); err != nil {
		t.Fatal(err)
	}
}

func TestSyncResumeRetainsRingAndNamesWithoutOriginalFiles(t *testing.T) {
	root := t.TempDir()
	ring, names, state := filepath.Join(root, "ring.json"), filepath.Join(root, "names"), filepath.Join(root, "state")
	if err := os.WriteFile(ring, []byte(`{"hash":"jump_fnv1a","nodes":[{"id":"target","node":"target","endpoint":"`+syncT+`"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(names, []byte("a\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := parseSyncCommand([]string{"--source", syncD1, "--to", syncT, "--state", state, "--ring", ring, "--names", names})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := syncIdentity(c.sync)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(ring); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(names); err != nil {
		t.Fatal(err)
	}
	resumed, err := parseSyncCommand([]string{"--resume", state})
	if err != nil {
		t.Fatal(err)
	}
	got, err := syncIdentity(resumed.sync)
	if err != nil || got != identity {
		t.Fatalf("journal identity changed: %s != %s: %v", got, identity, err)
	}
	var config savedSyncConfig
	data, err := os.ReadFile(filepath.Join(state, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(config.Flags["ring"]); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(config.Flags["names"]); err != nil {
		t.Fatal(err)
	}
}
