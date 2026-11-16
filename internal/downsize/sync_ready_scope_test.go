package downsize

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadyScopeSelectionPreservesPriorJournal(t *testing.T) {
	for _, change := range []string{"window", "target", "donor", "ring", "covered", "block"} {
		t.Run(change, func(t *testing.T) {
			root := t.TempDir()
			state := filepath.Join(root, "state")
			ring := filepath.Join(root, "ring.json")
			if err := os.WriteFile(ring, []byte(`{"hash":"jump_fnv1a","nodes":[{"id":"target","node":"target","endpoint":"`+syncT+`"}]}`), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := parseSyncCommand([]string{"--source", syncD1, "--to", syncT, "--state", state, "--window", "gap:100:200", "--no-full-history", "--ring", ring})
			if err != nil {
				t.Fatal(err)
			}
			if err = c.saveConfig(); err != nil {
				t.Fatal(err)
			}
			id, _ := syncIdentity(c.sync)
			j, err := openSyncJournal(filepath.Join(state, "sync.jsonl"), id)
			if err != nil {
				t.Fatal(err)
			}
			j.close()
			if err = atomicStateJSON(filepath.Join(state, "ready.json"), readyReport{Verdict: "READY"}); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(filepath.Join(state, "sync.jsonl"))
			args := []string{"--resume", state}
			switch change {
			case "window":
				args = append(args, "--window", "gap:150:250")
			case "covered":
				args = append(args, "--window", "gap:120:180")
			case "target":
				args = append(args, "--to", syncD2)
			case "donor":
				args = append(args, "--source", syncD2)
			case "ring":
				other := filepath.Join(root, "other.json")
				if err := os.WriteFile(other, []byte(`{"hash":"jump_fnv1a","nodes":[{"id":"other","node":"other","endpoint":"`+syncT+`"}]}`), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--ring", other)
			}
			next, err := parseSyncCommand(args)
			if err != nil {
				t.Fatal(err)
			}
			if change == "block" {
				next.resuming = false
				next.explicit["block"] = true
			}
			if err = next.prepareStateIntent(); err != nil {
				t.Fatal(err)
			}
			after, _ := os.ReadFile(filepath.Join(state, "sync.jsonl"))
			if !bytes.Equal(before, after) {
				t.Fatal("prior journal changed")
			}
			if change == "covered" || change == "block" {
				if next.sync.state != state || !next.resuming {
					t.Fatalf("did not retain prior state: %+v", next)
				}
				return
			}
			if filepath.Dir(next.sync.state) != state || !strings.HasPrefix(filepath.Base(next.sync.state), "scope-") {
				t.Fatal(next.sync.state)
			}
			if change == "window" && (next.sync.windows.String() != "gap:200:250" || next.sync.fullHistory) {
				t.Fatalf("reopened old window: %+v", next.sync)
			}
			if err = next.saveConfig(); err != nil {
				t.Fatal(err)
			}
			selected := next.sync.state
			nextID, _ := syncIdentity(next.sync)
			resumed, err := parseSyncCommand([]string{"--resume", selected})
			if err != nil {
				t.Fatal(err)
			}
			resumedID, _ := syncIdentity(resumed.sync)
			if nextID != resumedID {
				t.Fatalf("child scope drift: %s != %s", nextID, resumedID)
			}
		})
	}
}
