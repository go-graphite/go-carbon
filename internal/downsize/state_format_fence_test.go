package downsize

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// This tests the proposed cutover marker against actual previously released
// executables. It does not claim that a production SQLite cutover is wired.
func TestFutureResumeConfigRefusedByOlderExecutables(t *testing.T) {
	binaries := os.Getenv("STORECTL_FENCE_OLD_BINARIES")
	if binaries == "" {
		t.Skip("set STORECTL_FENCE_OLD_BINARIES to separately built older executables")
	}
	for _, binary := range filepath.SplitList(binaries) {
		t.Run(filepath.Base(binary), func(t *testing.T) {
			state := t.TempDir()
			config := []byte("{\"version\":2,\"flags\":{}}\n")
			if err := os.WriteFile(filepath.Join(state, "config.json"), config, 0600); err != nil {
				t.Fatal(err)
			}
			journal := []byte("legacy evidence must remain untouched\n")
			if err := os.WriteFile(filepath.Join(state, "sync.jsonl"), journal, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "sync", "--resume", state)
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "unsupported or incomplete resume config") {
				t.Fatalf("did not refuse future format: %v %s", err, out)
			}
			for name, want := range map[string][]byte{"config.json": config, "sync.jsonl": journal} {
				got, e := os.ReadFile(filepath.Join(state, name))
				if e != nil || !bytes.Equal(got, want) {
					t.Fatalf("modified %s: %v", name, e)
				}
			}
			entries, err := os.ReadDir(state)
			if err != nil || len(entries) != 2 {
				t.Fatalf("old executable created state: %v %v", entries, err)
			}
			t.Logf("refused before replay, config and legacy journal unchanged")
		})
	}
}
