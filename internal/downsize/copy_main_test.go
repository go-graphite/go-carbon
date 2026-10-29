package downsize

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/storecopy"
)

func TestCandidatePreservesIdentityAndStagedPaths(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "candidate.toml")
	manifest := &storecopy.Manifest{Metadata: map[string]any{"node": "old-host", "hash": "carbon", "replicas": float64(1), "nodes": []any{"old-host", "other"}, "encoding": "exact", "incremental": false}}
	text := "[buckyd]\nnode='old-host'\nnodes=['old-host','other']\n[whisper]\nstore-encoding='exact'\nstore-dir='" + filepath.Join(root, "store") + "'\n[input-log]\npath='" + filepath.Join(root, "input-log") + "'\n"
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkIdentity(path, root, manifest); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(text, "node='old-host'", "node='new-example-base'", 1), strings.Replace(text, "['old-host','other']", "['other','old-host']", 1), strings.Replace(text, "store-encoding='exact'", "store-encoding='legacy'", 1), strings.Replace(text, filepath.Join(root, "input-log"), filepath.Join(root, "wrong-log"), 1)} {
		if err := os.WriteFile(path, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if err := checkIdentity(path, root, manifest); err == nil {
			t.Fatal("candidate identity/path change accepted")
		}
	}
}
