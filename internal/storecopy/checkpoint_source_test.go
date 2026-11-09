package storecopy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckpointSourceRequiresCompletedUnchangedPull(t *testing.T) {
	for _, damage := range []string{"none", "partial", "modified", "symlink", "missing-current", "duplicate"} {
		t.Run(damage, func(t *testing.T) {
			root := t.TempDir()
			root, err := filepath.EvalSymlinks(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(root, "store"), 0700); err != nil {
				t.Fatal(err)
			}
			state := Progress{Manifest: &Manifest{Version: 1, ID: strings.Repeat("a", 32)}, Files: map[string]FileStamp{}}
			for _, name := range []string{"CHUNKSTORE", "CURRENT"} {
				path := filepath.Join(root, "store", name)
				data := []byte(name)
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				proof, err := stamp(info)
				if err != nil {
					t.Fatal(err)
				}
				file := File{"store/" + name, int64(len(data))}
				state.Manifest.Files = append(state.Manifest.Files, file)
				state.Files[file.Name] = proof
			}
			switch damage {
			case "partial":
				delete(state.Files, "store/CURRENT")
			case "modified":
				if err := os.WriteFile(filepath.Join(root, "store/CURRENT"), []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(filepath.Join(root, "store/CURRENT"), filepath.Join(root, "elsewhere")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(root, "elsewhere"), filepath.Join(root, "store/CURRENT")); err != nil {
					t.Fatal(err)
				}
			case "missing-current":
				state.Manifest.Files = state.Manifest.Files[:1]
			case "duplicate":
				state.Manifest.Files = append(state.Manifest.Files, state.Manifest.Files[0])
			}
			data, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "copy-state.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = CheckpointSourceManifest(root)
			if (damage == "none") != (err == nil) {
				t.Fatalf("damage=%s err=%v", damage, err)
			}
		})
	}
}
