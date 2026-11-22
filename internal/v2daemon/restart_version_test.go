package v2daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRestartRejectsOldNameRouting(t *testing.T) {
	for _, version := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "restart.json")
			before, err := json.Marshal(restartGeometry{version, 3, 2, 4})
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, before, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := Open(root, Config{Layer: 3, Producers: 2, Shards: 4})
			if err == nil {
				_ = s.Close()
				t.Fatal("accepted incompatible routing version")
			}
			if !strings.Contains(err.Error(), "unsupported restart format version") {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("rejected store marker changed", err)
			}
			entries, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if entry.Name() != "restart.json" && entry.Name() != "restart.lock" {
					t.Fatal("opened old store data", entry.Name())
				}
			}
		})
	}
}

func TestRestartWritesCurrentNameRouting(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 2; i++ {
		s, err := Open(root, Config{Layer: 3, Producers: 2, Shards: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(root, "restart.json"))
		if err != nil {
			t.Fatal(err)
		}
		var geometry restartGeometry
		if err = json.Unmarshal(data, &geometry); err != nil {
			t.Fatal(err)
		}
		if geometry.Version != 3 {
			t.Fatalf("restart version %d, want XXH3 version 3", geometry.Version)
		}
	}
}
