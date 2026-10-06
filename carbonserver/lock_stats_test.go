package carbonserver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFileScanLockFiles(t *testing.T) {
	for _, trie := range []bool{false, true} {
		t.Run(fmt.Sprintf("trie=%t", trie), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "directory.lock"), 0o700); err != nil {
				t.Fatal(err)
			}
			locks := []string{
				filepath.Join(dir, "metric.wsp.lock"),
				filepath.Join(dir, "directory.lock", ".hidden.lock"),
				filepath.Join(dir, "orphan.lock"),
			}
			if err := os.Symlink(locks[0], filepath.Join(dir, "symlink.lock")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "ignored.lock.tmp"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			listener := NewCarbonserverListener(nil)
			listener.timeBuckets = make([]uint64, listener.buckets+1)
			listener.SetWhisperData(dir)
			listener.SetTrieIndex(trie)
			scan := func(wantLocks uint64) {
				t.Helper()
				listener.updateFileList(dir, nil, nil)
				assertSidecarGauges(t, listener, 0, 0, wantLocks)
				if listener.metrics.MetricsKnown != 0 {
					t.Fatalf("metrics known = %d, want 0", listener.metrics.MetricsKnown)
				}
			}
			scan(0)
			for i, path := range locks {
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				scan(uint64(i + 1))
			}
			for i, path := range locks {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				scan(uint64(len(locks) - i - 1))
			}
		})
	}
}
