package downsize

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestSyncChangedWaveKeepsPartialCopyJournal(t *testing.T) {
	for _, legacyWindow := range []bool{false, true} {
		s := &syncer{o: syncOptions{state: t.TempDir()}, legacyCopyReceipts: legacyWindow}
		if legacyWindow {
			s.o.windows = syncRanges{{Name: "gap", From: 1, Until: 2}}
		}
		create := func(names []string) (string, string) {
			t.Helper()
			path, err := s.namesFile("missing-batch", 1, 0, names)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := copyIdentity(copyOptions{froms: []string{syncD1}, to: syncT, names: strings.TrimSuffix(path, ".jsonl") + ".names"})
			if err != nil {
				t.Fatal(err)
			}
			j, err := openCopyJournal(path, identity)
			if err != nil {
				t.Fatal(err)
			}
			if err := j.done(syncD1, names[0], names[0], 1, 1, time.Second); err != nil {
				t.Fatal(err)
			}
			j.close()
			return path, identity
		}
		old, identity := create([]string{"a", "b"})
		before, err := os.ReadFile(old)
		if err != nil {
			t.Fatal(err)
		}
		matching, err := s.namesFile("missing-batch", 1, 0, []string{"b", "a"})
		if err != nil || matching != old {
			t.Fatalf("matching partial copy did not resume: %q %v", matching, err)
		}
		changed, _ := create([]string{"b", "c", "d"})
		if changed == old {
			t.Fatal("changed wave reused another copy's journal")
		}
		again, err := s.namesFile("missing-batch", 1, 0, []string{"d", "b", "c"})
		if err != nil || again != changed {
			t.Fatalf("changed wave did not resume its journal: %q %v", again, err)
		}
		after, err := os.ReadFile(old)
		if err != nil || string(before) != string(after) {
			t.Fatal("original journal changed", err)
		}
		input, err := os.ReadFile(strings.TrimSuffix(old, ".jsonl") + ".names")
		if err != nil || string(input) != "a\nb\n" {
			t.Fatal("original journal input changed", err)
		}
		j, err := openCopyJournal(old, identity)
		if err != nil {
			t.Fatal(err)
		}
		if !j.cover(syncD1)("a") || j.cover(syncD1)("b") {
			t.Fatal("partial copy receipt lost")
		}
		j.close()
	}
}
