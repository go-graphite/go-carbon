package downsize

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestSyncWindowReusesLegacyMissingReceipts(t *testing.T) {
	for _, phase := range []string{"missing", "missing-batch"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), "legacy")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			if err := os.WriteFile(filepath.Join(dir, "missing.tsv"), []byte("a\t1\nb\t2\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := j.append(syncRow{Kind: "copied", Phase: phase, Source: syncD1, Round: 1, Names: 1, First: "a", Last: "a", Copied: 1}); err != nil {
				t.Fatal(err)
			}
			f := newFakeSync()
			f.put(syncD1, "a", syncTestSchema)
			f.put(syncD2, "b", syncTestSchema)
			makeSyncer := func() *syncer {
				return &syncer{o: syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: dir, streams: 1, batch: 1, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }, windows: syncRanges{{Name: "gap", From: syncTestNow - 600, Until: syncTestNow}}}, b: f, j: j, copied: map[string]bool{}, failedCp: map[string]bool{}, forbid: map[string]bool{}, sum: syncSummary{phases: map[string]*syncPhase{}}}
			}
			s := makeSyncer()
			if err := s.copyMissing(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !s.copied["a"] || !s.copied["b"] {
				t.Fatalf("lost receipts: %v", s.copied)
			}
			want := []string{syncD2 + " b"}
			if !reflect.DeepEqual(f.copies, want) {
				t.Fatalf("repeated completed import: %v", f.copies)
			}
			if err := makeSyncer().copyMissing(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(f.copies, want) {
				t.Fatalf("resume repeated import: %v", f.copies)
			}
		})
	}
}
