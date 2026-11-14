package downsize

import (
	"context"
	"errors"
	"io"
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
			if err := os.WriteFile(filepath.Join(dir, "missing.tsv"), []byte("a\t1\nb\t"+map[string]string{"missing": "2", "missing-batch": "1"}[phase]+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := j.append(syncRow{Kind: "copied", Phase: phase, Source: syncD1, Round: 1, Names: 1, First: "a", Last: "a", Copied: 1}); err != nil {
				t.Fatal(err)
			}
			f := newFakeSync()
			f.put(syncD1, "a", syncTestSchema)
			f.put(syncD2, "b", syncTestSchema)
			f.put(syncD1, "b", syncTestSchema)
			makeSyncer := func() *syncer {
				return &syncer{o: syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: dir, streams: 2, batch: 1, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }, windows: syncRanges{{Name: "gap", From: syncTestNow - 600, Until: syncTestNow}}}, b: &legacyWindowTransport{f}, j: j, copied: map[string]bool{}, failedCp: map[string]bool{}, forbid: map[string]bool{}, sum: syncSummary{phases: map[string]*syncPhase{}}}
			}
			s := makeSyncer()
			if err := s.copyMissing(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !s.copied["a"] || !s.copied["b"] {
				t.Fatalf("lost receipts: %v", s.copied)
			}
			source := syncD2
			if phase == "missing-batch" {
				source = syncD1
			}
			want := []string{source + " b"}
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

// Old campaigns imported full history even when verification selected windows.
// Reusing their receipts must not silently change the remaining copy transport.
type legacyWindowTransport struct{ *fakeSync }

func (f *legacyWindowTransport) CopyWindow(context.Context, string, string, []string, string, syncRanges) (syncCopyResult, error) {
	return syncCopyResult{}, errors.New("legacy campaign changed copy transport")
}

func TestSyncLegacyWindowVerificationSelection(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "union.tsv"), []byte("a\t1\t0\nb\t1\t1\nc\t1\t0\nz\t0\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: syncOptions{state: dir}, legacyCopyReceipts: true, both: 1, copied: map[string]bool{"a": true}, sum: syncSummary{Discovered: 4}}
	source := s.unionSource()
	defer source.close()
	var names []string
	for {
		n, _, err := source.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	if !reflect.DeepEqual(names, []string{"a", "b"}) || source.total != 2 {
		t.Fatalf("legacy verification boundaries changed: %v total %d", names, source.total)
	}
}
