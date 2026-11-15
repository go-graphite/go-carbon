package downsize

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

type eventuallySelectedBackend struct {
	*fakeSync
	attempts int
	names    [][]string
}

func (b *eventuallySelectedBackend) WindowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	b.attempts++
	b.names = append(b.names, append([]string(nil), names...))
	if b.attempts == 1 {
		return map[string][]syncDigest{"pending": {{Hash: syncWindowUnresolved}}}, nil
	}
	return b.Digest(ctx, source, names, w.From, w.Until)
}

func TestSelectionWaitNarrowRetiresSuperseded(t *testing.T) {
	b := &eventuallySelectedBackend{fakeSync: newFakeSync()}
	w := syncRange{From: syncTestNow - 600, Until: syncTestNow - 120}
	for _, n := range []string{"pending", "clean"} {
		b.put(syncD1, n, syncTestSchema)
	}
	path := t.TempDir()
	j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "selection-wait")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err = j.append(syncRow{Kind: "window-selected", Round: 1, Source: syncD1, Skipped: []syncSkip{{"pending", syncWindowSelectionReason}, {"pending", syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: path, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}, b: b, j: j, forbid: map[string]bool{}}
	s.addResidual(syncHole{Name: "pending", Source: syncD1}, syncWindowSelectionReason)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = s.waitWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if b.attempts != 2 || len(s.sum.residual) != 0 {
		t.Fatal(b.attempts, s.sum.residual)
	}
	for _, ns := range b.names {
		if len(ns) != 1 || ns[0] != "pending" {
			t.Fatal("scope inflated", ns)
		}
	}
	before := len(b.copies)
	if err = s.waitWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if b.attempts != 2 || len(b.copies) != before {
		t.Fatal("resolved name repeated")
	}
}
