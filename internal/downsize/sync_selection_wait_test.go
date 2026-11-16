package downsize

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
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
	t.Parallel()
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

type rejectedSelectionRepair struct{ *fakeSync }

func (b rejectedSelectionRepair) CopyWindow(context.Context, string, string, []string, string, syncRanges) (syncCopyResult, error) {
	return syncCopyResult{Failed: []string{"pending"}}, nil
}
func TestSelectionWaitRejectedRepairFailsLoudly(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "rejected")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err = j.append(syncRow{Kind: "window-selected", Round: 1, Source: syncD1, Skipped: []syncSkip{{"pending", syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
		t.Fatal(err)
	}
	w := syncRange{From: syncTestNow - 600, Until: syncTestNow - 120}
	f := newFakeSync()
	f.store(syncD1).archives["pending"] = []syncArchive{{Step: 60, Points: 10000}}
	f.store(syncD1).points["pending"] = map[int64]map[int64]float64{60: {w.From: 42}}
	s := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: path, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}, b: rejectedSelectionRepair{f}, j: j, forbid: map[string]bool{}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = s.waitWindowSelections(ctx)
	if !errors.Is(err, ErrSyncResidual) || !strings.Contains(err.Error(), "pending") {
		t.Fatal("rejected repair entered endless selection wait", err)
	}
}

type stalledSelectionBackend struct {
	*fakeSync
	now *time.Time
}

func (b stalledSelectionBackend) WindowPresence(context.Context, string, []string, syncRange) (map[string][]syncDigest, error) {
	*b.now = b.now.Add(3 * time.Minute)
	return map[string][]syncDigest{"pending": {{Hash: syncWindowUnresolved}}}, nil
}
func TestSelectionWaitStalledFrontierFailsWithNames(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "stalled")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err = j.append(syncRow{Kind: "window-selected", Round: 1, Source: syncD1, Skipped: []syncSkip{{"pending", syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
		t.Fatal(err)
	}
	f := newFakeSync()
	f.put(syncD1, "pending", syncTestSchema)
	now := time.Unix(syncTestNow, 0)
	s := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: path, windows: syncRanges{{From: syncTestNow - 600, Until: syncTestNow - 120}}, includeShort: true, now: func() time.Time { return now }}, b: stalledSelectionBackend{f, &now}, j: j, forbid: map[string]bool{}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = s.waitWindowSelections(ctx)
	if !errors.Is(err, ErrSyncResidual) || !strings.Contains(err.Error(), "pending") || !strings.Contains(err.Error(), syncD1) {
		t.Fatal("stalled selection did not fail with its frontier", err)
	}
	if len(s.sum.residual) != 1 || len(j.find(func(r syncRow) bool { return r.Kind == "window-resolved" })) != 0 {
		t.Fatal("stalled selection was retired")
	}
}
