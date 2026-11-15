package downsize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

// A terminal round followed by saved window resolutions used to start a new
// generation against the whole catalog, including names already proved clean.
func TestSyncResolvedWindowResumeOnlyRechecksResiduals(t *testing.T) {
	f := newFakeSync()
	o := convergeOptions(t)
	o.windows = syncRanges{{"gap", syncTestNow - 7200, syncTestNow - 3600}}
	o.fullHistory = false
	schema := []syncArchive{{60, 10000}}
	for _, host := range []string{syncT, syncP} {
		f.put(host, "clean", schema)
		f.put(host, "hole", schema)
	}
	delete(f.store(syncT).points["hole"][60], int64(syncTestNow-6000))
	o.noCopy = true
	if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
		t.Fatal(err)
	}
	identity, _ := syncIdentity(o)
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []syncRow{
		{Kind: "window-selected", Round: 1, Source: syncP, Skipped: []syncSkip{{"hole", syncWindowSelectionReason}}, SelectionComplete: true},
		{Kind: "window-resolved", Round: 1, Source: syncP, Selected: []string{"hole"}},
	} {
		if err := j.append(row); err != nil {
			t.Fatal(err)
		}
	}
	j.close()
	o.noCopy = false
	b := &resumeDigestWatch{fakeSync: f}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.clean, []string(nil)) {
		t.Fatalf("reverified completed names: %v", b.clean)
	}
}

type resumeDigestWatch struct {
	*fakeSync
	clean []string
}

func (b *resumeDigestWatch) Digest(ctx context.Context, host string, names []string, from, until int64) (map[string][]syncDigest, error) {
	b.mu.Lock()
	for _, n := range names {
		if n == "clean" {
			b.clean = append(b.clean, n)
		}
	}
	b.mu.Unlock()
	return b.fakeSync.Digest(ctx, host, names, from, until)
}

func TestReadyResumeRunsSavedChildBeforeRoot(t *testing.T) {
	f, o, r := newReadyFixture(t, 100)
	identity, _ := syncIdentity(o)
	processes := map[string]readyProcess{}
	for host, start := range f.starts {
		if host == syncT || host == syncD1 || host == syncD2 || host == syncP {
			processes[host] = readyProcess{Start: start, Observed: syncTestNow - 10}
		}
	}
	cp := readyRecovery{Identity: identity, Scope: r.scope, State: "ready-recovery-123-0", RestartWindow: true, Windows: syncRanges{{"restart", syncTestNow - 100, syncTestNow - 10}}, Processes: processes}
	if err := atomicStateJSON(filepath.Join(o.state, "ready-recovery.json"), cp); err != nil {
		t.Fatal(err)
	}
	rootErr := errors.New("root pass ran before saved recovery")
	err := runSyncReady(context.Background(), &o, r, f, func() error { return rootErr })
	if err != nil {
		t.Fatal(err)
	}
	if report := readReadyReport(t, o.state); report.Verdict != "READY" || report.ProofState != filepath.Join(o.state, cp.State) {
		t.Fatalf("lost child proof: %+v", report)
	}
}

func TestReadyResumePreparesBackendAndRejectsInvalidIntent(t *testing.T) {
	for _, valid := range []bool{true, false} {
		t.Run(map[bool]string{true: "valid", false: "invalid scope"}[valid], func(t *testing.T) {
			f, o, r := newReadyFixture(t, 100)
			identity, _ := syncIdentity(o)
			processes := map[string]readyProcess{}
			for host, start := range f.starts {
				processes[host] = readyProcess{Start: start, Observed: syncTestNow - 10}
			}
			cp := readyRecovery{Identity: identity, Scope: r.scope, State: "ready-recovery-123-0", RestartWindow: true, Windows: syncRanges{{"gap", syncTestNow - 100, syncTestNow - 10}}, Processes: processes}
			if !valid {
				cp.Identity = "another scope"
			}
			if err := atomicStateJSON(filepath.Join(o.state, "ready-recovery.json"), cp); err != nil {
				t.Fatal(err)
			}
			prepared, root := false, false
			err := runSyncReadyPrepared(context.Background(), &o, r, f, func() error { prepared = true; return nil }, func() error { root = true; return errors.New("unexpected root pass") })
			if !prepared || root || (err == nil) != valid {
				t.Fatalf("prepare=%v root=%v err=%v", prepared, root, err)
			}
			if !valid && readReadyReport(t, o.state).Verdict != "NOT_READY" {
				t.Fatal("bad intent certified")
			}
		})
	}
}

func TestWindowResumeSetLegacySparseMembership(t *testing.T) {
	o := convergeOptions(t)
	o.windows = syncRanges{{"gap", 1, 2}}
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte("a\t1\t1\nb\t1\t1\nc\t1\t1\nz\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	// b is never filled. A sparse later batch a..c must not clear its hole.
	for _, row := range []syncRow{
		{Kind: "verified", Round: 1, First: "a", Last: "z", Names: 4, Holes: []syncHole{{Name: "a"}, {Name: "b"}, {Name: "c"}}},
		{Kind: "copied", Phase: "fill", Round: 1, Names: 2, Selected: []string{"a", "c"}},
		{Kind: "verified", Round: 2, First: "a", Last: "c", Names: 2},
		{Kind: "verification-retry", Round: 2},
	} {
		if err := j.append(row); err != nil {
			t.Fatal(err)
		}
	}
	s := &syncer{o: o, j: j, verificationBase: 2, sum: syncSummary{Discovered: 4}}
	got, scoped, err := s.windowResumeSet()
	if err != nil || !scoped || !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("set=%v scoped=%v err=%v", got, scoped, err)
	}
	// The plan must survive an interrupted retry with the same names.
	got, scoped, err = s.windowResumeSet()
	if err != nil || !scoped || !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("resume set=%v scoped=%v err=%v", got, scoped, err)
	}
}

func TestWindowResumeSetRejectsIncompleteOriginalProof(t *testing.T) {
	o := convergeOptions(t)
	o.windows = syncRanges{{"gap", 1, 2}}
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte("a\t1\t1\nb\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := j.append(syncRow{Kind: "verified", Round: 1, First: "a", Last: "a", Names: 1}); err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: o, j: j, verificationBase: 1, sum: syncSummary{Discovered: 2}}
	if _, _, err := s.windowResumeSet(); err == nil {
		t.Fatal("accepted missing original proof")
	}
	if _, err := s.setSource([]string{"not-in-catalog"}); err == nil {
		t.Fatal("silently dropped residual outside catalog")
	}
}

// A fixed-window repair changes only its touched names. Keep their per-name
// mutation fences, including the before/after write check, without restarting
// the whole catalog's proof/data pass after each successful fill.
type repairedWindowProofWatch struct {
	*proofFixture
	repaired    atomic.Bool
	proofMu     sync.Mutex
	afterRepair map[string]bool
}

func (b *repairedWindowProofWatch) CopyWindow(ctx context.Context, source, target string, names []string, journal string, windows syncRanges) (syncCopyResult, error) {
	result, err := b.fakeSync.CopyWindow(ctx, source, target, names, journal, windows)
	if err == nil {
		b.repaired.Store(true)
	}
	return result, err
}
func (b *repairedWindowProofWatch) Proofs(ctx context.Context, host string, names []string) ([]string, error) {
	if b.repaired.Load() {
		b.proofMu.Lock()
		for _, n := range names {
			b.afterRepair[n] = true
		}
		b.proofMu.Unlock()
	}
	return b.proofFixture.Proofs(ctx, host, names)
}
func TestSyncWindowRepairRechecksOnlyTouchedMutationFences(t *testing.T) {
	f, o := frontierFixture(t)
	o.noCopy = false
	o.maxRounds = 0
	o.windows = syncRanges{{"gap", syncTestNow - 6*3600, syncTestNow - 4*3600}}
	b := &repairedWindowProofWatch{proofFixture: f, afterRepair: map[string]bool{}}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	var names []string
	for n := range b.afterRepair {
		names = append(names, n)
	}
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"metric.19"}) {
		t.Fatalf("rechecked outside repair: %v", names)
	}
}

func TestWindowResumePreservesHolesFromInterruptedBroadRetry(t *testing.T) {
	o := convergeOptions(t)
	o.windows = syncRanges{{"gap", 1, 2}}
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte("a\t1\t1\nb\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for _, row := range []syncRow{
		{Kind: "verified", Round: 1, First: "a", Last: "b", Names: 2},
		{Kind: "verification-retry", Round: 1},
		{Kind: "verified", Round: 2, First: "b", Last: "b", Names: 1, Holes: []syncHole{{Name: "b"}}},
	} {
		if err := j.append(row); err != nil {
			t.Fatal(err)
		}
	}
	s := &syncer{o: o, j: j, verificationBase: 1, sum: syncSummary{Discovered: 2}}
	for range 2 {
		got, scoped, err := s.windowResumeSet()
		if err != nil || !scoped || !reflect.DeepEqual(got, []string{"b"}) {
			t.Fatalf("lost interrupted hole: %v %v %v", got, scoped, err)
		}
	}
}
