package downsize

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// This is the legacy failure shape: a sparse round 16, a saved fill cursor,
// and an unresolved receipt frontier larger than the old checkpoint allowance.
// The scale gate uses the actual 128MiB allowance; ordinary tests exercise the
// identical spill path with a smaller allowance.
func seedReceiptResume(t *testing.T, s *syncer, large bool) {
	t.Helper()
	var union strings.Builder
	for _, n := range []string{"receipt.a", "receipt.b", "receipt.c", "receipt.d", "receipt.e", "receipt.f"} {
		fmt.Fprintf(&union, "%s\t1\t1\n", n)
	}
	if err := os.WriteFile(s.unionPath(), []byte(union.String()), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.missingPath(), nil, 0600); err != nil {
		t.Fatal(err)
	}
	appendRow := func(r syncRow) {
		t.Helper()
		if err := s.j.append(r); err != nil {
			t.Fatal(err)
		}
	}
	appendRow(syncRow{Kind: "discovered", Names: 6, OnTarget: 6, Both: 6})
	payload := strings.Repeat("x", 1024)
	count := 2
	if large {
		payload = strings.Repeat("x", 1<<20)
		count = 129
	}
	for i := range count {
		n := fmt.Sprintf("history.%03d", i)
		appendRow(syncRow{Kind: "verified", Round: 1, First: n, Last: n, Names: 1, Skipped: []syncSkip{{Name: n, Reason: payload}}})
	}
	appendRow(syncRow{Kind: "verification-retry", Round: 15, Selected: []string{"receipt.b", "receipt.c", "receipt.d", "receipt.e"}, SelectionComplete: true})
	appendRow(syncRow{Kind: "verified", Round: 16, First: "receipt.b", Last: "receipt.c", Names: 2})
	appendRow(syncRow{Kind: "verified", Round: 16, First: "receipt.d", Last: "receipt.e", Names: 2})
	// Conflicting membership with the same First used to overwrite the usable
	// receipt in the map, producing the exact unambiguous-receipt refusal.
	appendRow(syncRow{Kind: "verified", Round: 16, First: "receipt.d", Last: "receipt.d", Names: 1, Unindexed: []string{"receipt.d"}})
	s.sum.Discovered = 6
}

func TestSyncReceiptResumeLegacySpill(t *testing.T) {
	large := os.Getenv("STORECTL_RECEIPT_SCALE") == "1"
	s, j := phaseResumeFixture(t)
	s.o.batch = 2
	s.o.windows = syncRanges{{"gap", syncTestNow - 7200, syncTestNow - 3600}}
	s.o.fullHistory = false
	j.file.Close()
	j.lock.Close()
	if err := os.Remove(j.file.Name()); err != nil {
		t.Fatal(err)
	}
	identity, _ := syncIdentity(s.o)
	var openErr error
	j, openErr = openSyncJournal(filepath.Join(s.o.state, "sync.jsonl"), identity)
	if openErr != nil {
		t.Fatal(openErr)
	}
	s.j = j
	t.Cleanup(j.close)
	seedReceiptResume(t, s, large)
	// An interruption after planning but before observing, followed by a changed
	// batch size, must not count or log the same names twice.
	if err := j.append(syncRow{Kind: "ambiguous-receipt", Round: 16, First: "receipt.d", Last: "receipt.e", Names: 2, Selected: []string{"receipt.d", "receipt.e"}, InvalidationReason: "overlapping legacy receipts"}); err != nil {
		t.Fatal(err)
	}
	s.o.batch = 1
	f := &proofFixture{fakeSync: newFakeSync(), epoch: "one", unknown: map[string]bool{}, seen: map[string]bool{}}
	for _, host := range []string{s.o.to, s.o.from[0]} {
		for _, n := range []string{"receipt.a", "receipt.b", "receipt.c", "receipt.d", "receipt.e", "receipt.f"} {
			f.put(host, n, []syncArchive{{60, 10000}})
		}
	}
	s.b = f
	// Preserve the real saved production counter shape without depending on a
	// multi-million-name fixture or treating that display floor as data proof.
	if err := s.savePhase("fill", 16, 3854336, 9537100); err != nil {
		t.Fatal(err)
	}
	if err := s.restorePhase(); err != nil {
		t.Fatal(err)
	}
	s.verificationBase = 15
	w := s.o.windows[0]
	if err := j.append(syncRow{Kind: "verified", Round: 1, First: "receipt.a", Last: "receipt.f", Names: 6, Work: []syncRedoWork{{Keys: []uint64{redoKey("verify", s.o.to, w.From, w.Until)}, IDs: []syncRedoRange{{0, 5}}}}}); err != nil {
		t.Fatal(err)
	}
	s.redo = newSyncRedoGuard(j)
	s.startVerificationCheckpoint()
	budget := 256
	if large {
		budget = syncCheckpointLimit / 2
	}
	if err := j.checkpointVerificationBudget(budget); err != nil {
		t.Fatal(err)
	}
	cp, err := readSyncCheckpoint(j.file, j.checkpoint.Identity)
	if err != nil || cp == nil || cp.Schema != 2 {
		t.Fatalf("spill missing: cp=%v err=%v", cp != nil, err)
	}
	before := s.prog.durableFloor
	set, err := s.setSource([]string{"receipt.b", "receipt.c", "receipt.d", "receipt.e"})
	if err != nil {
		t.Fatal(err)
	}
	holes, unknown, skips, err := s.verifyRound(context.Background(), 1, set)
	if err != nil || len(holes)+len(unknown)+len(skips) != 0 {
		t.Fatalf("recovery: holes=%v unknown=%v skips=%v err=%v", holes, unknown, skips, err)
	}
	if !reflect.DeepEqual(f.seen, map[string]bool{"receipt.d": true, "receipt.e": true}) {
		t.Fatalf("replayed proven names: %v", f.seen)
	}
	if s.prog.phase != "fill" || s.resume.Round != 16 || s.prog.durableFloor != before || s.prog.done.Load() != 0 {
		t.Fatalf("saved phase changed: %+v", s.resume)
	}
	if j.progress.AmbiguousReceiptNames != 2 || j.progress.PhaseRegressions != 0 || j.progress.RedoNames["verify"] != 2 {
		t.Fatalf("wrong recovery counters: %+v", j.progress)
	}
	// A second interruption must reuse the new observations and not log, count
	// or read these names again, including after the spill cache is reopened.
	if err := j.checkpointVerificationBudget(budget); err != nil {
		t.Fatal(err)
	}
	saved, err := readSyncCheckpoint(j.file, j.checkpoint.Identity)
	if err != nil {
		t.Fatal(err)
	}
	j.rows = saved.Rows
	f.seen = map[string]bool{}
	s.o.batch = 1 // recovery membership survives new batch boundaries too
	set, err = s.setSource([]string{"receipt.b", "receipt.c", "receipt.d", "receipt.e"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = s.verifyRound(context.Background(), 1, set); err != nil {
		t.Fatal(err)
	}
	if len(f.seen) != 0 || j.progress.AmbiguousReceiptNames != 2 || j.progress.RedoNames["verify"] != 2 {
		t.Fatalf("second resume repeated recovery: seen=%v progress=%+v", f.seen, j.progress)
	}
	t.Logf("legacy fill round16 done=%d retained; ambiguous_receipt_names=2 redo_names.verify=2 phase_regressions=0 spill=%s", before, saved.ReceiptFile)
}

func TestSyncReceiptResumeChangedBatchAndDuplicate(t *testing.T) {
	s, j := phaseResumeFixture(t)
	s.sum.Discovered = 4
	s.o.batch = 3
	if err := os.WriteFile(s.unionPath(), []byte("a\t1\t1\nb\t1\t1\nc\t1\t1\nd\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, r := range []syncRow{{Kind: "verified", Round: 16, First: "a", Last: "b", Names: 2}, {Kind: "verified", Round: 16, First: "a", Last: "b", Names: 2}, {Kind: "verified", Round: 16, First: "c", Last: "d", Names: 2}} {
		if err := j.append(r); err != nil {
			t.Fatal(err)
		}
	}
	s.resume = syncPhasePosition{Phase: "fill", Round: 16}
	if _, _, _, err := s.verifyRound(context.Background(), 16, s.unionSource()); err != nil {
		t.Fatal(err)
	}
	if j.progress.AmbiguousReceiptNames != 0 {
		t.Fatal("batch tuning or duplicate repeated data")
	}
}

func TestSyncReceiptResumeContradiction(t *testing.T) {
	for _, verify := range []bool{false, true} {
		t.Run(fmt.Sprint(verify), func(t *testing.T) {
			s, j := phaseResumeFixture(t)
			phase := "fill"
			total := 1
			if verify {
				phase = "verify"
				total = 4
				if err := j.append(syncRow{Kind: "verified", Round: 16, First: "a", Last: "a", Names: 1}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.savePhase(phase, 16, 2, total); err != nil {
				t.Fatal(err)
			}
			if err := s.restorePhase(); err == nil || !strings.Contains(err.Error(), "contradictory journal") {
				t.Fatal("impossible done accepted", err)
			}
			if j.progress.ReceiptContradictions != 1 {
				t.Fatal("missing error metric")
			}
		})
	}
}

func TestSyncReceiptSpillCorruptionFallsBack(t *testing.T) {
	j, s := checkpointFixture(t)
	defer j.close()
	s.startVerificationCheckpoint()
	if err := j.checkpointVerificationBudget(1); err != nil {
		t.Fatal(err)
	}
	cp, err := readSyncCheckpoint(j.file, j.checkpoint.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.o.state, cp.ReceiptFile), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSyncCheckpoint(j.file, j.checkpoint.Identity); err == nil {
		t.Fatal("corrupt spill authorized receipt reuse")
	}
	// The original journal stays authoritative.
	data, err := os.ReadFile(j.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	var header syncRow
	if err := json.Unmarshal([]byte(strings.SplitN(string(data), "\n", 2)[0]), &header); err != nil || header.Kind != "header" {
		t.Fatal("journal changed")
	}
}
