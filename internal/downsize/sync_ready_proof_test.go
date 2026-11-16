package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeReadyBlock(t *testing.T, o syncOptions, r syncReadyOptions, complete bool) string {
	t.Helper()
	b := syncBlock{Target: o.to, Donors: append([]string(nil), o.from...), Period: r.period[0], CarbonAPIURL: r.carbonAPI, GapLedgerComplete: complete}
	data, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "ready.json")
	if err = os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestIndependentReadyProofWidensWithoutChangingCopyJournal(t *testing.T) {
	t.Parallel()
	f, full, r := newReadyFixture(t, 110)
	narrow := full
	narrow.from = []string{syncD1}
	narrow.windows = syncRanges{{"old-gap", syncTestNow - 7200, syncTestNow - 3600}}
	narrow.fullHistory = false
	narrow.sampleEvery = 0
	if err := runSync(context.Background(), narrow, f); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(narrow.state, "sync.jsonl")
	before, err := os.ReadFile(journal)
	if err != nil {
		t.Fatal(err)
	}
	// A hole visible only on another donor and outside the old window must be
	// repaired by the new phase, not hidden behind old copy receipts.
	f.put(syncD2, "other.only", []syncArchive{{3600, 24 * 10}})
	r.proofBlock = writeReadyBlock(t, full, r, true)
	if err := runSyncReadyPrepared(context.Background(), &narrow, r, f, nil, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	report := readReadyReport(t, narrow.state)
	if report.Verdict != "READY" || len(report.Sources) != 3 || !strings.Contains(report.ProofState, "ready-proof-") {
		t.Fatalf("bad proof %+v", report)
	}
	if _, ok := f.store(syncT).archives["other.only"]; !ok {
		t.Fatal("other-donor history was not copied")
	}
	after, _ := os.ReadFile(journal)
	if string(before) != string(after) {
		t.Fatal("original copy journal modified")
	}
	if narrow.fullHistory || narrow.sampleEvery != 0 || len(narrow.from) != 1 {
		t.Fatal("copy scope changed")
	}
	proof, _, err := loadReadyProof(narrow, r)
	if err != nil {
		t.Fatal(err)
	}
	if !proof.fullHistory || proof.sampleEvery != 1 || !proof.includeShort {
		t.Fatal("proof not exhaustive")
	}
}

func TestIndependentReadyProofKeepsQuarantineAndGapGate(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		t.Run(map[bool]string{false: "open-ledger", true: "accepted-exception"}[complete], func(t *testing.T) {
			f, o, r := newReadyFixture(t, 110)
			var err error
			o.issues, err = openSyncIssues(o.state)
			if err != nil {
				t.Fatal(err)
			}
			if err = o.issues.record([]syncResidual{{syncHole: syncHole{Name: "metric.000"}, Reason: "nonfinite_target_conflict"}}, "fill", false); err != nil {
				t.Fatal(err)
			}
			if err = o.issues.checkpoint(); err != nil {
				t.Fatal(err)
			}
			q := filepath.Join(o.state, "quarantine.json")
			before, _ := os.ReadFile(q)
			r.proofBlock = writeReadyBlock(t, o, r, complete)
			err = runSyncReadyPrepared(context.Background(), &o, r, f, nil, func() error { return ErrSyncQuarantined })
			report := readReadyReport(t, o.state)
			if complete {
				if !errors.Is(err, ErrSyncQuarantined) || report.Verdict != "READY_EXCEPT" || report.Quarantined != 1 {
					t.Fatalf("%+v %v", report, err)
				}
			} else {
				if !errors.Is(err, ErrSyncNotReady) || report.Verdict != "NOT_READY" || !strings.Contains(strings.Join(report.Reasons, " "), "gap-ledger-complete") {
					t.Fatalf("%+v %v", report, err)
				}
			}
			after, _ := os.ReadFile(q)
			if string(before) != string(after) {
				t.Fatal("accepted exception ledger changed")
			}
		})
	}
}

func TestIndependentReadyScopeIsSavedAndBound(t *testing.T) {
	t.Parallel()
	c, err := parseSyncCommand(syncCLIArgs(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	_, full, r := newReadyFixture(t, 100)
	full.to = c.sync.to
	path := writeReadyBlock(t, full, r, false)
	resumed, err := parseSyncCommand([]string{"--resume", c.sync.state, "--ready-block", path})
	if err != nil {
		t.Fatal(err)
	}
	identity, _ := syncIdentity(c.sync)
	newIdentity, _ := syncIdentity(resumed.sync)
	if identity != newIdentity {
		t.Fatal("READY override changed copy identity")
	}
	if err = resumed.saveConfig(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	again, err := parseSyncCommand([]string{"--resume", c.sync.state})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := loadReadyProof(again.sync, again.ready)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(again.ready.proofBlock)
	var block syncBlock
	if err = json.Unmarshal(data, &block); err != nil {
		t.Fatal(err)
	}
	block.Donors = block.Donors[:1]
	data, _ = json.Marshal(block)
	if err = os.WriteFile(again.ready.proofBlock, data, 0600); err != nil {
		t.Fatal(err)
	}
	second, _, err := loadReadyProof(again.sync, again.ready)
	if err != nil {
		t.Fatal(err)
	}
	if first.state == second.state {
		t.Fatal("changed scope reused old proof journal")
	}
	block.Target = syncD2
	data, _ = json.Marshal(block)
	_ = os.WriteFile(again.ready.proofBlock, data, 0600)
	if _, _, err = loadReadyProof(again.sync, again.ready); err == nil {
		t.Fatal("different proof target accepted")
	}
	if !reflect.DeepEqual(c.sync.from, resumed.sync.from) {
		t.Fatal("copy donor scope changed")
	}
}

func TestIndependentReadyProofPublishesNestedProgress(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	p := &progressMetrics{paths: map[string]bool{filepath.Join(root, "progress.json"): true}}
	proof := filepath.Join(root, "ready-proof-abc")
	p.observe(filepath.Join(proof, "progress.json"), progressSnapshot{Phase: "verify", Done: 20, Total: 100, Status: "running"})
	p.observe(filepath.Join(proof, "ready-recovery-123-0", "progress.json"), progressSnapshot{Phase: "verify recovery", Done: 30, Total: 100, Status: "running"})
	p.observe(filepath.Join(proof, "copy-batch", "progress.json"), progressSnapshot{Phase: "private copy", Done: 999, Status: "running"})
	if p.phase != "verify_recovery" && p.phase != "verify-recovery" && p.phase != "verify recovery" {
		t.Fatalf("proof phase hidden: %+v", p)
	}
	if p.done != 30 {
		t.Fatalf("nested copy replaced READY proof progress: %+v", p)
	}
}
