package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSyncLegacyUnionToRepair(t *testing.T) {
	for _, round := range []int{2, 4} {
		for _, rescan := range []bool{false, true} {
			for _, restart := range []bool{false, true} {
				t.Run(fmt.Sprintf("round%d/rescan=%v/restart=%v", round, rescan, restart), func(t *testing.T) {
					s, j := phaseResumeFixture(t)
					s.o.rescanAll = rescan
					s.o.noCopy = false
					s.o.maxRounds = 2
					f, _ := frontierFixture(t)
					b := &legacyRoundBackend{proofFixture: f, reads: map[string]int{}}
					s.b = b
					var union bytes.Buffer
					for i := 0; i < 20; i++ {
						fmt.Fprintf(&union, "metric.%02d\t1\t1\n", i)
					}
					if err := os.WriteFile(s.unionPath(), union.Bytes(), 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(s.missingPath(), nil, 0600); err != nil {
						t.Fatal(err)
					}
					s.sum.Discovered, s.sum.OnTarget, s.both = 20, 20, 20
					if err := s.openFrontier(context.Background()); err != nil {
						t.Fatal(err)
					}
					s.frontier.file.Close()
					s.frontier = nil
					hole := syncHole{Name: "metric.19", Source: syncD1, Step: 3600, Points: 1}
					for _, r := range []syncRow{
						{Kind: "discovered", Names: 20, OnTarget: 20, Both: 20},
						{Kind: "verified", Round: round - 1, First: "metric.00", Last: "metric.19", Names: 20, Holes: []syncHole{hole}, Skipped: []syncSkip{{"metric.16", "previous failure"}, {"metric.17", "previous failure"}}, Unindexed: []string{"metric.18"}},
						{Kind: "copied", Round: round - 1, Phase: "fill", Source: syncD1, Names: 2, Copied: 2, Selected: []string{"metric.15", "metric.19"}},
						{Kind: "verified", Round: round, First: "metric.00", Last: "metric.04", Names: 5},
						{Kind: "verified", Round: round, First: "metric.17", Last: "metric.17", Names: 1, Skipped: []syncSkip{{"metric.17", "old failure"}}},
						{Kind: "verification-round-scope", Round: round, Phase: "union"},
						{Kind: "legacy-round-adoption", Round: round},
						{Kind: "verified", Round: round, First: "metric.17", Last: "metric.17", Names: 1},
					} {
						if err := j.append(r); err != nil {
							t.Fatal(err)
						}
					}
					if err := s.savePhase("verify", round, 6, 20); err != nil {
						t.Fatal(err)
					}
					prefix, err := os.ReadFile(j.file.Name())
					if err != nil {
						t.Fatal(err)
					}
					s = restartPhaseFixture(t, s)
					s.b = b
					if err := s.restorePhase(); err != nil {
						t.Fatal(err)
					}
					if restart && !rescan {
						if _, switched, err := s.prepareLegacyRepair(nil, false, map[syncKey]bool{}); err != nil || !switched {
							t.Fatalf("switch: %v %v", switched, err)
						}
						// Kill after the transition, with a stale union progress snapshot.
						if err := atomicStateJSON(filepath.Join(s.o.state, "progress.json"), progressSnapshot{Phase: fmt.Sprintf("verify round %d", round), Round: round, Done: 6, Total: 20}); err != nil {
							t.Fatal(err)
						}
						s.startVerificationCheckpoint()
						if err := s.j.checkpointVerificationBudget(512); err != nil {
							t.Fatal(err)
						}
						s = restartPhaseFixture(t, s)
						s.b = b
						if err := s.restorePhase(); err != nil {
							t.Fatal(err)
						}
						if s.resume.Done != 0 || s.resume.Total != 5 {
							t.Fatalf("restored union counters: %+v", s.resume)
						}
					}
					// Exercise the real runner, including terminal residual.json emission.
					s.j.close()
					b.reads = map[string]int{}
					if err := runSync(context.Background(), s.o, b); !errors.Is(err, ErrSyncResidual) {
						t.Fatalf("want residual report: %v", err)
					}
					after, err := os.ReadFile(filepath.Join(s.o.state, "sync.jsonl"))
					if err != nil || !bytes.HasPrefix(after, prefix) {
						t.Fatal("journal prefix changed", err)
					}
					var residual []syncResidual
					data, err := os.ReadFile(filepath.Join(s.o.state, "residual.json"))
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(data, &residual); err != nil || len(residual) == 0 {
						t.Fatalf("residual report: %s %v", data, err)
					}
					if !rescan {
						wanted := map[string]bool{"metric.15": true, "metric.16": true, "metric.18": true, "metric.19": true}
						for n := range b.reads {
							if !wanted[n] {
								t.Fatalf("read outside pending repairs: %s", n)
							}
						}
						for n := range wanted {
							if b.reads[n] == 0 {
								t.Fatalf("repair not read: %s", n)
							}
						}
						for _, r := range residual {
							if r.Name == "metric.17" {
								t.Fatal("fresh observation lost")
							}
						}
						if bytes.Count(after, []byte(`"kind":"legacy-round-repair"`)) != 1 {
							t.Fatal("transition missing or duplicated")
						}
					} else {
						if b.reads["metric.05"] == 0 || b.reads["metric.00"] == 0 {
							t.Fatal("explicit rescan did not retain union")
						}
						if bytes.Contains(after, []byte(`"kind":"legacy-round-repair"`)) {
							t.Fatal("rescan switched")
						}
					}
					if len(readRoundRows(t, s.o.state, round+1)) != 0 {
						t.Fatal("continued past round limit")
					}
				})
			}
		}
	}
}

// A completed sparse batch must never claim an adopted dirty name between its
// endpoints. Reopening after that batch must retain the dirty observation.
func TestSyncLegacyRepairSparseReceiptRestart(t *testing.T) {
	s, j := phaseResumeFixture(t)
	for _, r := range []syncRow{
		{Kind: "verified", Round: 2, First: "b", Last: "b", Names: 1, Skipped: []syncSkip{{"b", "unavailable"}}},
		{Kind: "verification-round-scope", Round: 2, Phase: "union"},
		{Kind: "legacy-round-adoption", Round: 2},
		{Kind: "verified", Round: 2, First: "a", Last: "c", Names: 2, Selected: []string{"a", "c"}, SelectionComplete: true},
	} {
		if err := j.append(r); err != nil {
			t.Fatal(err)
		}
	}
	s = restartPhaseFixture(t, s)
	p, err := s.legacyRoundReceipts(2, true, false)
	if err != nil {
		t.Fatal(err)
	}
	p = p.restrict([]string{"a", "b", "c"})
	var skips []syncSkip
	p.observations(func(r syncRow) { skips = append(skips, r.Skipped...) })
	if len(skips) != 1 || skips[0].Name != "b" {
		t.Fatalf("lost adopted dirty observation: %v", skips)
	}
}

func TestSyncLegacyRepairPartialRestart(t *testing.T) {
	s, j := phaseResumeFixture(t)
	s.o.rescanAll = false
	s.resume = syncPhasePosition{Phase: "verify", Round: 2}
	p := syncPhasePosition{Phase: "verify", Round: 2, Total: 3, Options: syncCheckpointOptions(s.o)}
	for _, r := range []syncRow{
		{Kind: "verified", Round: 1, First: "a", Last: "c", Names: 3},
		{Kind: "verification-round-scope", Round: 2, Phase: "union"},
		{Kind: "legacy-round-adoption", Round: 2},
		{Kind: "verified", Round: 2, First: "a", Last: "a", Names: 1, Selected: []string{"a"}, SelectionComplete: true},
		{Kind: "legacy-round-repair", Round: 2, Selected: []string{"a", "b", "c"}, SelectionComplete: true, Position: &p},
		{Kind: "verified", Round: 2, First: "c", Last: "c", Names: 1, Selected: []string{"c"}, SelectionComplete: true},
	} {
		if err := j.append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.savePhase("verify", 2, 2, 3); err != nil {
		t.Fatal(err)
	}
	s = restartPhaseFixture(t, s)
	if err := s.restorePhase(); err != nil {
		t.Fatal(err)
	}
	if !s.adoptingLegacyRound() {
		t.Fatal("lost transition provenance after first sparse receipt")
	}
	if _, switched, err := s.prepareLegacyRepair(nil, false, map[syncKey]bool{}); err != nil || !switched {
		t.Fatalf("switch: %v %v", switched, err)
	}
	if err := s.beginPhase("verify", 2, 3); err != nil {
		t.Fatal(err)
	}
	if s.prog.durableFloor != 2 {
		t.Fatalf("lost repair progress floor: %d", s.prog.durableFloor)
	}
	adopted, err := s.legacyRoundReceipts(2, true, false)
	if err != nil {
		t.Fatal(err)
	}
	adopted = adopted.restrict([]string{"a", "b", "c"})
	if !adopted.contains("a") || adopted.contains("b") || !adopted.contains("c") {
		t.Fatal("partial repair progress lost")
	}
}
