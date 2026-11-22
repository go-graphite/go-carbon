package downsize

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type legacyRoundBackend struct {
	*proofFixture
	mu    sync.Mutex
	reads map[string]int
}

func (b *legacyRoundBackend) record(names []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, n := range names {
		b.reads[n]++
	}
}
func (b *legacyRoundBackend) Info(c context.Context, h string, n []string) (map[string][]syncArchive, error) {
	b.record(n)
	return b.proofFixture.Info(c, h, n)
}
func (b *legacyRoundBackend) Digest(c context.Context, h string, n []string, a, z int64) (map[string][]syncDigest, error) {
	b.record(n)
	return b.proofFixture.Digest(c, h, n, a, z)
}
func (b *legacyRoundBackend) Fetch(c context.Context, h string, n []string, a, z int64) (map[string]syncSeries, error) {
	b.record(n)
	return b.proofFixture.Fetch(c, h, n, a, z)
}
func (b *legacyRoundBackend) Proofs(c context.Context, h string, n []string) ([]string, error) {
	b.record(n)
	return b.proofFixture.Proofs(c, h, n)
}

func TestSyncLegacyEnteredRoundAdoption(t *testing.T) {
	for _, round := range []int{1, 2, 4} {
		for _, upgraded := range []bool{false, true} {
			t.Run(fmt.Sprintf("round%d/upgraded=%v", round, upgraded), func(t *testing.T) {
				s, j := phaseResumeFixture(t)
				s.o.rescanAll = false
				s.o.maxRounds = 0
				s.o.noCopy = false
				s.o.sourceScope = true
				s.o.batch = 3
				f, _ := frontierFixture(t)
				s.b = f
				// 18/20 completed, like db's 172.5M/178.8M. Target-only names
				// interleaved with donor names caused the new default to shrink this input.
				var union strings.Builder
				for i := 0; i < 20; i++ {
					mask := 1
					if i%3 == 0 {
						mask = 0
					}
					fmt.Fprintf(&union, "metric.%02d\t%d\t1\n", i, mask)
				}
				if err := os.WriteFile(s.unionPath(), []byte(union.String()), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(s.missingPath(), nil, 0600); err != nil {
					t.Fatal(err)
				}
				s.sum.Discovered = 20
				s.sum.OnTarget = 20
				s.both = 13
				if err := j.append(syncRow{Kind: "discovered", Names: 20, OnTarget: 20, Both: 13}); err != nil {
					t.Fatal(err)
				}
				if err := s.openFrontier(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := s.frontier.file.Close(); err != nil {
					t.Fatal(err)
				}
				s.frontier = nil
				// Original receipts have a different batch size and include dirty results.
				hole := syncHole{Name: "metric.01", Source: syncD1, Step: 3600, Points: 2}
				skip := syncSkip{"metric.02", "old read failed"}
				for i := 0; i < 18; i += 6 {
					r := syncRow{Kind: "verified", Round: round, First: fmt.Sprintf("metric.%02d", i), Last: fmt.Sprintf("metric.%02d", i+5), Names: 6, MutationObserved: true, ObservationProof: "old fence"}
					if i == 0 {
						r.Holes = []syncHole{hole}
						r.Skipped = []syncSkip{skip}
						r.Unindexed = []string{"metric.03"}
					}
					if err := j.append(r); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.savePhase("verify", round, 18, 20); err != nil {
					t.Fatal(err)
				}
				if upgraded {
					for _, r := range []syncRow{
						{Kind: "verification-round-scope", Round: round, Phase: "union"},
						{Kind: "ambiguous-receipt", Round: round, Names: 10, Selected: []string{"metric.01"}},
						{Kind: "receipt-reverified", Round: round, First: "metric.00", Last: "metric.04", Names: 5, Selected: []string{"metric.00", "metric.01", "metric.02", "metric.03", "metric.04"}},
						// A partial reread must neither erase old dirty observations nor double count.
						{Kind: "verified", Round: round, First: "metric.00", Last: "metric.04", Names: 5},
						{Kind: "sync-replay-skipped", Round: round},
						{Kind: "verified", Round: round, First: "metric.17", Last: "metric.19", Names: 2},
					} {
						if err := j.append(r); err != nil {
							t.Fatal(err)
						}
					}
					if err := s.savePhase("verify", round, 18, 13); err != nil {
						t.Fatal(err)
					}
				}
				if err := atomicStateJSON(filepath.Join(s.o.state, "progress.json"), progressSnapshot{Phase: fmt.Sprintf("verify round %d", round), Round: round, Done: 18, Total: 20, Cursors: map[string]string{syncT: "metric.19", syncD1: "metric.17"}}); err != nil {
					t.Fatal(err)
				}
				before, err := os.ReadFile(j.file.Name())
				if err != nil {
					t.Fatal(err)
				}
				s = restartPhaseFixture(t, s)
				if err := s.restorePhase(); err != nil {
					t.Fatal(err)
				}
				b := &legacyRoundBackend{proofFixture: f, reads: map[string]int{}}
				s.b = b
				if !s.adoptingLegacyRound() {
					t.Fatal("lost legacy provenance")
				}
				if s.unionSource().total != 20 {
					t.Fatal("legacy scope shrank")
				}
				// Test the actual union verify path, then reopen a spilled checkpoint.
				var suffixReads int
				for attempt := 0; attempt < 2; attempt++ {
					holes, unindexed, skipped, err := s.verifyRound(context.Background(), round, s.unionSource())
					if err != nil {
						t.Fatal(err)
					}
					found := false
					for _, h := range holes {
						if reflect.DeepEqual(h, hole) {
							found = true
						}
					}
					if !found || !reflect.DeepEqual(unindexed, []string{"metric.03"}) || !reflect.DeepEqual(skipped, []syncSkip{skip}) {
						t.Fatalf("lost observations: %v %v %v", holes, unindexed, skipped)
					}
					if s.prog.done.Load() != 20 || s.prog.total != 20 {
						t.Fatalf("progress=%d/%d", s.prog.done.Load(), s.prog.total)
					}
					for i := 0; i < 18; i++ {
						n := fmt.Sprintf("metric.%02d", i)
						if b.reads[n] != 0 {
							t.Fatalf("completed name reread: %s (%d)", n, b.reads[n])
						}
					}
					if b.reads["metric.18"] == 0 || b.reads["metric.19"] == 0 {
						t.Fatal("pending suffix was skipped")
					}
					if s.frontier != nil {
						s.frontier.file.Close()
						s.frontier = nil
					}
					if attempt == 1 && b.reads["metric.19"] != suffixReads {
						t.Fatal("adopted binary's suffix reread after restart")
					}
					if attempt == 0 {
						suffixReads = b.reads["metric.19"]
						s.startVerificationCheckpoint()
						if s.j.checkpoint == nil {
							t.Fatal("no checkpoint")
						}
						if err := s.j.checkpointVerificationBudget(512); err != nil {
							t.Fatal(err)
						}
						s = restartPhaseFixture(t, s)
						s.b = b
						if err := s.restorePhase(); err != nil {
							t.Fatal(err)
						}
					}
				}
				after, err := os.ReadFile(s.j.file.Name())
				if err != nil || !bytes.HasPrefix(after, before) {
					t.Fatal("journal prefix changed", err)
				}
				// Existing recovery rows remain byte-for-byte; adoption adds no new plans.
				count := 0
				for _, r := range s.j.rows {
					if r.Kind == "ambiguous-receipt" {
						count++
					}
				}
				want := 0
				if upgraded {
					want = 1
				}
				if count != want {
					t.Fatalf("new ambiguity plans: %d", count)
				}
			})
		}
	}
}

func TestSyncLegacyRoundAdoptionBoundaries(t *testing.T) {
	for _, mode := range []string{"legacy", "modern", "sparse", "rescan", "ready", "window"} {
		t.Run(mode, func(t *testing.T) {
			s, j := phaseResumeFixture(t)
			s.o.rescanAll = false
			s.resume = syncPhasePosition{Phase: "verify", Round: 1}
			if mode == "modern" {
				j.append(syncRow{Kind: "verification-round-scope", Round: 1, Phase: "union"})
			}
			// Deliberately overlapping/out-of-order ranges, and an unreceipted gap.
			for _, r := range []syncRow{
				{Kind: "verified", Round: 1, First: "d", Last: "f", Names: 3, Skipped: []syncSkip{{"e", "original"}}},
				{Kind: "verified", Round: 1, First: "a", Last: "b", Names: 2},
				{Kind: "verified", Round: 1, First: "e", Last: "h", Names: 4, Skipped: []syncSkip{{"e", "new"}, {"g", "new suffix"}}},
			} {
				if err := j.append(r); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "sparse":
				j.append(syncRow{Kind: "verification-round-scope", Round: 1, Phase: "repairs"})
			case "rescan":
				s.o.rescanAll = true
			case "ready":
				s.o.readyFrontier = true
			case "window":
				s.o.fullHistory = false
				s.o.windows = syncRanges{{Name: "window", From: 1, Until: 2}}
			}
			p, err := s.legacyRoundProgress(1)
			if err != nil {
				t.Fatal(err)
			}
			if mode != "legacy" {
				if p != nil {
					t.Fatal("adopted outside legacy union")
				}
				return
			}
			for _, n := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
				got := p.contains(n)
				want := n != "c" && n != "i"
				if got != want {
					t.Fatalf("%s=%v", n, got)
				}
			}
			var skipped []syncSkip
			p.observations(func(r syncRow) { skipped = append(skipped, r.Skipped...) })
			if !reflect.DeepEqual(skipped, []syncSkip{{"e", "original"}, {"g", "new suffix"}}) {
				t.Fatalf("overlap lost original: %v", skipped)
			}
		})
	}
}

// The near-complete round-1 shape is deliberately larger than a worker wave:
// 1725/1788 models 172.5M/178.8M without reading any of its completed prefix.
func TestSyncLegacyRoundOneNearComplete(t *testing.T) {
	s, j := phaseResumeFixture(t)
	s.o.rescanAll = false
	s.o.sourceScope = true
	s.o.batch = 17
	f, _ := frontierFixture(t)
	b := &legacyRoundBackend{proofFixture: f, reads: map[string]int{}}
	s.b = b
	var union strings.Builder
	for i := 0; i < 1788; i++ {
		mask := 1
		if i%3 == 0 {
			mask = 0
		}
		fmt.Fprintf(&union, "scaled.%04d\t%d\t1\n", i, mask)
	}
	if err := os.WriteFile(s.unionPath(), []byte(union.String()), 0600); err != nil {
		t.Fatal(err)
	}
	s.sum.Discovered = 1788
	s.sum.OnTarget = 1788
	s.both = 1192
	for _, r := range []syncRow{{Kind: "discovered", Names: 1788, OnTarget: 1788, Both: 1192}, {Kind: "verified", Round: 1, First: "scaled.0000", Last: "scaled.1724", Names: 1725}} {
		if err := j.append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.savePhase("verify", 1, 1725, 1788); err != nil {
		t.Fatal(err)
	}
	s = restartPhaseFixture(t, s)
	s.b = b
	if err := s.restorePhase(); err != nil {
		t.Fatal(err)
	}
	// No verification-scope file: retain the old no-frontier mode too.
	if _, _, _, err := s.verifyRound(context.Background(), 1, s.unionSource()); err != nil {
		t.Fatal(err)
	}
	if s.prog.done.Load() != 1788 || s.prog.total != 1788 {
		t.Fatalf("progress=%d/%d", s.prog.done.Load(), s.prog.total)
	}
	for n := range b.reads {
		if n < "scaled.1725" {
			t.Fatalf("completed prefix read: %s", n)
		}
	}
	for i := 1725; i < 1788; i++ {
		if b.reads[fmt.Sprintf("scaled.%04d", i)] == 0 {
			t.Fatalf("pending name %d skipped", i)
		}
	}
}
