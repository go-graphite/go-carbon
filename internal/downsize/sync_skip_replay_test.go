package downsize

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

type skipReplayBackend struct {
	*proofFixture
	s           *syncer
	mu          sync.Mutex
	reads       map[string]int
	floor       int64
	firstRead   bool
	skipCurrent bool
}

func (b *skipReplayBackend) Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	b.mu.Lock()
	if !b.firstRead {
		b.floor, b.firstRead = b.s.prog.durableFloor, true
	}
	b.mu.Unlock()
	if b.skipCurrent && names[0] == "metric.18" {
		return nil, context.DeadlineExceeded
	}
	return b.fakeSync.Info(ctx, store, names)
}

func (b *skipReplayBackend) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	b.mu.Lock()
	for _, n := range names {
		b.reads[n]++
	}
	b.mu.Unlock()
	return b.proofFixture.Digest(ctx, store, names, from, until)
}

func TestSyncResumeSkipCompletedReplay(t *testing.T) {
	for _, tc := range []struct {
		name           string
		round, limit   int
		rescan, replay bool
		sparse, fill   bool
	}{
		{name: "legacy-round4", round: 4},
		{name: "legacy-round2", round: 2},
		{name: "rescan-round2", round: 2, limit: 2, rescan: true},
		{name: "below-limit", round: 2, limit: 3, replay: true},
		{name: "unlimited-rescan", round: 2, rescan: true, replay: true},
		{name: "sparse-at-limit", round: 2, sparse: true, replay: true},
		{name: "fill-at-limit", round: 2, fill: true, replay: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, j := phaseResumeFixture(t)
			s.o.rescanAll, s.o.maxRounds, s.o.noCopy = tc.rescan, tc.limit, false
			f, _ := frontierFixture(t)
			if tc.replay {
				// Completed fills are empty in replay controls. All names are clean.
				f.store(syncT).points["metric.19"][3600][syncTestNow-5*3600] = f.store(syncD1).points["metric.19"][3600][syncTestNow-5*3600]
			}
			s.b = f
			var union strings.Builder
			var names []string
			for i := range 20 {
				n := fmt.Sprintf("metric.%02d", i)
				names = append(names, n)
				fmt.Fprintf(&union, "%s\t1\t1\n", n)
			}
			if err := os.WriteFile(s.unionPath(), []byte(union.String()), 0600); err != nil {
				t.Fatal(err)
			}
			s.sum.Discovered = len(names)
			// Produce reusable observations for the first seven names of the
			// current union, then model a kill with its durable counter at seven.
			set, err := s.setSource(names[:7])
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := s.verifyRound(context.Background(), tc.round, set); err != nil {
				t.Fatal(err)
			}
			if err := s.frontier.file.Close(); err != nil {
				t.Fatal(err)
			}
			for round := 1; round < tc.round; round++ {
				// Ambiguous membership would require live reads on replay. Stale
				// holes/skips must not leak into a terminal union's residuals.
				if err := j.append(syncRow{Kind: "verified", Round: round, First: names[0], Last: names[19], Names: 1,
					Holes: []syncHole{{Name: names[0], Source: syncD1, Points: 1}}, Skipped: []syncSkip{{names[1], "old skip"}}}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.sparse {
				if err := j.append(syncRow{Kind: "verification-round-scope", Round: tc.round, Phase: "repairs"}); err != nil {
					t.Fatal(err)
				}
			}
			phase := "verify"
			if tc.fill {
				phase = "fill"
			}
			if err := s.savePhase(phase, tc.round, 7, len(names)); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(j.file.Name())
			if err != nil {
				t.Fatal(err)
			}
			s = restartPhaseFixture(t, s)
			s.sum.Discovered = len(names)
			b := &skipReplayBackend{proofFixture: f, s: s, reads: map[string]int{}, skipCurrent: !tc.replay}
			s.b = b
			if err := s.restorePhase(); err != nil {
				t.Fatal(err)
			}
			if err := s.verifyAndFill(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !tc.rescan && !tc.sparse && !tc.fill {
				// Both previous residual names already have clean current-round
				// observations. Switching scopes needs no further store reads.
				transition := s.legacyRepairScope(tc.round)
				if transition == nil || len(transition.Selected) != 2 || b.firstRead || len(b.reads) != 0 || len(f.copies) != 0 || len(s.sum.residual) != 0 || s.sum.Rounds != tc.round {
					t.Fatalf("repair scope did not reuse clean observations: transition=%+v reads=%v residual=%v", transition, b.reads, s.sum.residual)
				}
				after, err := os.ReadFile(s.j.file.Name())
				if err != nil || !bytes.HasPrefix(after, before) {
					t.Fatalf("existing journal changed: %v", err)
				}
				return
			}
			if !b.firstRead || b.floor != 7 {
				t.Fatalf("saved counter lost: first read=%v floor=%d", b.firstRead, b.floor)
			}
			if got := b.reads[names[0]] > 0; got != tc.replay {
				t.Fatalf("earlier name read=%v, want replay=%v; reads=%v", got, tc.replay, b.reads)
			}
			var repaired bool
			for _, row := range s.j.rows {
				if row.Kind == "ambiguous-receipt" {
					repaired = true
				}
			}
			if repaired != tc.replay {
				t.Fatalf("receipt replay=%v, want %v", repaired, tc.replay)
			}
			if !tc.replay {
				if s.sum.Rounds != tc.round || len(f.copies) != 0 {
					t.Fatalf("round=%d copies=%v", s.sum.Rounds, f.copies)
				}
				if len(s.sum.residual) != 2 || s.sum.residual[0].Name != names[19] || s.sum.residual[0].Reason != "round_limit" || s.sum.residual[1].Name != names[18] || !strings.Contains(s.sum.residual[1].Reason, "deadline") {
					t.Fatalf("residual=%+v", s.sum.residual)
				}
				for _, n := range names[:7] {
					if b.reads[n] != 0 {
						t.Fatalf("completed current-round name reread: %s", n)
					}
				}
			}
			after, err := os.ReadFile(s.j.file.Name())
			if err != nil || !bytes.HasPrefix(after, before) {
				t.Fatalf("existing journal changed: %v", err)
			}
		})
	}
}
