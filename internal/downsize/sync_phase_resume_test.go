package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func phaseResumeFixture(t *testing.T) (*syncer, *syncJournal) {
	t.Helper()
	o := convergeOptions(t)
	o.from = []string{syncD1}
	o.batch = 1
	identity, err := syncIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(j.close)
	s := &syncer{o: o, j: j, b: newFakeSync(), copied: map[string]bool{}, forbid: map[string]bool{}, failedCp: map[string]bool{}}
	s.sum.phases = map[string]*syncPhase{}
	return s, j
}

func TestSyncPhaseResumeEveryPosition(t *testing.T) {
	for _, phase := range []string{"discover", "copy", "window missing", "window retry", "verify", "fill", "complete"} {
		for _, complete := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/complete=%v", phase, complete), func(t *testing.T) {
				s, j := phaseResumeFixture(t)
				round := 0
				if phase == "verify" || phase == "fill" {
					round = 16
				}
				if phase == "verify" {
					if err := j.append(syncRow{Kind: "verified", Round: round, First: "a", Last: "g", Names: 7}); err != nil {
						t.Fatal(err)
					}
				}
				p := syncPhasePosition{Phase: phase, Round: round, Done: 7, Total: 11, Complete: complete, Options: syncCheckpointOptions(s.o)}
				if err := j.append(syncRow{Kind: "phase-position", Position: &p}); err != nil {
					t.Fatal(err)
				}
				// Model a kill, with a stale progress file from before the transition.
				if err := atomicStateJSON(filepath.Join(s.o.state, "progress.json"), progressSnapshot{Phase: "discover", Done: 1, Total: 11}); err != nil {
					t.Fatal(err)
				}
				// Drop every in-memory field as a killed client would. Reopen the
				// actual journal and lock, rather than testing the live writer's rows.
				s = restartPhaseFixture(t, s)
				if err := s.restorePhase(); err != nil {
					t.Fatal(err)
				}
				got, _ := json.Marshal(s.resume)
				want, _ := json.Marshal(p)
				if string(got) != string(want) {
					t.Fatalf("position lost: %s != %s", got, want)
				}
				if s.prog.durableFloor != 7 {
					t.Fatalf("initial counter reset: %d", s.prog.durableFloor)
				}
				if !complete {
					if err := s.beginPhase(phase, round, 11); err != nil {
						t.Fatal(err)
					}
					if s.prog.durableFloor != 7 {
						t.Fatal("phase entry lost saved count")
					}
				}
			})
		}
	}
}

func restartPhaseFixture(t *testing.T, prior *syncer) *syncer {
	t.Helper()
	prior.j.file.Close()
	prior.j.lock.Close()
	identity, err := syncIdentity(prior.o)
	if err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(prior.o.state, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(j.close)
	s := &syncer{o: prior.o, j: j, b: newFakeSync(), copied: map[string]bool{}, forbid: map[string]bool{}, failedCp: map[string]bool{}}
	s.sum.phases = map[string]*syncPhase{}
	return s
}

func TestSyncPhaseResumeLegacyProgressFloor(t *testing.T) {
	for _, label := range []string{"copy", "window missing", "window retry", "verify round 16", "fill"} {
		t.Run(label, func(t *testing.T) {
			s, j := phaseResumeFixture(t)
			if err := j.append(syncRow{Kind: "discovered", Names: 271248522, Missing: 137041866}); err != nil {
				t.Fatal(err)
			}
			if label == "verify round 16" {
				if err := j.append(syncRow{Kind: "verified", Round: 16, First: "a", Last: "z", Names: 146847}); err != nil {
					t.Fatal(err)
				}
			}
			if err := atomicStateJSON(filepath.Join(s.o.state, "progress.json"), progressSnapshot{Phase: label, Round: 16, Done: 146847, Total: 43830096}); err != nil {
				t.Fatal(err)
			}
			if err := s.restorePhase(); err != nil {
				t.Fatal(err)
			}
			if s.prog.phase != label || s.prog.durableFloor != 146847 || s.prog.total != 43830096 {
				t.Fatalf("legacy startup reset %+v", s.resume)
			}
		})
	}
}

type phaseResumeRetryBackend struct {
	*fakeSync
	t      *testing.T
	cancel context.CancelFunc
	state  string
	infos  int
}

func (b *phaseResumeRetryBackend) List(context.Context, string, string, string, int) ([]string, error) {
	return nil, errors.New("completed discovery called backend")
}
func (b *phaseResumeRetryBackend) WindowPresenceRanges(ctx context.Context, store string, names []string, _ syncRanges) (map[string][]syncDigest, error) {
	_, err := b.Info(ctx, store, names)
	return nil, err
}
func (b *phaseResumeRetryBackend) Info(context.Context, string, []string) (map[string][]syncArchive, error) {
	b.infos++
	data, err := os.ReadFile(filepath.Join(b.state, "progress.json"))
	if err != nil {
		b.t.Fatal(err)
	}
	var p progressSnapshot
	if err = json.Unmarshal(data, &p); err != nil {
		b.t.Fatal(err)
	}
	if p.Phase != "window retry" || p.Done < 7 {
		b.t.Errorf("first published progress regressed: %+v", p)
	}
	b.cancel()
	return nil, context.Canceled
}

func TestSyncPhaseResumePendingWindowSkipsMissing(t *testing.T) {
	s, j := phaseResumeFixture(t)
	s.o.windows = syncRanges{{Name: "gap", From: syncTestNow - 600, Until: syncTestNow - 120}}
	s.o.fullHistory = false
	// Recreate header with the real window identity before adding any work.
	identity, _ := syncIdentity(s.o)
	j.rows[0].Identity = identity
	if err := j.file.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if _, err := j.file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(j.rows[0])
	if _, err := j.file.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := j.append(syncRow{Kind: "discovered", Names: 20, Missing: 10}); err != nil {
		t.Fatal(err)
	}
	row := syncRow{Kind: "window-selected", Source: syncD1, Round: 1, SelectionComplete: true}
	for i := 0; i < 10; i++ {
		row.Skipped = append(row.Skipped, syncSkip{fmt.Sprintf("pending.%02d", i), syncWindowSelectionReason})
	}
	if err := j.append(row); err != nil {
		t.Fatal(err)
	}
	if err := atomicStateJSON(filepath.Join(s.o.state, "progress.json"), progressSnapshot{Phase: "window retry", Done: 7, Total: 10}); err != nil {
		t.Fatal(err)
	}
	// No missing.tsv exists: entering completed missing copy fails this test even
	// before it could issue a selection or transfer.
	j.file.Close()
	j.lock.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &phaseResumeRetryBackend{fakeSync: newFakeSync(), t: t, cancel: cancel, state: s.o.state}
	if err := runSync(ctx, s.o, b); !errors.Is(err, context.Canceled) {
		t.Fatalf("wanted current retry cancellation: %v", err)
	}
	if b.infos == 0 {
		t.Fatal("did not enter the saved retry frontier")
	}
}

func TestSyncPhaseResumeCompletedRoundsUseOnlyReceipts(t *testing.T) {
	for _, phase := range []string{"verify", "fill"} {
		t.Run(phase, func(t *testing.T) {
			s, j := phaseResumeFixture(t)
			if err := os.WriteFile(s.unionPath(), []byte("a\t1\t1\n"), 0600); err != nil {
				t.Fatal(err)
			}
			s.sum.Discovered = 1
			for round := 1; round <= 16; round++ {
				if err := j.append(syncRow{Kind: "verified", Round: round, First: "a", Last: "a", Names: 1}); err != nil {
					t.Fatal(err)
				}
			}
			s.resume = syncPhasePosition{Phase: phase, Round: 16, Done: 1, Total: 1}
			s.prog.beginReplay(phase, 1, 1)
			for round := 1; round <= 15; round++ {
				if _, _, _, err := s.verifyRound(context.Background(), round, s.unionSource()); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "fill" {
				if _, _, _, err := s.verifyRound(context.Background(), 16, s.unionSource()); err != nil {
					t.Fatal(err)
				}
			}
			if s.prog.phase != phase || s.prog.durableFloor != 1 || s.prog.done.Load() != 0 {
				t.Fatal("completed rounds changed live position")
			}
		})
	}
}

func TestSyncPhaseResumeMissingReceiptIsSparse(t *testing.T) {
	s, j := phaseResumeFixture(t)
	s.sum.Discovered = 2
	if err := os.WriteFile(s.unionPath(), []byte("a\t1\t1\nb\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := j.append(syncRow{Kind: "verified", Round: 1, First: "a", Last: "a", Names: 1}); err != nil {
		t.Fatal(err)
	}
	s.resume = syncPhasePosition{Phase: "fill", Round: 1}
	if _, unknown, _, err := s.verifyRound(context.Background(), 1, s.unionSource()); err != nil {
		t.Fatalf("sparse legacy recovery: unknown=%v err=%v", unknown, err)
	}
	if j.progress.AmbiguousReceiptNames != 1 || j.progress.PhaseRegressions != 0 {
		t.Fatalf("bad recovery counters: %+v", j.progress)
	}
}

func TestSyncPhaseResumeScopeChangeFails(t *testing.T) {
	s, j := phaseResumeFixture(t)
	if err := s.savePhase("fill", 16, 7, 11); err != nil {
		t.Fatal(err)
	}
	s.o.settle++
	if err := s.restorePhase(); err == nil || !strings.Contains(err.Error(), "scope changed") {
		t.Fatal("changed scope silently reused", err)
	}
	if len(j.rows) != 2 {
		t.Fatal("scope error mutated journal")
	}
}

func TestSyncPhaseResumeJournalProgressSurvivesLostJSON(t *testing.T) {
	s, j := phaseResumeFixture(t)
	if err := s.savePhase("fill", 16, 0, 43830096); err != nil {
		t.Fatal(err)
	}
	p := syncPhasePosition{Phase: "fill", Round: 16, Done: 2840000, Total: 43830096}
	if err := j.append(syncRow{Kind: "phase-progress", Position: &p}); err != nil {
		t.Fatal(err)
	}
	// A delayed old-phase publisher cannot rewind the latest transition.
	stale := syncPhasePosition{Phase: "verify", Round: 16, Done: 99, Total: 100}
	if err := j.append(syncRow{Kind: "phase-progress", Position: &stale}); err == nil {
		t.Fatal("delayed old-phase publication was accepted")
	}
	if err := s.restorePhase(); err != nil {
		t.Fatal(err)
	}
	if s.resume.Phase != "fill" || s.resume.Round != 16 || s.resume.Done != 2840000 {
		t.Fatalf("lost journal-only position: %+v", s.resume)
	}
	if err := s.beginPhase("fill", 16, 43830096); err != nil {
		t.Fatal(err)
	}
	// Kill between writing phase entry and the next progress-file publication.
	s = restartPhaseFixture(t, s)
	if err := s.restorePhase(); err != nil {
		t.Fatal(err)
	}
	if s.resume.Done != 2840000 {
		t.Fatalf("phase entry crash lost counter: %+v", s.resume)
	}
}

func TestSyncPhaseResumeLegacyReceiptsWithoutProgress(t *testing.T) {
	for _, phase := range []string{"verify", "fill", "window retry"} {
		t.Run(phase, func(t *testing.T) {
			s, j := phaseResumeFixture(t)
			r := syncRow{Kind: "verified", Round: 16, First: "a", Last: "z", Names: 2840000}
			if phase == "fill" {
				r.Kind, r.Phase = "copied", "fill"
			}
			if phase == "window retry" {
				r.Kind, r.Names = "window-retry-floor", 146847
			}
			if err := j.append(r); err != nil {
				t.Fatal(err)
			}
			s = restartPhaseFixture(t, s)
			if err := s.restorePhase(); err != nil {
				t.Fatal(err)
			}
			if s.resume.Phase != phase || s.resume.Done != int64(r.Names) {
				t.Fatalf("lost legacy journal cursor: %+v", s.resume)
			}
		})
	}
}

func TestSyncPhaseResumeRejectsDurableRegression(t *testing.T) {
	for _, next := range []syncPhasePosition{{Phase: "copy", Done: 7}, {Phase: "fill", Round: 16, Done: 6}, {Phase: "verify", Round: 15, Done: 99}} {
		t.Run(fmt.Sprintf("%s/%d/%d", next.Phase, next.Round, next.Done), func(t *testing.T) {
			s, j := phaseResumeFixture(t)
			if err := s.savePhase("fill", 16, 7, 11); err != nil {
				t.Fatal(err)
			}
			s = restartPhaseFixture(t, s)
			j = s.j
			if err := j.append(syncRow{Kind: "phase-progress", Position: &next}); err == nil || !strings.Contains(err.Error(), "record 2 saved fill round 16 done 7") {
				t.Fatalf("regression was not refused against named record: %v", err)
			}
			if j.progress.PhaseRegressions != 1 {
				t.Fatal("missing regression metric", j.progress)
			}
			s = restartPhaseFixture(t, s)
			if err := s.restorePhase(); err != nil {
				t.Fatal(err)
			}
			if s.resume.Done != 7 || s.resume.Phase != "fill" || s.j.progress.PhaseRegressions != 1 {
				t.Fatal("denied publication changed durable position", s.resume)
			}
		})
	}
}

// A state saved while its requested window was still settling stores the
// window clipped at that run's horizon. A resume after the window settled
// recomputes the full window; that is the same scope, not a changed one.
func TestSyncPhaseResumeAfterWindowSettled(t *testing.T) {
	const until = int64(1_795_861_440)
	launched := time.Unix(until+5*60, 0) // 5 min after the window ends, inside the 10 min settle
	prelaunch := syncRanges{{Name: "prelaunch", From: until - 3600, Until: until}}
	nested := syncRanges{{Name: "outer", From: until - 7200, Until: until}, {Name: "inner", From: until - 600, Until: until}}
	for _, tc := range []struct {
		name    string
		windows syncRanges
		resumed time.Time
	}{
		{"settled", prelaunch, launched.Add(20 * time.Minute)},
		{"still settling", prelaunch, launched.Add(2 * time.Minute)},
		{"nested windows", nested, launched.Add(time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := convergeOptions(t)
			o.from = []string{syncD1}
			o.batch = 1
			o.windows, o.fullHistory, o.settle = tc.windows, true, 10*time.Minute
			o.now = func() time.Time { return launched }
			identity, err := syncIdentity(o)
			if err != nil {
				t.Fatal(err)
			}
			j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(j.close)
			s := &syncer{o: o, j: j, b: newFakeSync(), copied: map[string]bool{}, forbid: map[string]bool{}, failedCp: map[string]bool{}}
			s.sum.phases = map[string]*syncPhase{}
			if got := s.copyWindows(); got[len(got)-1].Until != until-5*60 {
				t.Fatalf("fixture must save a clipped window, got %+v", got)
			}
			if err := s.savePhase("copy", 0, 7, 11); err != nil {
				t.Fatal(err)
			}
			policy, _ := json.Marshal([]any{s.o.batch, s.copyWindows()})
			s = restartPhaseFixture(t, s)
			s.o.now = func() time.Time { return tc.resumed }
			if err := s.restorePhase(); err != nil {
				t.Fatalf("resume refused: %v", err)
			}
			if s.resume.Phase != "copy" || s.resume.Done != 7 {
				t.Fatalf("resume position lost: %+v", s.resume)
			}
			if !s.compatibleWindowPolicy(string(policy)) {
				t.Fatal("window policy saved before settling refused")
			}
			s.o.batch++
			if s.compatibleWindowPolicy(string(policy)) {
				t.Fatal("window policy accepted a changed batch size")
			}
		})
	}
}

// Only the requested windows clipped at an older horizon are compatible.
func TestSyncSettledWindowsCompatible(t *testing.T) {
	s, _ := phaseResumeFixture(t)
	s.o.windows = syncRanges{{Name: "w", From: 1000, Until: 5000}}
	s.o.settle = 0
	s.o.now = func() time.Time { return time.Unix(3000, 0) }
	for _, tc := range []struct {
		name  string
		saved syncRanges
		ok    bool
	}{
		{"current", syncRanges{{Name: "w", From: 1000, Until: 3000}}, true},
		{"older horizon", syncRanges{{Name: "w", From: 1000, Until: 2000}}, true},
		{"future horizon", syncRanges{{Name: "w", From: 1000, Until: 4000}}, false},
		{"moved start", syncRanges{{Name: "w", From: 1500, Until: 2000}}, false},
		{"renamed", syncRanges{{Name: "x", From: 1000, Until: 2000}}, false},
		{"empty", nil, false},
	} {
		if got := s.settledWindowsCompatible(tc.saved); got != tc.ok {
			t.Errorf("%s: compatible=%v, want %v", tc.name, got, tc.ok)
		}
	}
}

// Options saved by a release build 10 minutes after launch, while the window
// was still settling (Until 1795861178 of 1795861440). Every later resume
// refused this state before the fix.
func TestSyncPhaseOptionsAcceptReleaseStateSavedWhileSettling(t *testing.T) {
	s, _ := phaseResumeFixture(t)
	s.o.windows = syncRanges{{Name: "prelaunch", From: 1795857840, Until: 1795861440}}
	s.o.fullHistory, s.o.settle, s.o.batch = true, 10*time.Minute, 500
	s.o.now = func() time.Time { return time.Unix(1795869600, 0) }
	saved := `{"NoCopy":false,"NoVerify":false,"FullHistory":true,"Settle":600000000000,"Batch":500,"Windows":[{"Name":"prelaunch","From":1795857840,"Until":1795861178}]}`
	if !s.compatiblePhaseOptions(saved) {
		t.Fatal("release state saved while its window settled is refused")
	}
	if s.compatiblePhaseOptions(strings.Replace(saved, "1795857840", "1795857900", 1)) {
		t.Fatal("a moved window start is accepted")
	}
}
