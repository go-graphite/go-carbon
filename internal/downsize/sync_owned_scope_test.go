package downsize

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

func ownedTestRing(t *testing.T, state string) (string, func(string) bool) {
	t.Helper()
	path := filepath.Join(state, "ring.json")
	data := `{"hash":"jump_fnv1a","replicas":1,"nodes":[{"id":"a","node":"a","endpoint":"` + syncT + `"},{"id":"b","node":"b","endpoint":"` + syncD2 + `"}]}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	owner, err := ringplan.TargetOwner(path, syncT)
	if err != nil {
		t.Fatal(err)
	}
	return path, owner
}

// Compare with the former broad scope: every owned series must end with the
// same points; non-owned series must receive neither reads nor repairs.
func TestSyncOwnedScopeDifferential(t *testing.T) {
	f, o, _ := newReadyFixture(t, 64)
	broad := &scopeRecorder{readyFixture: f, read: map[string]bool{}}
	gap := (syncTestNow - 2*86400) / 3600 * 3600
	for i := 0; i < 64; i++ {
		delete(f.store(syncT).points[fmt.Sprintf("metric.%03d", i)][3600], gap)
	}
	if err := runSync(context.Background(), o, broad); err != nil {
		t.Fatal(err)
	}
	expected := f.store(syncT).points
	f, o, _ = newReadyFixture(t, 64)
	o.ring, _ = ownedTestRing(t, o.state)
	_, owner := ownedTestRing(t, o.state)
	candidate := &scopeRecorder{readyFixture: f, read: map[string]bool{}}
	for i := 0; i < 64; i++ {
		delete(f.store(syncT).points[fmt.Sprintf("metric.%03d", i)][3600], gap)
	}
	f.put(syncT, "target.only", []syncArchive{{3600, 240}})
	if err := runSync(context.Background(), o, candidate); err != nil {
		t.Fatal(err)
	}
	owned := 0
	for i := 0; i < 64; i++ {
		n := fmt.Sprintf("metric.%03d", i)
		if owner(n) {
			owned++
			if !reflect.DeepEqual(expected[n], f.store(syncT).points[n]) {
				t.Fatalf("owned result differs: %s", n)
			}
		} else {
			if candidate.read[n] {
				t.Fatalf("non-owned name read: %s", n)
			}
			if _, ok := f.store(syncT).points[n][3600][gap]; ok {
				t.Fatalf("non-owned hole filled: %s", n)
			}
		}
	}
	if owned == 0 || owned == 64 || round1Verified(t, o.state) != owned {
		t.Fatalf("scope=%d owned=%d", round1Verified(t, o.state), owned)
	}
	if candidate.read["target.only"] {
		t.Fatal("ring scope read target-only name")
	}
}

func TestSyncOwnedScopeOldJournalResume(t *testing.T) {
	for _, modern := range []bool{false, true} {
		t.Run(fmt.Sprint(modern), func(t *testing.T) {
			s, j := phaseResumeFixture(t)
			s.o.rescanAll = false
			s.o.batch = 3
			s.o.sourceScope = true
			s.o.maxRounds = 2
			var catalog strings.Builder
			for i := 0; i < 20; i++ {
				fmt.Fprintf(&catalog, "metric.%03d\t1\t1\n", i)
			}
			if err := os.WriteFile(s.unionPath(), []byte(catalog.String()), 0600); err != nil {
				t.Fatal(err)
			}
			s.sum.Discovered = 20
			s.sum.OnTarget = 20
			s.both = 20
			if modern {
				if err := j.append(syncRow{Kind: "verification-round-scope", Round: 1, Phase: "union"}); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 12; i += 6 {
				r := syncRow{Kind: "verified", Round: 1, First: fmt.Sprintf("metric.%03d", i), Last: fmt.Sprintf("metric.%03d", i+5), Names: 6}
				if modern {
					r.MutationObserved = true
					r.ObservationProof = "prior-seal"
				}
				if i == 0 {
					r.Holes = []syncHole{{Name: "metric.001", Source: syncD1, Step: 3600, Points: 1}, {Name: "metric.002", Source: syncD1, Step: 3600, Points: 1}}
				}
				if err := j.append(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.savePhase("verify", 1, 12, 20); err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(j.file.Name())
			if err != nil {
				t.Fatal(err)
			}
			s = restartPhaseFixture(t, s)
			if err := s.restorePhase(); err != nil {
				t.Fatal(err)
			}
			s.owner = func(n string) bool { return (n[len(n)-1]-'0')%2 == 0 }
			f, _, _ := newReadyFixture(t, 20)
			b := &scopeRecorder{readyFixture: f, read: map[string]bool{}}
			s.b = b
			// Both old observations are dirty. Only the owned one may be filled.
			gap := (syncTestNow - 2*86400) / 3600 * 3600
			delete(f.store(syncT).points["metric.001"][3600], gap)
			delete(f.store(syncT).points["metric.002"][3600], gap)
			if err := s.verifyAndFill(context.Background()); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 20; i++ {
				n := fmt.Sprintf("metric.%03d", i)
				if !s.owner(n) && b.read[n] {
					t.Fatalf("non-owned read on resume: %s", n)
				}
				if i < 12 && i != 2 && b.read[n] {
					t.Fatalf("completed name redone: %s", n)
				}
			}
			for _, copy := range f.copies {
				if strings.Contains(copy, "metric.001") {
					t.Fatal("non-owned fill", copy)
				}
			}
			found := false
			for _, r := range s.j.rows {
				if r.Kind == "phase-position" && r.Position.Phase == "verify" && r.Position.Round == 1 {
					p := r.Position
					if p.Done < 12 || p.Total < p.Done {
						t.Fatalf("regressed progress: %+v", p)
					}
					if p.Done == 16 && p.Total == 16 {
						found = true
					}
				}
			}
			if !found {
				t.Fatal("expected 12 retained + 4 pending owned names")
			}
			after, _ := os.ReadFile(s.j.file.Name())
			if !strings.HasPrefix(string(after), string(original)) {
				t.Fatal("old journal was rewritten")
			}
			// Reopen the mixed broad/sparse receipts. No completed data read is repeated.
			s.resume = syncPhasePosition{Phase: "verify", Round: 1, Done: 16, Total: 16, Complete: true}
			b.read = map[string]bool{}
			if _, _, _, err := s.replayOwnedRound(context.Background(), 1, s.unionSource()); err != nil {
				t.Fatal(err)
			}
			if len(b.read) != 0 {
				t.Fatal("replay read completed data")
			}
			owner := s.owner
			copies := len(f.copies)
			s = restartPhaseFixture(t, s)
			if err := s.restorePhase(); err != nil {
				t.Fatal(err)
			}
			s.owner = owner
			s.b = b
			b.read = map[string]bool{}
			if err := s.verifyAndFill(context.Background()); err != nil {
				t.Fatal("later-round restart:", err)
			}
			if len(b.read) != 0 || len(f.copies) != copies {
				t.Fatal("later-round restart redid completed work")
			}

		})
	}
}

func TestSyncOwnedScopeRefusesProgressRegression(t *testing.T) {
	s, _ := phaseResumeFixture(t)
	s.owner = func(string) bool { return false }
	s.sum.Discovered = 2
	if err := os.WriteFile(s.unionPath(), []byte("a\t1\t1\nb\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.resume = syncPhasePosition{Phase: "verify", Round: 1, Done: 1, Total: 2}
	input := s.unionSource()
	if err := s.narrowOwnedRound(context.Background(), 1, &input, nil); err == nil || !strings.Contains(err.Error(), "lower published progress") {
		t.Fatal(err)
	}
	input.close()
	if s.resume.Done != 1 {
		t.Fatal("published done lowered")
	}
}

func TestSyncOwnedScopePartialFillKeepsCompletedImports(t *testing.T) {
	s, _ := phaseResumeFixture(t)
	s.owner = func(n string) bool { return n != "b" }
	f := newFakeSync()
	s.b = f
	for _, n := range []string{"a", "b", "c"} {
		f.put(syncD1, n, []syncArchive{{3600, 240}})
		f.put(syncT, n, []syncArchive{{3600, 240}})
	}
	names := []string{"a", "b", "c"}
	identity, err := s.fillIdentity(1, 0, names)
	if err != nil {
		t.Fatal(err)
	}
	path, err := s.namesFile("fill-"+identity, 1, 0, names)
	if err != nil {
		t.Fatal(err)
	}
	ci, err := copyIdentity(copyOptions{froms: []string{syncD1}, to: syncT, names: strings.TrimSuffix(path, ".jsonl") + ".names"})
	if err != nil {
		t.Fatal(err)
	}
	j, err := openCopyJournal(path, ci)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.done(syncD1, "a", "a", 1, 1, time.Second); err != nil {
		t.Fatal(err)
	}
	j.close()
	before, _ := os.ReadFile(path)
	if _, err := s.copyFrom(context.Background(), "fill", 1, 0, names); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != 1 || f.copies[0] != syncD1+" c" {
		t.Fatalf("completed or non-owned import repeated: %v", f.copies)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("old transport journal changed")
	}
}

// The first upgrade must also survive another restart without republishing
// the original, broader legacy catalog total.
func TestSyncOwnedScopeNarrowedTotalSurvivesRestart(t *testing.T) {
	s, j := phaseResumeFixture(t)
	s.sum.Discovered = 20
	s.owner = func(string) bool { return true }
	if err := j.append(syncRow{Kind: "verified", Round: 1, First: "a", Last: "m", Names: 12}); err != nil {
		t.Fatal(err)
	}
	if err := j.append(syncRow{Kind: "ownership-scope", Round: 1, Names: 16, NotOwned: 4}); err != nil {
		t.Fatal(err)
	}
	if err := s.savePhase("verify", 1, 12, 16); err != nil {
		t.Fatal(err)
	}
	s = restartPhaseFixture(t, s)
	if err := s.restorePhase(); err != nil {
		t.Fatal(err)
	}
	if s.resume.Done != 12 || s.resume.Total != 16 {
		t.Fatalf("narrowed position lost: %+v", s.resume)
	}
	if s.j.progress.OwnershipExcluded != 4 {
		t.Fatal("excluded gauge lost on restart")
	}
}

func TestSyncOwnedScopeReadyIgnoresLegacyNonOwnedCatalog(t *testing.T) {
	f, o, r := newReadyFixture(t, 20)
	var owner func(string) bool
	o.ring, owner = ownedTestRing(t, o.state)
	var catalog strings.Builder
	for i := 0; i < 20; i++ {
		n := fmt.Sprintf("metric.%03d", i)
		fmt.Fprintf(&catalog, "%s\t7\t1\n", n)
	}
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte(catalog.String()), 0600); err != nil {
		t.Fatal(err)
	}
	// Old redundant copies can have a different policy without affecting owner readiness.
	f.policy = func(_, n string, p *readyPolicy) {
		if !owner(n) {
			t.Errorf("READY queried a non-owned name: %s", n)
		}
	}
	report := readyReport{Counts: map[string]readyCount{}}
	if err := readyChecks(context.Background(), o, r, f, o.state, &report); err != nil {
		t.Fatal(err)
	}
	if report.PolicyMismatches != 0 {
		t.Fatal(report)
	}
	proof := &proofFixture{fakeSync: f.fakeSync, epoch: "one"}
	scope := &syncer{o: o, b: proof}
	if err := scope.initOwnership(); err != nil {
		t.Fatal(err)
	}
	if err := scope.openFrontier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if scope.frontier == nil {
		t.Fatal("missing frontier")
	}
	scope.frontier.file.Close()
	if _, _, err := readyFrontier(context.Background(), o, proof, true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		n := fmt.Sprintf("metric.%03d", i)
		if !owner(n) {
			proof.store(syncT).archives[n] = []syncArchive{{3600, 241}}
		}
	}
	valid, changed, err := readyFrontierChanges(context.Background(), o, proof)
	if err != nil || !valid || len(changed) != 0 {
		t.Fatalf("non-owned history reopened READY: %v %v %v", valid, changed, err)
	}
}

func TestSyncOwnedScopeSparseResumePreservesDirtyObservation(t *testing.T) {
	s, j := phaseResumeFixture(t)
	s.o.batch = 3
	s.o.rescanAll = false
	s.o.sourceScope = true
	s.sum.Discovered = 20
	s.sum.OnTarget = 20
	var catalog strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&catalog, "metric.%03d\t1\t1\n", i)
	}
	if err := os.WriteFile(s.unionPath(), []byte(catalog.String()), 0600); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"metric.010", "metric.011"} {
		if err := j.append(syncRow{Kind: "verified", Round: 1, First: n, Last: n, Names: 1, Holes: []syncHole{{Name: n, Source: syncD1, Step: 3600, Points: 1}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.savePhase("verify", 1, 2, 20); err != nil {
		t.Fatal(err)
	}
	s = restartPhaseFixture(t, s)
	if err := s.restorePhase(); err != nil {
		t.Fatal(err)
	}
	s.owner = func(n string) bool { return (n[len(n)-1]-'0')%2 == 0 }
	f, _, _ := newReadyFixture(t, 20)
	b := &scopeRecorder{readyFixture: f, read: map[string]bool{}}
	s.b = b
	if _, _, _, err := s.verifyRound(context.Background(), 1, s.unionSource()); err != nil {
		t.Fatal(err)
	}
	if b.read["metric.010"] || b.read["metric.011"] {
		t.Fatal("earlier observations reread")
	}
	s.resume = syncPhasePosition{Phase: "fill", Round: 1}
	holes, _, _, err := s.replayOwnedRound(context.Background(), 1, s.unionSource())
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, h := range holes {
		found[h.Name] = true
	}
	if !found["metric.010"] || found["metric.011"] {
		t.Fatalf("narrowed historical fill identity lost: %v", holes)
	}
	for _, r := range s.j.rows {
		if r.OwnershipScoped && r.First < "metric.010" && r.Last > "metric.010" {
			t.Fatal("new range crosses an earlier observation")
		}
	}
}

func TestSyncOwnedScopeAmbiguousRecoveryReadsOnlyOwner(t *testing.T) {
	s, _ := phaseResumeFixture(t)
	s.owner = func(n string) bool { return (n[len(n)-1]-'0')%2 == 0 }
	s.sum.Discovered = 20
	s.sum.OnTarget = 20
	s.resume = syncPhasePosition{Phase: "fill", Round: 1}
	var catalog strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&catalog, "metric.%03d\t1\t1\n", i)
	}
	if err := os.WriteFile(s.unionPath(), []byte(catalog.String()), 0600); err != nil {
		t.Fatal(err)
	}
	f, _, _ := newReadyFixture(t, 20)
	b := &scopeRecorder{readyFixture: f, read: map[string]bool{}}
	s.b = b
	if err := s.j.append(syncRow{Kind: "verified", Round: 1, First: "metric.005", Last: "metric.005", Names: 1}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.replayVerifyRound(context.Background(), 1, s.unionSource()); err != nil {
		t.Fatal(err)
	}
	if len(b.read) != 10 {
		t.Fatalf("unexpected recovery scope: %v", b.read)
	}
	for n := range b.read {
		if !s.owner(n) {
			t.Fatalf("non-owned recovery read: %s", n)
		}
	}
}
