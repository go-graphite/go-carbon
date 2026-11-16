package downsize

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyWindowQuarantineRepairUsesFreshNamedProof(t *testing.T) {
	t.Parallel()
	base, o, _ := newReadyFixture(t, 0)
	o.from = []string{syncD1}
	f := &quarantineFixture{base, map[string]int{}}
	for _, n := range []string{"already", "repair", "broken", "held"} {
		for _, h := range []string{syncD1, syncT} {
			f.put(h, n, syncTestSchema)
		}
	}
	// A pre-existing target value is authoritative, even when it differs.
	f.store(syncT).points["already"][60][syncTestNow-86400] = 12345
	delete(f.store(syncT).archives, "repair")
	delete(f.store(syncT).points, "repair")
	delete(f.store(syncT).archives, "broken")
	delete(f.store(syncT).points, "broken")
	q, err := openSyncIssues(o.state)
	if err != nil {
		t.Fatal(err)
	}
	o.issues = q
	var rows []syncResidual
	for _, n := range []string{"already", "repair", "broken"} {
		rows = append(rows, syncResidual{syncHole: syncHole{Name: n}, Reason: "copy_failed"})
	}
	if err = q.record(rows, "window retry", false); err != nil {
		t.Fatal(err)
	}
	if err = q.record([]syncResidual{{syncHole: syncHole{Name: "held"}, Reason: "nonfinite_target_conflict"}}, "verify", false); err != nil {
		t.Fatal(err)
	}
	first := q.quarantined["broken"].FirstSeen
	if err = repairLegacyWindowQuarantine(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if q.excluded("already") || q.excluded("repair") || !q.excluded("broken") || !q.excluded("held") {
		t.Fatal("fresh proof did not resolve only repaired names", q.quarantined)
	}
	if !strings.HasPrefix(q.quarantined["broken"].Reason, "fresh quarantine repair: ") || !q.quarantined["broken"].FirstSeen.Equal(first) {
		t.Fatal("unresolved reason/first seen lost", q.quarantined)
	}
	if f.store(syncT).points["already"][60][syncTestNow-86400] != 12345 || f.attempted["held"] != 0 || f.attempted["repair"] == 0 {
		t.Fatal("target wins or targeted repair violated", f.attempted)
	}
	// A process crash after publishing the replacement snapshot must not
	// replay the former issue journal and resurrect resolved exclusions.
	reopened, err := openSyncIssues(o.state)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.excluded("already") || reopened.excluded("repair") || !reopened.excluded("broken") || !reopened.excluded("held") {
		t.Fatal("resolved exclusions resurrected on recovery", reopened.quarantined)
	}
	files, _ := filepath.Glob(filepath.Join(o.state, "quarantine-repair-*", "original-quarantine.json"))
	if len(files) != 1 {
		t.Fatal("legacy evidence not preserved", files)
	}
	before := f.attempted["broken"]
	if err = repairLegacyWindowQuarantine(context.Background(), o, f); err != nil || f.attempted["broken"] != before {
		t.Fatal("unresolved name retried on every resume", err, f.attempted)
	}
}

func TestLegacyWindowQuarantineCancellationPreservesExclusions(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	o.fullHistory = true
	o.noCopy = false
	q, err := openSyncIssues(o.state)
	if err != nil {
		t.Fatal(err)
	}
	o.issues = q
	if err = q.record([]syncResidual{{syncHole: syncHole{Name: "broken"}, Reason: "copy_failed"}}, "window retry", false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(o.state, "quarantine.json")
	before, _ := os.ReadFile(path)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = repairLegacyWindowQuarantine(ctx, o, f); err == nil {
		t.Fatal("cancelled repair succeeded")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) || !q.excluded("broken") {
		t.Fatal("cancelled repair dropped quarantine")
	}
}

func TestWindowSelectionQuarantineDoesNotExcludeProvisionalCopies(t *testing.T) {
	t.Parallel()
	q, err := openSyncIssues(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: syncOptions{issues: q}}
	s.addResidual(syncHole{Name: "boundary"}, "copy_failed")
	s.addResidual(syncHole{Name: "pending"}, syncWindowSelectionReason)
	if err = s.quarantineWindowSelections(); err != nil {
		t.Fatal(err)
	}
	if q.excluded("boundary") || !q.excluded("pending") || len(s.sum.residual) != 2 {
		t.Fatal("provisional failure lost its final verification", q.quarantined, s.sum.residual)
	}
}
