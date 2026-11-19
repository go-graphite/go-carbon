package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// A completed window scope can acquire a late selection resolution without a
// preceding verification-incomplete row. The old client replayed its stale
// round-one receipt (or reopened the entire catalog) after that import.
func TestSyncLateWindowRepairReverifiesExactMembership(t *testing.T) {
	t.Parallel()
	testSyncLateWindowRepair(t, false)
}

func TestSyncLateWindowRepairAdoptsLegacyCompleteProof(t *testing.T) {
	t.Parallel()
	testSyncLateWindowRepair(t, true)
}

func testSyncLateWindowRepair(t *testing.T, legacy bool) {
	t.Helper()
	f := newFakeSync()
	o := convergeOptions(t)
	o.windows = syncRanges{{"gap", syncTestNow - 7200, syncTestNow - 3600}}
	for _, host := range []string{syncT, syncP} {
		for _, name := range []string{"clean", "repair"} {
			f.put(host, name, syncTestSchema)
		}
	}
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if legacy {
		path := filepath.Join(o.state, "sync.jsonl")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var old strings.Builder
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			var row syncRow
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				t.Fatal(err)
			}
			if row.Kind != "verification-pass" {
				old.WriteString(line + "\n")
			}
		}
		if err := os.WriteFile(path, []byte(old.String()), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path + ".checkpoint.json"); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	ts := int64(syncTestNow - 6000)
	delete(f.store(syncT).points["repair"][60], ts)
	identity, _ := syncIdentity(o)
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.append(syncRow{Kind: "window-selected", Round: 1, Source: syncP, Skipped: []syncSkip{{"repair", syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
		t.Fatal(err)
	}
	j.close()
	b := &resumeDigestWatch{fakeSync: f}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if len(b.clean) != 0 {
		t.Fatalf("reverified clean data: %v", b.clean)
	}
	if _, ok := f.store(syncT).points["repair"][60][ts]; !ok {
		t.Fatal("repair lost")
	}
	data, err := os.ReadFile(filepath.Join(o.state, "progress.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p progressSnapshot
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	if p.Total != 1 {
		t.Fatalf("repair scope grew: %+v", p)
	}
	copies := len(f.copies)
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if len(b.clean) != 0 || len(f.copies) != copies {
		t.Fatal("repeated completed work")
	}
}

type retryShapeBackend struct {
	*fakeSync
	now        *time.Time
	unknown    int
	selections int
}

func (b *retryShapeBackend) WindowPresence(_ context.Context, _ string, names []string, _ syncRange) (map[string][]syncDigest, error) {
	b.selections++
	*b.now = b.now.Add(3 * time.Minute)
	out := map[string][]syncDigest{}
	for _, name := range names {
		var ordinal int
		fmt.Sscanf(name, "retry.%d", &ordinal)
		hash := "empty"
		if ordinal < b.unknown {
			hash = syncWindowUnresolved
		}
		out[name] = []syncDigest{{Hash: hash, Step: 60, Points: 10000}}
	}
	return out, nil
}
func TestSyncRetryRealShapeStopsBeforeCatalogVerification(t *testing.T) {
	t.Parallel()
	const verified, retried, unknown = 161068460, 22247, 19517
	state := t.TempDir()
	j, err := openSyncJournal(filepath.Join(state, "sync.jsonl"), "real-shape")
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeSync()
	row := syncRow{Kind: "window-selected", Round: 1, Source: syncD1, SelectionComplete: true}
	for i := range retried {
		name := fmt.Sprintf("retry.%05d", i)
		row.Skipped = append(row.Skipped, syncSkip{name, syncWindowSelectionReason})
		// Committed metadata disproves absence, but the pin's coverage is unknown.
		f.store(syncD1).archives[name] = syncTestSchema
	}
	for _, r := range []syncRow{{Kind: "verification-pass", Round: 1, Names: verified, SelectionComplete: true}, row} {
		if err := j.append(r); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(syncTestNow, 0)
	b := &retryShapeBackend{fakeSync: f, now: &now, unknown: unknown}
	s := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: state, windows: syncRanges{{"gap", syncTestNow - 600, syncTestNow - 120}}, includeShort: true, now: func() time.Time { return now }}, b: b, j: j, forbid: map[string]bool{}}
	err = s.waitWindowSelections(context.Background())
	var terminal *syncWindowSelectionError
	if !errors.As(err, &terminal) || len(terminal.Residuals) != unknown {
		t.Fatalf("terminal=%v err=%v", terminal, err)
	}
	if s.j.progress.RepairFailedNames != unknown {
		t.Fatal("missing failure gauge", s.j.progress)
	}
	for _, r := range terminal.Residuals {
		if r.Source != syncD1 || r.Reason != syncWindowSelectionReason {
			t.Fatal(r)
		}
	}
	if len(j.find(func(r syncRow) bool { return r.Kind == "verified" })) != 0 {
		t.Fatal("full verify started")
	}
	if n := len(s.resolvedWindowSelections()[syncWindowSelectionKey{1, syncD1}]); n != retried-unknown {
		t.Fatal("resolved membership", n)
	}
	kept := verificationCheckpointRows(j.rows)
	found := false
	for _, r := range kept {
		found = found || r.Kind == "window-retry-terminal"
	}
	if !found {
		t.Fatal("checkpoint dropped terminal evidence")
	}
	j.close()
	j, err = openSyncJournal(filepath.Join(state, "sync.jsonl"), "real-shape")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	s.j = j
	before := b.selections
	if err := s.waitWindowSelections(context.Background()); !errors.Is(err, ErrSyncResidual) || b.selections != before {
		t.Fatal("terminal resume retried", err)
	}
	t.Logf("verified=%d retried=%d unknown=%d reverified=0 terminal_retries=0", verified, retried, unknown)
}

func TestSyncReadyTerminalSelectionNamesHolderAndReason(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 100)
	residuals := []syncResidual{{syncHole: syncHole{Name: "pending", Source: syncD1}, Reason: syncWindowSelectionReason}}
	err := runSyncReady(context.Background(), &o, r, f, func() error { return &syncWindowSelectionError{Residuals: residuals} })
	report := readReadyReport(t, o.state)
	if !errors.Is(err, ErrSyncNotReady) || report.Verdict != "NOT_READY" || !reflect.DeepEqual(report.PendingSelections, residuals) {
		t.Fatalf("err=%v report=%+v", err, report)
	}
	if !strings.Contains(strings.Join(report.Reasons, " "), "holder="+syncD1) {
		t.Fatal("missing named NOT_READY reason", report.Reasons)
	}
}
