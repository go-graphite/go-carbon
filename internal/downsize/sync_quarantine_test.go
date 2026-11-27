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

type quarantineFixture struct {
	*readyFixture
	attempted map[string]int
}

func (f *quarantineFixture) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	var good []string
	var failed []string
	for _, n := range names {
		f.attempted[n]++
		if len(n) > 4096 {
			return syncCopyResult{}, errors.New("oversized name reached transport")
		}
		if n == "broken" {
			failed = append(failed, n)
		} else {
			good = append(good, n)
		}
	}
	r, e := f.fakeSync.Copy(ctx, source, target, good, journal)
	r.Failed = append(r.Failed, failed...)
	return r, e
}
func TestSyncQuarantineThousandsAndResume(t *testing.T) {
	t.Parallel()
	base, o, r := newReadyFixture(t, 0)
	o.from = []string{syncD1}
	o.batch = 200
	f := &quarantineFixture{base, map[string]int{}}
	oversized := strings.Repeat("z", 4097)
	for i := 0; i < 2002; i++ {
		n := fmt.Sprintf("metric.%04d", i)
		if i == 2000 {
			n = "broken"
		}
		if i == 2001 {
			n = oversized
		}
		for _, h := range []string{syncD1, syncT} {
			s := f.store(h)
			s.archives[n] = []syncArchive{{3600, 24 * 10}}
			s.points[n] = map[int64]map[int64]float64{3600: {syncTestNow - 7200: 42}}
		}
		if i%2 == 0 || i >= 2000 {
			delete(f.store(syncT).archives, n)
			delete(f.store(syncT).points, n)
		} else {
			delete(f.store(syncT).points[n][3600], syncTestNow-7200)
		}
	}
	check := func() {
		t.Helper()
		err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) })
		report := readReadyReport(t, o.state)
		if err == nil || report.Verdict != "READY_EXCEPT" {
			t.Fatalf("want READY_EXCEPT with distinct error, got %s: %v", report.Verdict, err)
		}
		for i := 0; i < 2000; i++ {
			n := fmt.Sprintf("metric.%04d", i)
			if f.store(syncT).points[n][3600][syncTestNow-7200] != 42 {
				t.Fatalf("healthy name %s not repaired", n)
			}
		}
		for file, name := range map[string]string{"quarantine.json": "broken", "policy-skipped.json": oversized} {
			data, e := os.ReadFile(filepath.Join(o.state, file))
			if e != nil {
				t.Fatal(e)
			}
			var rows []map[string]any
			if e = json.Unmarshal(data, &rows); e != nil {
				t.Fatal(e)
			}
			if len(rows) != 1 || rows[0]["name"] != name || rows[0]["reason"] == "" || rows[0]["phase"] == "" || rows[0]["first_seen"] == nil || rows[0]["last_seen"] == nil {
				t.Fatalf("invalid %s: %s", file, data)
			}
		}
	}
	check()
	calls := f.attempted["metric.0000"]
	brokenCalls := f.attempted["broken"]
	check()
	if f.attempted["metric.0000"] != calls || f.attempted["broken"] != brokenCalls {
		t.Fatal("resume repeated confirmed/quarantined copies")
	}
}

func TestSyncPolicySkipAloneAllowsReady(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 110)
	oversized := strings.Repeat("x", 4097)
	f.store(syncD1).archives[oversized] = []syncArchive{{3600, 240}}
	f.store(syncD1).points[oversized] = map[int64]map[int64]float64{3600: {syncTestNow - 7200: 1}}
	if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
		t.Fatal(err)
	}
	report := readReadyReport(t, o.state)
	if report.Verdict != "READY" || report.Quarantined != 0 || report.PolicySkipped != 1 {
		t.Fatalf("%+v", report)
	}
	if f.store(syncT).archives[oversized] != nil {
		t.Fatal("policy skipped name was imported")
	}
}

func TestSyncIssueLedgerRecovery(t *testing.T) {
	t.Parallel()
	state := t.TempDir()
	q, err := openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	rows := []syncResidual{{syncHole: syncHole{Name: "broken"}, Reason: "target rejected"}, {syncHole: syncHole{Name: "broken"}, Reason: "deferred combine"}}
	if err = q.record(rows, "fill", false); err != nil {
		t.Fatal(err)
	}
	q2, err := openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	a, b := q2.counts()
	if a != 1 || b != 0 || !q2.excluded("broken") {
		t.Fatal(a, b)
	}
	first := q2.quarantined["broken"].FirstSeen
	if err = q2.record(rows[:1], "fill", false); err != nil {
		t.Fatal(err)
	}
	if !q2.quarantined["broken"].FirstSeen.Equal(first) || q2.quarantined["broken"].LastSeen.Before(first) {
		t.Fatal("observation times lost")
	}
	if err = os.WriteFile(filepath.Join(state, "quarantine.json"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = openSyncIssues(state); err == nil {
		t.Fatal("corrupt issue ledger accepted")
	}
}

func TestSyncWindowSelectionStallQuarantinesAndResumeRetries(t *testing.T) {
	t.Parallel()
	state := t.TempDir()
	q, err := openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(state, "sync.jsonl"), "quarantine-selection")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err = j.append(syncRow{Kind: "window-selected", Round: 1, Source: syncD1, Skipped: []syncSkip{{"pending", syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
		t.Fatal(err)
	}
	// A state written by an older client that ended the run on this frontier.
	if err = j.append(syncRow{Kind: "window-retry-terminal", Holes: []syncHole{{Name: "pending", Source: syncD1}}}); err != nil {
		t.Fatal(err)
	}
	f := newFakeSync()
	f.put(syncD1, "pending", syncTestSchema)
	now := time.Unix(syncTestNow, 0)
	s := &syncer{o: syncOptions{issues: q, to: syncT, from: []string{syncD1}, state: state, windows: syncRanges{{From: syncTestNow - 600, Until: syncTestNow - 120}}, includeShort: true, now: func() time.Time { return now }}, b: stalledSelectionBackend{f, &now}, j: j, forbid: map[string]bool{}}
	if err = s.savedWindowSelectionFailure(); err != nil {
		t.Fatal("old terminal stop still ends the run", err)
	}
	if err = s.waitWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a, b := q.counts(); a != 1 || b != 0 || !q.excluded("pending") || !s.hasPendingWindowSelections() {
		t.Fatal(a, b)
	}
	if len(j.find(func(r syncRow) bool { return r.Kind == "window-resolved" })) != 0 {
		t.Fatal("quarantine manufactured a resolution")
	}
	if err = q.checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Resume retries the quarantined selection on a fresh pin.
	reopened, err := openSyncIssues(state)
	if err != nil || !reopened.excluded("pending") {
		t.Fatal("quarantine lost on resume", err)
	}
	s.o.issues = reopened
	before := now
	if err = s.waitWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	if now.Equal(before) {
		t.Fatal("resume did not retry quarantined selection")
	}
	if !reopened.excluded("pending") {
		t.Fatal("unresolved selection released")
	}
}

func TestSyncSelectionQuarantineReleaseSurvivesReplay(t *testing.T) {
	t.Parallel()
	state := t.TempDir()
	q, err := openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.record([]syncResidual{{syncHole: syncHole{Name: "pending"}, Reason: syncWindowSelectionReason}, {syncHole: syncHole{Name: "broken"}, Reason: "copy_failed"}}, "window retry", false); err != nil {
		t.Fatal(err)
	}
	if err = q.releaseSelections([]string{"pending", "broken"}); err != nil {
		t.Fatal(err)
	}
	if q.excluded("pending") || !q.excluded("broken") {
		t.Fatal("release touched the wrong names", q.quarantined)
	}
	replayed, err := openSyncIssues(state)
	if err != nil || replayed.excluded("pending") || !replayed.excluded("broken") {
		t.Fatal("journal replay lost the release", err)
	}
	if err = replayed.checkpoint(); err != nil {
		t.Fatal(err)
	}
	snap, err := openSyncIssues(state)
	if a, _ := snap.counts(); err != nil || a != 1 || snap.excluded("pending") {
		t.Fatal("checkpoint kept a released name", err)
	}
}

func TestSyncReadOnlyResidualDoesNotQuarantine(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) || errors.Is(err, ErrSyncQuarantined) {
		t.Fatal(err)
	}
	q, err := openSyncIssues(o.state)
	if err != nil {
		t.Fatal(err)
	}
	if a, b := q.counts(); a != 0 || b != 0 {
		t.Fatal("audit without repair attempts created quarantine", a, b)
	}
}
