package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func addReadyGap(t *testing.T, path, name string, from, until int64, open bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var b syncBlock
	if err = json.Unmarshal(data, &b); err != nil {
		t.Fatal(err)
	}
	g := struct {
		Name  string `json:"name"`
		From  int64  `json:"from"`
		Until int64  `json:"until"`
		Open  bool   `json:"open"`
	}{name, from, until, open}
	b.Gaps = append(b.Gaps, g)
	data, err = json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestReadyGapfillClosedAlongsideOpen(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 110)
	w := syncRange{"closed", syncTestNow - 3*3600, syncTestNow - 3600}
	name := "metric.001"
	delete(f.store(syncT).points[name][3600], w.From)
	f.store(syncD2).points[name][3600][w.From] = math.Copysign(0, -1)
	delete(f.store(syncD1).points[name][3600], w.From)
	delete(f.store(syncP).points[name][3600], w.From)
	r.proofBlock = writeReadyBlock(t, o, r, false)
	addReadyGap(t, r.proofBlock, w.Name, w.From, w.Until, false)
	addReadyGap(t, r.proofBlock, "still-open", w.Until, 0, true)
	called := false
	run := func() error {
		return runSyncReadyPrepared(context.Background(), &o, r, f, nil, func() error { called = true; return errors.New("original job must not run") })
	}
	if err := run(); !errors.Is(err, ErrSyncNotReady) {
		t.Fatal(err)
	}
	if called {
		t.Fatal("old copy/proof was invoked")
	}
	if v, ok := f.store(syncT).points[name][3600][w.From]; !ok || math.Float64bits(v) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatal("donor point bits lost", v)
	}
	report := readReadyReport(t, o.state)
	if report.Verdict != "NOT_READY" || !strings.Contains(strings.Join(report.Reasons, " "), "still-open") || len(report.GapReceipts) != 1 {
		t.Fatalf("%+v", report)
	}
	var receipt gapfillReceipt
	data, _ := os.ReadFile(report.GapReceipts[0])
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.Complete || receipt.Names != 110 || len(receipt.Sources) != 3 || len(receipt.Samples) == 0 {
		t.Fatalf("bad receipt names=%d sources=%d samples=%d complete=%v", receipt.Names, len(receipt.Sources), len(receipt.Samples), receipt.Complete)
	}
	before := len(f.copies)
	// A new open ledger entry changes the descriptor hash, not completed windows.
	addReadyGap(t, r.proofBlock, "another-open", w.Until, 0, true)
	if err := run(); !errors.Is(err, ErrSyncNotReady) {
		t.Fatal(err)
	}
	if len(f.copies) != before {
		t.Fatal("receipt window refilled")
	}
	after, _ := os.ReadFile(report.GapReceipts[0])
	if string(after) != string(data) {
		t.Fatal("finished receipt changed")
	}
}

type interruptedGapfill struct {
	mu sync.Mutex
	*readyFixture
	failFrom  int64
	interrupt bool
	windows   []int64
}

func (f *interruptedGapfill) CopyWindow(ctx context.Context, source, target string, names []string, path string, ws syncRanges) (syncCopyResult, error) {
	f.mu.Lock()
	f.windows = append(f.windows, ws[0].From)
	f.mu.Unlock()
	if f.interrupt && ws[0].From == f.failFrom {
		return syncCopyResult{}, context.Canceled
	}
	return f.fakeSync.CopyWindow(ctx, source, target, names, path, ws)
}

func TestReadyGapfillInterruptedWindowKeepsReceipts(t *testing.T) {
	t.Parallel()
	base, o, r := newReadyFixture(t, 110)
	first, second := syncTestNow-8*3600, syncTestNow-4*3600
	delete(base.store(syncT).points["metric.000"][3600], first)
	delete(base.store(syncT).points["metric.000"][3600], second)
	f := &interruptedGapfill{readyFixture: base, failFrom: second, interrupt: true}
	r.proofBlock = writeReadyBlock(t, o, r, false)
	addReadyGap(t, r.proofBlock, "first", first, first+3600, false)
	addReadyGap(t, r.proofBlock, "second", second, second+3600, false)
	addReadyGap(t, r.proofBlock, "open", second+3600, 0, true)
	run := func() error {
		return runSyncReadyPrepared(context.Background(), &o, r, f, nil, func() error { t.Fatal("original invoked"); return nil })
	}
	if err := run(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	report := readReadyReport(t, o.state)
	if len(report.GapReceipts) != 1 {
		t.Fatalf("first not durable: %+v", report)
	}
	f.interrupt = false
	f.windows = nil
	if err := run(); !errors.Is(err, ErrSyncNotReady) {
		t.Fatal(err)
	}
	for _, from := range f.windows {
		if from == first {
			t.Fatal("completed first window was refilled")
		}
	}
	report = readReadyReport(t, o.state)
	if len(report.GapReceipts) != 2 {
		t.Fatalf("second not completed: %+v", report)
	}
}

func TestReadyGapfillParserRetainsOpenGate(t *testing.T) {
	t.Parallel()
	c, err := parseSyncCommand(syncCLIArgs(t))
	if err != nil {
		t.Fatal(err)
	}
	_, full, r := newReadyFixture(t, 100)
	full.to = c.sync.to
	path := writeReadyBlock(t, full, r, false)
	addReadyGap(t, path, "open-1", 100, 0, true)
	args := append(syncCLIArgs(t), "--ready-block", path)
	if _, err := parseSyncCommand(args); err != nil {
		t.Fatal("open ledger rejected before work", err)
	}
}

func TestGapfillSamplesAllRetentionsAndResume(t *testing.T) {
	t.Parallel()
	f, o, _ := newReadyFixture(t, 1)
	o.windows = syncRanges{{"gap", syncTestNow - 20*86400, syncTestNow - 3600}}
	schema := []syncArchive{{60, 86400 / 60}, {3600, 30 * 24}}
	f.put(syncD1, "multi", schema)
	f.put(syncT, "multi", schema)
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte("multi\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	o.from = []string{syncD1}
	receipt := gapfillReceipt{FilledAt: syncTestNow}
	if err := sampleGapfill(context.Background(), o, f, &receipt); err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, s := range receipt.Samples {
		seen[s.Step] = true
	}
	if !seen[60] || !seen[3600] {
		t.Fatalf("retention strata missing: %+v", receipt.Samples)
	}
	before := len(receipt.Samples)
	if err := sampleGapfill(context.Background(), o, f, &receipt); err != nil {
		t.Fatal(err)
	}
	if len(receipt.Samples) != before {
		t.Fatal("completed samples duplicated")
	}
}

func TestReadyGapfillDifferentialPointBits(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 110)
	from := syncTestNow - 3*3600
	values := []float64{0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, 1.125, -42.25}
	for i, v := range values {
		name := fmt.Sprintf("metric.%03d", i)
		for _, host := range []string{syncT, syncD1, syncD2, syncP} {
			delete(f.store(host).points[name][3600], from)
		}
		f.store(syncD2).points[name][3600][from] = v
	}
	r.proofBlock = writeReadyBlock(t, o, r, false)
	addReadyGap(t, r.proofBlock, "closed", from, from+3600, false)
	addReadyGap(t, r.proofBlock, "duplicate", from, from+3600, false)
	addReadyGap(t, r.proofBlock, "open", from+3600, 0, true)
	err := runSyncReadyPrepared(context.Background(), &o, r, f, nil, func() error { t.Fatal("original job invoked"); return nil })
	if !errors.Is(err, ErrSyncNotReady) {
		t.Fatal(err)
	}
	for i, want := range values {
		name := fmt.Sprintf("metric.%03d", i)
		got, ok := f.store(syncT).points[name][3600][from]
		if !ok || math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("%s want=%016x got=%016x present=%v", name, math.Float64bits(want), math.Float64bits(got), ok)
		}
	}
	report := readReadyReport(t, o.state)
	if len(report.GapReceipts) != 2 {
		t.Fatal("per-label receipts missing")
	}
	// Identical bounds share the native fill and verification work.
	if len(f.copies) != len(o.from)*3 {
		t.Fatalf("same bounds filled %d times", len(f.copies))
	}
}

type sampleInterruptedGapfill struct {
	*readyFixture
	state   string
	fail    bool
	digests atomic.Int64
}

func (f *sampleInterruptedGapfill) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	filled, _ := filepath.Glob(filepath.Join(f.state, "gapfill-window-*", "filled.json"))
	if f.fail && len(filled) > 0 {
		return nil, context.Canceled
	}
	return f.readyFixture.Fetch(ctx, store, names, from, until)
}
func (f *sampleInterruptedGapfill) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	f.digests.Add(1)
	return f.readyFixture.Digest(ctx, store, names, from, until)
}
func TestReadyGapfillResumeSamplesWithoutVerification(t *testing.T) {
	base, o, r := newReadyFixture(t, 110)
	f := &sampleInterruptedGapfill{readyFixture: base, state: o.state, fail: true}
	r.proofBlock = writeReadyBlock(t, o, r, false)
	addReadyGap(t, r.proofBlock, "closed", syncTestNow-3*3600, syncTestNow-3600, false)
	addReadyGap(t, r.proofBlock, "open", syncTestNow-3600, 0, true)
	run := func() error {
		return runSyncReadyPrepared(context.Background(), &o, r, f, nil, func() error { t.Fatal("original job invoked"); return nil })
	}
	if err := run(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f.fail = false
	copies := len(f.copies)
	f.digests.Store(0)
	if err := run(); !errors.Is(err, ErrSyncNotReady) {
		t.Fatal(err)
	}
	if f.digests.Load() != 0 {
		t.Fatalf("repeated %d completed verification digests", f.digests.Load())
	}
	if len(f.copies) != copies {
		t.Fatal("finished fill repeated while resuming samples")
	}
}

func TestReadyGapfillSelectionLeavesOtherClosedPending(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 110)
	r.proofBlock = writeReadyBlock(t, o, r, false)
	addReadyGap(t, r.proofBlock, "selected", syncTestNow-3*3600, syncTestNow-2*3600, false)
	addReadyGap(t, r.proofBlock, "pending", syncTestNow-2*3600, syncTestNow-3600, false)
	r.gapSelection = []string{"selected"}
	err := runSyncReadyPrepared(context.Background(), &o, r, f, nil, func() error { t.Fatal("old job invoked"); return nil })
	if !errors.Is(err, ErrSyncNotReady) {
		t.Fatal(err)
	}
	report := readReadyReport(t, o.state)
	if len(report.GapReceipts) != 1 || !strings.Contains(strings.Join(report.Reasons, " "), "pending") {
		t.Fatalf("%+v", report)
	}
	if len(f.copies) != len(o.from)*3 {
		t.Fatal("unselected window was copied", len(f.copies))
	}
	r.gapSelection = []string{"unknown"}
	if _, _, err := loadReadyProof(o, r); err == nil {
		t.Fatal("unknown window accepted")
	}
}

func TestGapfillNativePointBitsAndResume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	addPoints(t, source, "new", math.Copysign(0, -1))
	addPoints(t, source, "overlap", math.SmallestNonzeroFloat64)
	addPoints(t, target, "overlap", 42)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	survivor := httptest.NewServer(nativeControllerService(t, target))
	defer survivor.Close()
	o := syncOptions{state: t.TempDir(), to: survivor.URL, from: []string{donor.URL}, batch: 1, streams: 1,
		windows: syncRanges{{Name: "gap", From: testMetricNow - 10, Until: testMetricNow}}, now: func() time.Time { return time.Unix(testMetricNow, 0) }}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), mustGapfillSyncIdentity(o), syncCheckpointOptions(o))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.append(syncRow{Kind: "discovered", Names: 2}); err != nil {
		t.Fatal(err)
	}
	j.close()
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte("new\t1\t0\noverlap\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b := newSyncLiveBackend(syncLiveOptions{})
	for range 2 {
		if err := runGapfillWindow(ctx, o, b); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"new", "overlap"} {
			want, err := source.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			for ai, a := range want.Archives {
				for _, p := range a.Points {
					if p.Timestamp < o.windows[0].From || p.Timestamp >= o.windows[0].Until {
						continue
					}
					expected := p.Value
					if name == "overlap" {
						expected = 42
					}
					found := false
					for _, q := range got.Archives[ai].Points {
						if q.Timestamp == p.Timestamp {
							found = true
							if math.Float64bits(q.Value) != math.Float64bits(expected) {
								t.Fatalf("%s point bits: want %016x got %016x", name, math.Float64bits(expected), math.Float64bits(q.Value))
							}
						}
					}
					if !found {
						t.Fatal("native point missing", name, p)
					}
				}
			}
		}
	}
	counts, err := collectWindowPointCounts(o.state)
	if err != nil || counts.Added == nil || *counts.Added <= 0 {
		t.Fatal("native point additions missing", counts, err)
	}
}
