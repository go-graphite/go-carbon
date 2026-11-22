package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type staleSync struct{ *fakeSync }

func (f staleSync) List(ctx context.Context, store, prefix, after string, limit int) ([]string, error) {
	if store == syncD1 && after == "" {
		return []string{"gone"}, nil
	}
	return f.fakeSync.List(ctx, store, prefix, after, limit)
}
func convergeOptions(t *testing.T) syncOptions {
	// These fixtures explicitly exercise convergence beyond one repair pass.
	return syncOptions{rescanAll: true, to: syncT, from: []string{syncD1, syncP}, state: t.TempDir(), streams: 2, batch: 64, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
}
func TestSyncConvergeAbsentAllSources(t *testing.T) {
	t.Parallel()
	f := newFakeSync()
	o := convergeOptions(t)
	if err := runSync(context.Background(), o, staleSync{f}); err != nil {
		t.Fatal(err)
	}
	rows, err := os.ReadFile(filepath.Join(o.state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rows), `"reason":"absent_all_sources"`) {
		t.Fatal("missing absence evidence", string(rows))
	}
	residual, _ := os.ReadFile(filepath.Join(o.state, "residual.json"))
	if string(residual) != "null\n" && string(residual) != "[]\n" {
		t.Fatal(string(residual))
	}
}

// partialSync models a successful import exposing another point each round.
// It must converge past the former four-round limit, preserving float bits.
type partialSync struct {
	*fakeSync
	stuck bool
}

func (f partialSync) Copy(_ context.Context, source, target string, names []string, _ string) (syncCopyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forbidden[source] {
		return syncCopyResult{Forbidden: true}, nil
	}
	for _, n := range names {
		src, dst := f.store(source), f.store(target)
		for step, points := range src.points[n] {
			for ts, v := range points {
				if _, ok := dst.points[n][step][ts]; !ok && !f.stuck {
					dst.points[n][step][ts] = v
					break
				}
			}
		}
	}
	return syncCopyResult{Copied: len(names)}, nil
}
func TestSyncConvergeBitExact2000(t *testing.T) {
	t.Parallel()
	f := newFakeSync()
	f.listPageSize = 197 // mismatched pages still cross every 250-name batch
	o := convergeOptions(t)
	o.batch = 250
	schema := []syncArchive{{Step: 3600, Points: 100}}
	expected := map[string]map[int64]float64{}
	for i := 0; i < 2000; i++ {
		n := fmt.Sprintf("metric.%04d", i)
		expected[n] = map[int64]float64{}
		for _, store := range []string{syncT, syncP} {
			s := f.store(store)
			s.archives[n] = schema
			s.points[n] = map[int64]map[int64]float64{3600: {}}
		}
		for j := int64(0); j < 7; j++ {
			ts := syncTestNow - 20*3600 + j*3600
			v := math.Float64frombits(uint64(0x3ff0000000000000+i*17) + uint64(j))
			f.store(syncP).points[n][3600][ts] = v
			expected[n][ts] = v
		}
		// Existing target values win, including signed zero.
		ts := syncTestNow - 20*3600
		f.store(syncT).points[n][3600][ts] = math.Copysign(0, -1)
		expected[n][ts] = math.Copysign(0, -1)
	}
	if err := runSync(context.Background(), o, partialSync{fakeSync: f}); err != nil {
		t.Fatal(err)
	}
	for n, points := range expected {
		for ts, v := range points {
			if got := f.store(syncT).points[n][3600][ts]; math.Float64bits(got) != math.Float64bits(v) {
				t.Fatalf("%s %d bits differ", n, ts)
			}
		}
	}
	data, _ := os.ReadFile(filepath.Join(o.state, "sync.jsonl"))
	var maxRound int
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var r syncRow
		_ = json.Unmarshal([]byte(line), &r)
		maxRound = max(maxRound, r.Round)
	}
	if maxRound <= 4 {
		t.Fatalf("only %d rounds", maxRound)
	}
}
func TestSyncConvergeStalledEvidence(t *testing.T) {
	for _, stuck := range []bool{true, false} {
		t.Run(fmt.Sprint(stuck), func(t *testing.T) {
			f := newFakeSync()
			o := convergeOptions(t)
			schema := []syncArchive{{60, 10000}}
			for _, host := range []string{syncT, syncP, syncD1} {
				f.put(host, "hole", schema)
			}
			delete(f.store(syncT).points["hole"][60], syncTestNow-6000)
			out, e := os.CreateTemp(t.TempDir(), "events-")
			if e != nil {
				t.Fatal(e)
			}
			original := os.Stdout
			outputMu.Lock()
			os.Stdout = out
			outputMu.Unlock()
			defer func() { outputMu.Lock(); os.Stdout = original; outputMu.Unlock(); out.Close() }()
			err := runSync(context.Background(), o, partialSync{fakeSync: f, stuck: stuck})
			outputMu.Lock()
			os.Stdout = original
			outputMu.Unlock()
			if stuck && !errors.Is(err, ErrSyncResidual) || !stuck && err != nil {
				t.Fatalf("residual exit: %v", err)
			}
			data, e := os.ReadFile(filepath.Join(o.state, "progress.json"))
			if e != nil {
				t.Fatal(e)
			}
			var snapshot progressSnapshot
			if e = json.Unmarshal(data, &snapshot); e != nil {
				t.Fatal(e)
			}
			want := int64(0)
			if stuck {
				want = 1
			}
			if snapshot.RepairFailedNames != want {
				t.Fatalf("distinct repair failures=%d want%d", snapshot.RepairFailedNames, want)
			}
			data, e = os.ReadFile(out.Name())
			if e != nil {
				t.Fatal(e)
			}
			errorsSeen := 0
			summarySeen := false
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				var event map[string]json.RawMessage
				if e = json.Unmarshal([]byte(line), &event); e != nil {
					t.Fatal(e)
				}
				if string(event["event"]) == `"sync_repair_failed"` {
					errorsSeen++
					if string(event["metric"]) != `"hole"` || string(event["level"]) != `"ERROR"` || string(event["reason"]) != `"persisted_after_fill"` {
						t.Fatal(string(line))
					}
					var holes []syncResidual
					if e = json.Unmarshal(event["holes"], &holes); e != nil || len(holes) != 2 {
						t.Fatalf("donor evidence: %s", line)
					}
				}
				if string(event["event"]) == `"sync_done"` && string(event["level"]) == `"ERROR"` {
					summarySeen = true
				}
			}
			if int64(errorsSeen) != want || summarySeen != stuck {
				t.Fatalf("errors=%d summary=%t stuck=%t", errorsSeen, summarySeen, stuck)
			}
			data, _ = os.ReadFile(filepath.Join(o.state, "residual.json"))
			if stuck && (!strings.Contains(string(data), "persisted_after_fill") || !strings.Contains(string(data), `"points":1`)) {
				t.Fatal(string(data))
			}
		})
	}
}

func TestSyncConvergeAbsentProofError(t *testing.T) {
	t.Parallel()
	f := newFakeSync()
	o := convergeOptions(t)
	f.noDigest[syncP] = true
	f.poison = "gone"
	if err := runSync(context.Background(), o, staleSync{f}); err == nil {
		t.Fatal("failed proof resolved absence")
	}
}

func TestSyncConvergeKnownEmpty(t *testing.T) {
	t.Parallel()
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			f := newFakeSync()
			o := convergeOptions(t)
			for _, store := range []string{syncT, syncD1, syncP} {
				f.store(store).archives["empty"] = []syncArchive{{Step: 3600, Points: 100}}
				f.store(store).points["empty"] = map[int64]map[int64]float64{3600: {}}
				f.noDigest[store] = legacy
			}
			if err := runSync(context.Background(), o, f); err != nil {
				t.Fatal(err)
			}
			data, _ := os.ReadFile(filepath.Join(o.state, "resolved.json"))
			if !strings.Contains(string(data), "absent_all_sources") {
				t.Fatal(string(data))
			}
		})
	}
}

// servingSync enforces the live digest contract: positive From, one serving
// archive, and a found-but-outside-retention name distinct from not-found.
type servingSync struct{ *fakeSync }

func (f servingSync) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	if from <= 0 {
		return nil, fmt.Errorf("invalid digest From %d", from)
	}
	got, err := f.fakeSync.Digest(ctx, store, names, from, until)
	if err != nil {
		return nil, err
	}
	for n, archives := range got {
		got[n] = nil
		for _, a := range archives {
			if syncTestNow-a.Step*a.Points <= from {
				got[n] = []syncDigest{a}
				break
			}
		}
	}
	return got, nil
}
func TestSyncConvergeChecksEveryArchive(t *testing.T) {
	t.Parallel()
	f := newFakeSync()
	o := convergeOptions(t)
	schema := []syncArchive{{Step: 60, Points: 360}, {Step: 3600, Points: 100}}
	for _, store := range []string{syncT, syncP} {
		f.store(store).archives["recent.only"] = schema
		f.store(store).points["recent.only"] = map[int64]map[int64]float64{60: {syncTestNow - 3600: 42}, 3600: {}}
	}
	if err := runSync(context.Background(), o, servingSync{f}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(o.state, "resolved.json"))
	if strings.Contains(string(data), "recent.only") {
		t.Fatal("empty coarse archive hid finite fine points", string(data))
	}
}

func TestSyncConvergeNoCatalogHolders(t *testing.T) {
	t.Parallel()
	o := convergeOptions(t)
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte("absent\t0\t0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	s := &syncer{o: o, b: newFakeSync(), j: j, forbid: map[string]bool{}}
	s.sum.phases = map[string]*syncPhase{}
	if err := s.verifyAndFill(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := j.find(func(r syncRow) bool { return len(r.Resolved) > 0 })
	if len(rows) != 1 || rows[0].Resolved[0].Reason != "absent_all_sources" || len(s.sum.residual) != 0 {
		t.Fatal("unproven empty scope", rows, s.sum.residual)
	}
}

type noCatalogSync struct{ *fakeSync }

func (f noCatalogSync) List(context.Context, string, string, string, int) ([]string, error) {
	return nil, fmt.Errorf("explicit scope must not scan catalogs")
}

func TestSyncConvergeExplicitAbsentNames(t *testing.T) {
	t.Parallel()
	o := convergeOptions(t)
	o.namesFile = filepath.Join(t.TempDir(), "scope.names")
	if err := os.WriteFile(o.namesFile, []byte("absent.one\nabsent.two\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runSync(context.Background(), o, noCatalogSync{newFakeSync()}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(o.state, "resolved.json"))
	if !strings.Contains(string(data), "absent.one") || !strings.Contains(string(data), "absent.two") {
		t.Fatal(string(data))
	}
}

func TestSyncConvergeExplicitNamesCheckUnlistedPeersEveryRound(t *testing.T) {
	t.Parallel()
	o := convergeOptions(t)
	o.namesFile = "explicit-scope"
	s := &syncer{o: o}
	s.masks.Store("unlisted", uint64(0))
	if got := s.holders(1, []string{"unlisted"}); len(got) != 1 {
		t.Fatal(got)
	}
	s.masks.Store("unlisted", uint64(1))
	if got := s.holders(1, []string{"unlisted"}); len(got) != 1 {
		t.Fatal(got)
	}
}

// A failed fill must remain in the next verification set and be retried even
// when no other name makes progress. Permanent failures stay bounded and loud.
type failingFillSync struct {
	*fakeSync
	failures int
	calls    int
}

func (f *failingFillSync) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	f.calls++
	if f.calls <= f.failures {
		return syncCopyResult{Failed: append([]string(nil), names...)}, nil
	}
	return f.fakeSync.Copy(ctx, source, target, names, journal)
}
func TestSyncConvergeRetriesFailedFill(t *testing.T) {
	t.Parallel()
	for _, failures := range []int{1, 2, 100} {
		t.Run(fmt.Sprint(failures), func(t *testing.T) {
			f := &failingFillSync{fakeSync: newFakeSync(), failures: failures}
			o := convergeOptions(t)
			schema := []syncArchive{{60, 10000}}
			f.put(syncT, "hole", schema)
			f.put(syncP, "hole", schema)
			ts := int64(syncTestNow - 6000)
			want := math.Float64frombits(0x8000000000000000)
			f.store(syncP).points["hole"][60][ts] = want
			delete(f.store(syncT).points["hole"][60], ts)
			err := runSync(context.Background(), o, f)
			journal, _ := os.ReadFile(filepath.Join(o.state, "sync.jsonl"))
			if !strings.Contains(string(journal), `"failed":["hole"]`) {
				t.Fatal("lost failed-fill journal evidence", string(journal))
			}
			if failures < 3 {
				if err != nil {
					t.Fatal(err)
				}
				if f.calls != failures+1 {
					t.Fatalf("calls %d, want %d", f.calls, failures+1)
				}
				if got := f.store(syncT).points["hole"][60][ts]; math.Float64bits(got) != math.Float64bits(want) {
					t.Fatal("changed recovered point bits")
				}
			} else {
				if err == nil {
					t.Fatal("permanent failed fill succeeded")
				}
				if f.calls != 3 {
					t.Fatalf("calls %d, want 3", f.calls)
				}
				residual, _ := os.ReadFile(filepath.Join(o.state, "residual.json"))
				if !strings.Contains(string(residual), `"copy_failed"`) || !strings.Contains(string(residual), `"hole"`) {
					t.Fatal("lost residual", string(residual))
				}
			}
		})
	}
}

// A successful historical repair that only reduces a hole must stop with the
// remaining evidence. A shrinking score does not authorize replaying that name.
type partialWindowSync struct {
	partialSync
	calls int
}

func (f *partialWindowSync) CopyWindow(ctx context.Context, source, target string, names []string, path string, _ syncRanges) (syncCopyResult, error) {
	f.calls++
	return f.partialSync.Copy(ctx, source, target, names, path)
}
func TestSyncWindowSuccessfulPartialRepairIsNotRepeated(t *testing.T) {
	f := newFakeSync()
	o := convergeOptions(t)
	o.from = []string{syncP}
	o.includeShort = true
	o.windows = syncRanges{{Name: "closed", From: syncTestNow - 7200, Until: syncTestNow - 120}}
	schema := []syncArchive{{60, 10000}}
	f.put(syncP, "hole", schema)
	f.put(syncT, "hole", schema)
	for _, ts := range []int64{syncTestNow - 6000, syncTestNow - 5940, syncTestNow - 5880} {
		f.store(syncP).points["hole"][60][ts] = 1
		delete(f.store(syncT).points["hole"][60], ts)
	}
	b := &partialWindowSync{partialSync: partialSync{fakeSync: f}}
	if err := runSync(context.Background(), o, b); !errors.Is(err, ErrSyncResidual) {
		t.Fatalf("incomplete successful repair was accepted: %v", err)
	}
	if b.calls != 1 {
		t.Fatalf("successful name repaired %d times", b.calls)
	}
	data, err := os.ReadFile(filepath.Join(o.state, "residual.json"))
	if err != nil || !strings.Contains(string(data), "persisted_after_fill") {
		t.Fatal("remaining hole evidence missing", string(data), err)
	}
}
