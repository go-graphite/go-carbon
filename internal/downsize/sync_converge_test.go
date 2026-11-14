package downsize

import (
	"context"
	"encoding/json"
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
	return syncOptions{to: syncT, from: []string{syncD1, syncP}, state: t.TempDir(), streams: 2, batch: 64, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
}
func TestSyncConvergeAbsentAllSources(t *testing.T) {
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
	f := newFakeSync()
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
	f := newFakeSync()
	o := convergeOptions(t)
	schema := []syncArchive{{60, 10000}}
	f.put(syncT, "hole", schema)
	f.put(syncP, "hole", schema)
	delete(f.store(syncT).points["hole"][60], syncTestNow-6000)
	if err := runSync(context.Background(), o, partialSync{fakeSync: f, stuck: true}); err == nil {
		t.Fatal("stalled sync succeeded")
	}
	data, _ := os.ReadFile(filepath.Join(o.state, "residual.json"))
	if !strings.Contains(string(data), "persisted_after_fill") || !strings.Contains(string(data), `"points":1`) {
		t.Fatal(string(data))
	}
}
func TestSyncConvergeAbsentProofError(t *testing.T) {
	f := newFakeSync()
	o := convergeOptions(t)
	f.noDigest[syncP] = true
	f.poison = "gone"
	if err := runSync(context.Background(), o, staleSync{f}); err == nil {
		t.Fatal("failed proof resolved absence")
	}
}

func TestSyncConvergeKnownEmpty(t *testing.T) {
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
