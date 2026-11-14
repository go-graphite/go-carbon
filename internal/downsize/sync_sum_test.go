package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type sumSyncFixture struct {
	*fakeSync
	old     map[string]bool
	clipped bool
}

func (f *sumSyncFixture) SumDigest(_ context.Context, store string, names []string, step, from, until int64) (map[string][]syncDigest, error) {
	if f.old[store] {
		return nil, errSyncNoDigest
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]syncDigest{}
	for _, n := range names {
		for _, a := range f.store(store).archives[n] {
			var sum chunkstore.PointSum
			for ts, v := range f.store(store).points[n][a.Step] {
				if ts >= from && ts < until {
					sum.Add(ts, v)
				}
			}
			lo := from
			if f.clipped && store == syncT {
				lo += a.Step
			}
			out[n] = append(out[n], syncDigest{Step: a.Step, Points: a.Points, Count: int64(sum.Count), Hash: fmt.Sprintf("%016x", sum.Sum), From: lo, Until: until})
		}
	}
	return out, nil
}

func TestSyncFullSumDifferential2048Names(t *testing.T) {
	f := newFakeSync()
	schema := []syncArchive{{Step: 60, Points: 240}, {Step: 3600, Points: 48}}
	names := make([]string, 2048)
	schemas := map[string][]syncArchive{}
	for i := range names {
		n := fmt.Sprintf("differential.%04d", i)
		names[i] = n
		schemas[n] = schema
		for _, store := range []string{syncT, syncD1, syncD2, syncP} {
			f.put(store, n, schema)
		}
		for ai, a := range schema {
			from, until := syncWindow(syncTestNow, a, time.Hour)
			ts := from + int64((i+ai)%7)*a.Step
			if ts >= until {
				t.Fatal("empty fixture")
			}
			switch i % 8 {
			case 1:
				delete(f.store(syncT).points[n][a.Step], ts)
			case 2:
				f.store(syncT).points[n][a.Step][ts] = math.NaN()
			case 3:
				f.store(syncT).points[n][a.Step][ts] = math.Inf(1)
			case 4:
				f.store(syncD1).points[n][a.Step][ts] = math.Copysign(0, -1)
				f.store(syncT).points[n][a.Step][ts] = 0
			case 5:
				f.store(syncT).points[n][a.Step][ts] = 12345 // target wins
			case 6:
				delete(f.store(syncD2).points[n][a.Step], ts)
			case 7:
				f.store(syncD1).points[n][a.Step][ts] = math.SmallestNonzeroFloat64
			}
		}
		if i%17 == 0 {
			delete(f.store(syncP).archives, n)
		}
	}
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2, syncP}, settle: time.Hour}
	for _, legacy := range []string{"none", syncP, syncT} {
		t.Run(legacy, func(t *testing.T) {
			b := &sumSyncFixture{fakeSync: f, old: map[string]bool{legacy: true}}
			s := &syncer{o: o, b: b}
			for i, n := range names {
				s.masks.Store(n, uint64(1+(i%7)))
			}
			var want, got syncRow
			if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
				t.Fatal(err)
			}
			if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("verdict differs: got %d holes, want %d", len(got.Holes), len(want.Holes))
			}
			t.Logf("2048 names, %d ordered holes identical; digest matches=%d point comparisons=%d", len(got.Holes), s.prog.digests.Load(), s.prog.reads.Load())
		})
	}
}

func TestSyncSumMixedHolderSkipsTargetRead(t *testing.T) {
	f, o, _ := threeSources(t)
	b := &sumSyncFixture{fakeSync: f, old: map[string]bool{syncD1: true}}
	s := &syncer{o: o, b: b}
	s.masks.Store("a.clean", uint64(1))
	before := f.fetches.Load()
	var row syncRow
	if err := s.checkFullArchive(context.Background(), &row, []string{"a.clean"}, 60, syncTestNow-86400, syncTestNow-3600); err != nil {
		t.Fatal(err)
	}
	if len(row.Holes) != 0 || f.fetches.Load()-before != 1 {
		t.Fatalf("wanted one legacy read only: row=%+v reads=%d", row, f.fetches.Load()-before)
	}
}

func TestSyncLiveSumModeNegotiation(t *testing.T) {
	for _, mode := range []string{"sum-v1", "", "wrong"} {
		t.Run(mode, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct{ Mode string }
				_ = json.NewDecoder(r.Body).Decode(&req)
				if req.Mode != "sum-v1" {
					t.Errorf("mode=%q", req.Mode)
				}
				_ = json.NewEncoder(w).Encode([]syncDigestWire{{Name: "a", Mode: mode, Found: true, Archives: []syncDigest{{Step: 60, Count: 1, Hash: "0123456789abcdef", From: 120, Until: 240}}}})
			}))
			defer srv.Close()
			b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second})
			_, err := b.SumDigest(context.Background(), srv.URL, []string{"a"}, 60, 120, 240)
			if mode == "sum-v1" && err != nil {
				t.Fatal(err)
			}
			if mode != "sum-v1" && !errors.Is(err, errSyncNoDigest) {
				t.Fatalf("old mode accepted: %v", err)
			}
		})
	}
}

func TestSyncFullSumDifferentBoundsFallBack(t *testing.T) {
	f, o, _ := threeSources(t)
	b := &sumSyncFixture{fakeSync: f, old: map[string]bool{}, clipped: true}
	s := &syncer{o: o, b: b}
	s.masks.Store("b.holes", uint64(1))
	var want, got syncRow
	names := []string{"b.holes"}
	schemas := map[string][]syncArchive{"b.holes": syncTestSchema}
	if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) || s.prog.digests.Load() != 0 {
		t.Fatalf("incompatible bounds matched: %+v / %+v", got, want)
	}
}
