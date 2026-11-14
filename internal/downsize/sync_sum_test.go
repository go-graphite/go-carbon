package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
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

func TestSyncSumMixedHolderSkipsPointReads(t *testing.T) {
	f, o, _ := threeSources(t)
	b := &sumSyncFixture{fakeSync: f, old: map[string]bool{syncD1: true}}
	s := &syncer{o: o, b: b}
	s.masks.Store("a.clean", uint64(1))
	before := f.fetches.Load()
	var row syncRow
	if err := s.checkFullArchive(context.Background(), &row, []string{"a.clean"}, 60, syncTestNow-86400, syncTestNow-3600); err != nil {
		t.Fatal(err)
	}
	if len(row.Holes) != 0 || f.fetches.Load()-before != 0 {
		t.Fatalf("matching mixed-protocol digests caused point reads: row=%+v reads=%d", row, f.fetches.Load()-before)
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

// servingSHA models an older server which supports SHA but not sum-v1.
// Its digest covers the same single serving archive as a full point read.
type servingSHA struct{ *sumSyncFixture }

func (f *servingSHA) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	points, err := f.fakeSync.Fetch(ctx, store, names, from, until)
	if err != nil {
		return nil, err
	}
	out := map[string][]syncDigest{}
	for n, series := range points {
		h := sha256.New()
		var buf [16]byte
		var count int64
		for i, v := range series.Values {
			ts := series.Start + int64(i)*series.Step
			if ts < from || ts >= until || math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			binary.BigEndian.PutUint64(buf[:8], uint64(ts))
			binary.BigEndian.PutUint64(buf[8:], math.Float64bits(v))
			_, _ = h.Write(buf[:])
			count++
		}
		out[n] = []syncDigest{{Step: series.Step, Count: count, Hash: hex.EncodeToString(h.Sum(nil)[:8])}}
	}
	return out, nil
}

func TestSyncFullLegacySHADifferential(t *testing.T) {
	f, o, _ := threeSources(t)
	b := &servingSHA{&sumSyncFixture{fakeSync: f, old: map[string]bool{syncT: true, syncD1: true, syncD2: true, syncP: true}}}
	s := &syncer{o: o, b: b}
	names := []string{"a.clean", "b.holes"}
	schemas := map[string][]syncArchive{}
	for _, n := range names {
		s.masks.Store(n, uint64(7))
		schemas[n] = syncTestSchema
	}
	var want, got syncRow
	if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("legacy SHA changed verdict: got %+v want %+v", got, want)
	}
	if s.prog.digests.Load() == 0 {
		t.Fatal("legacy SHA never avoided point comparison")
	}
}

func TestSyncFullLegacySHAEmptySource(t *testing.T) {
	f, o, _ := threeSources(t)
	for _, src := range o.from {
		f.put(src, "a.clean", syncTestSchema)
		for _, a := range syncTestSchema {
			f.store(src).points["a.clean"][a.Step] = map[int64]float64{}
		}
	}
	b := &servingSHA{&sumSyncFixture{fakeSync: f, old: map[string]bool{syncT: true, syncD1: true, syncD2: true, syncP: true}}}
	s := &syncer{o: o, b: b}
	s.masks.Store("a.clean", uint64(7))
	var row syncRow
	if err := s.checkFull(context.Background(), &row, []string{"a.clean"}, map[string][]syncArchive{"a.clean": syncTestSchema}, syncTestNow); err != nil {
		t.Fatal(err)
	}
	if len(row.Holes) != 0 || s.prog.reads.Load() != 0 {
		t.Fatalf("empty source caused detail reads: %+v reads=%d", row, s.prog.reads.Load())
	}
}

// Count target SHA calls separately from fixture point reads.
type countedTargetSHA struct {
	*servingSHA
	calls int
}

func (f *countedTargetSHA) Digest(ctx context.Context, host string, names []string, from, until int64) (map[string][]syncDigest, error) {
	if host == syncT {
		f.calls++
	}
	return f.servingSHA.Digest(ctx, host, names, from, until)
}
func TestSyncLegacyTargetReadOnceAfterNegotiation(t *testing.T) {
	f, o, _ := threeSources(t)
	b := &countedTargetSHA{servingSHA: &servingSHA{&sumSyncFixture{fakeSync: f, old: map[string]bool{syncT: true, syncD1: true, syncD2: true, syncP: true}}}}
	s := &syncer{o: o, b: b}
	s.masks.Store("b.holes", uint64(7))
	names := []string{"b.holes"}
	schemas := map[string][]syncArchive{"b.holes": syncTestSchema}
	var want syncRow
	if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		var got syncRow
		if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("local target changed holes: got%+v want%+v", got, want)
		}
	}
	if b.calls != 1 {
		t.Fatalf("target SHA calls=%d want1", b.calls)
	}
}

func TestSyncFullTargetWithoutDigestEndpoints(t *testing.T) {
	f, o, _ := threeSources(t)
	f.noDigest[syncT] = true
	b := &sumSyncFixture{fakeSync: f, old: map[string]bool{syncT: true}}
	s := &syncer{o: o, b: b}
	names := []string{"a.clean", "b.holes", "c.peerhole"}
	schemas := map[string][]syncArchive{}
	for _, n := range names {
		s.masks.Store(n, uint64(7))
		schemas[n] = syncTestSchema
	}
	var want, got syncRow
	if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("digest-unavailable target changed holes: got %+v want %+v", got, want)
	}
}

// The serving SHA fixture computes the wire fingerprint independently of the
// additive sum fixture. Count only Fetch calls made by the controller itself.
type countedMixedFull struct {
	*servingSHA
	detail atomic.Int64
}

func (f *countedMixedFull) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	f.detail.Add(int64(len(names)))
	return f.fakeSync.Fetch(ctx, store, names, from, until)
}
func TestSyncMixedSHADigestBeforePointReads(t *testing.T) {
	for _, holes := range []bool{false, true} {
		t.Run(fmt.Sprint(holes), func(t *testing.T) {
			f, o, _ := threeSources(t)
			names := []string{"a.clean", "b.holes", "c.peerhole"}
			for _, store := range append([]string{syncT}, o.from...) {
				for _, n := range names {
					f.put(store, n, syncTestSchema)
				}
			}
			if holes {
				delete(f.store(syncT).points["b.holes"][60], syncTestNow-86400)
			}
			b := &countedMixedFull{servingSHA: &servingSHA{&sumSyncFixture{fakeSync: f, old: map[string]bool{syncD1: true, syncD2: true, syncP: true}}}}
			s := &syncer{o: o, b: b}
			schemas := map[string][]syncArchive{}
			for _, n := range names {
				s.masks.Store(n, uint64(7))
				schemas[n] = syncTestSchema
			}
			var want, got syncRow
			if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
				t.Fatal(err)
			}
			b.detail.Store(0)
			if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("mixed SHA changed ordered verdict: got %+v want %+v", got, want)
			}
			if !holes && b.detail.Load() != 0 {
				t.Fatalf("matching SHA fetched %d names", b.detail.Load())
			}
			if holes && (b.detail.Load() == 0 || len(got.Holes) == 0) {
				t.Fatal("SHA mismatch did not read points and retain holes")
			}
		})
	}
}

// Count controller target reads separately from the fixture's SHA oracle.
type absentFullSource struct {
	*countedMixedFull
	targetReads atomic.Int64
}

func (b *absentFullSource) Fetch(ctx context.Context, host string, names []string, from, until int64) (map[string]syncSeries, error) {
	if host == syncT {
		b.targetReads.Add(int64(len(names)))
	}
	return b.countedMixedFull.Fetch(ctx, host, names, from, until)
}

func TestSyncFullAbsentLegacySourceDifferential(t *testing.T) {
	for _, holes := range []bool{false, true} {
		t.Run(fmt.Sprint(holes), func(t *testing.T) {
			f, o, _ := threeSources(t)
			names := []string{"a.clean", "b.holes", "c.peerhole"}
			schemas := map[string][]syncArchive{}
			for _, n := range names {
				schemas[n] = syncTestSchema
				for _, host := range append([]string{syncT}, o.from...) {
					f.put(host, n, syncTestSchema)
				}
				// The discovery mask can outlive a donor's source retirement.
				delete(f.store(syncD1).archives, n)
				delete(f.store(syncP).archives, n)
			}
			if holes {
				delete(f.store(syncT).points["b.holes"][60], syncTestNow-86400)
			}
			b := &absentFullSource{countedMixedFull: &countedMixedFull{servingSHA: &servingSHA{&sumSyncFixture{fakeSync: f, old: map[string]bool{syncD1: true, syncP: true}}}}}
			s := &syncer{o: o, b: b}
			for _, n := range names {
				s.masks.Store(n, uint64(7))
			}
			var want, got syncRow
			if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
				t.Fatal(err)
			}
			b.targetReads.Store(0)
			if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
				t.Fatal(err)
			}
			a, _ := json.Marshal(want)
			z, _ := json.Marshal(got)
			if string(a) != string(z) {
				t.Fatalf("ordered verdict bytes differ: got %s want %s", z, a)
			}
			if !holes && b.targetReads.Load() != 0 {
				t.Fatalf("absent legacy holders caused %d target archive reads", b.targetReads.Load())
			}
			if holes && (b.targetReads.Load() == 0 || len(got.Holes) == 0) {
				t.Fatal("real hole did not retain target read and verdict")
			}
		})
	}
}
