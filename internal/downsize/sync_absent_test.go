package downsize

import (
	"context"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

type absenceProbeFixture struct {
	*fakeSync
	legacy  bool
	callsMu sync.Mutex
	calls   map[string]map[string]int
}

func (f *absenceProbeFixture) record(store string, names []string) {
	f.callsMu.Lock()
	defer f.callsMu.Unlock()
	if f.calls[store] == nil {
		f.calls[store] = map[string]int{}
	}
	for _, n := range names {
		f.calls[store][n]++
	}
}

func (f *absenceProbeFixture) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	if f.legacy {
		return nil, errSyncNoDigest
	}
	f.record(store, names)
	b := servingSHA{&sumSyncFixture{fakeSync: f.fakeSync}}
	return b.Digest(ctx, store, names, from, until)
}

func (f *absenceProbeFixture) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	f.record(store, names)
	return f.fakeSync.Fetch(ctx, store, names, from, until)
}

func TestSyncAbsenceStopsOnlyAfterStorePresence(t *testing.T) {
	t.Parallel()
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "digest", true: "fetch"}[legacy], func(t *testing.T) {
			f := newFakeSync()
			schema := []syncArchive{{Step: 60, Points: 360}, {Step: 3600, Points: 100}}
			names := []string{"recent", "fine", "coarse", "empty", "gone"}
			for _, store := range []string{syncT, syncD1, syncP} {
				for _, n := range names[:4] {
					f.store(store).archives[n] = schema
					f.store(store).points[n] = map[int64]map[int64]float64{60: {}, 3600: {}}
				}
			}
			f.store(syncD1).points["recent"][3600][syncTestNow-7200] = 42
			f.store(syncD1).points["fine"][60][syncTestNow-3600] = 42
			f.store(syncD1).points["coarse"][3600][syncTestNow-48*3600] = 42
			// The first source being present must not suppress another source's proof.
			f.store(syncP).points["recent"][3600][syncTestNow-48*3600] = 42
			f.store(syncD1).points["empty"][60][syncTestNow-3600] = math.NaN()
			f.store(syncD1).points["empty"][3600][syncTestNow-48*3600] = math.Inf(1)
			b := &absenceProbeFixture{fakeSync: f, legacy: legacy, calls: map[string]map[string]int{}}
			s := &syncer{o: convergeOptions(t), b: b}
			info, err := f.Info(context.Background(), syncT, names)
			if err != nil {
				t.Fatal(err)
			}
			var row syncRow
			if err := s.resolveAbsent(context.Background(), &row, names, info); err != nil {
				t.Fatal(err)
			}
			want := []syncSkip{{"empty", "absent_all_sources"}, {"gone", "absent_all_sources"}}
			if !reflect.DeepEqual(row.Resolved, want) || len(row.Skipped) != 0 {
				t.Fatalf("absence verdict: %+v", row)
			}
			for n, wantMask := range map[string]uint64{"recent": 3, "fine": 1, "coarse": 1} {
				got, _ := s.masks.Load(n)
				if got != wantMask {
					t.Fatalf("%s holder mask=%v want %d", n, got, wantMask)
				}
			}
			wantCalls := map[string]int{"recent": 1, "fine": 2, "coarse": 3, "empty": 3, "gone": 1}
			if legacy {
				wantCalls = map[string]int{"recent": 2, "fine": 1, "coarse": 2, "empty": 2, "gone": 1}
			}
			if !reflect.DeepEqual(b.calls[syncD1], wantCalls) {
				t.Fatalf("source name reads=%v want %v", b.calls[syncD1], wantCalls)
			}
			wantPeerCalls := 3
			if legacy {
				wantPeerCalls = 2
			}
			if b.calls[syncP]["recent"] != wantPeerCalls {
				t.Fatalf("second source not fully checked: %v", b.calls[syncP])
			}
		})
	}
}

func TestSyncPresenceShortcutStillVerifiesOlderHoles(t *testing.T) {
	t.Parallel()
	f := newFakeSync()
	schema := []syncArchive{{Step: 60, Points: 360}, {Step: 3600, Points: 100}}
	for _, store := range []string{syncT, syncP} {
		f.store(store).archives["metric"] = schema
		f.store(store).points["metric"] = map[int64]map[int64]float64{60: {syncTestNow - 3600: 42}, 3600: {}}
	}
	f.store(syncP).points["metric"][3600][syncTestNow-48*3600] = math.Copysign(0, -1)
	o := convergeOptions(t)
	o.settle = time.Hour
	s := &syncer{o: o, b: servingSync{f}}
	s.masks.Store("metric", uint64(2))
	row, err := s.verifyBatch(context.Background(), 1, []string{"metric"})
	if err != nil {
		t.Fatal(err)
	}
	if len(row.Holes) == 0 || len(row.Resolved) != 0 {
		t.Fatalf("presence shortcut hid old archive hole: %+v", row)
	}
}
