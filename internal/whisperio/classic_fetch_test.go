package whisperio

import (
	"context"
	"math"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func classicFetchFixture(tb testing.TB) (mapContinuationIO, store.Metadata, func() time.Time) {
	tb.Helper()
	clock := func() time.Time { return time.Unix(1000, 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "classic.fetch", Retentions: []store.Retention{{Step: 1, Count: 263}, {Step: 10, Count: 99}, {Step: 60, Count: 37}}, AggregationMethod: store.Average}}
	kv := mapContinuationIO{}
	snap := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(m.Retentions))}
	for ai, r := range m.Retentions {
		snap.Archives[ai].Retention = r
		for ts := max(r.Step, 1000-r.MaxRetention()+1); ts <= 1000; ts += r.Step {
			snap.Archives[ai].Points = append(snap.Archives[ai].Points, store.Point{Timestamp: int64(ts / r.Step * r.Step), Value: float64(ts)})
		}
	}
	if err := (&ContinuationCodec{}).RestoreArchives(context.Background(), kv, snap, clock); err != nil {
		tb.Fatal(err)
	}
	return kv, m, clock
}

func TestClassicFetchBitExact(t *testing.T) {
	kv, m, clock := classicFetchFixture(t)
	rng := rand.New(rand.NewSource(420))
	bits := []uint64{0, 1, 0x8000000000000000, 0x7ff8000000000123, 0x7ff0000000000000, 0xfff0000000000000}
	for cycle := 0; cycle < 4; cycle++ {
		// Exercise malformed legacy anchors, absent slot zero, holes, partial
		// final groups, uint32 timestamp truncation and every float bit.
		fs := newContinuationFiles(kv, false)
		file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		f := file.(*continuationFile)
		for ai, a := range f.data.layout.Archives {
			for gi := 0; gi*128 < a.Count; gi++ {
				var group store.PointBlock
				for i := 0; i < min(128, a.Count-gi*128); i++ {
					if rng.Intn(4) == 0 || cycle == 3 && gi == 0 && i == 0 {
						continue
					}
					ts := int64((rng.Intn(2*a.Count)-a.Count)*a.Step + 600)
					if cycle == 1 {
						ts++
					}
					if cycle == 2 {
						ts += 1 << 32
					}
					group.Present[i/64] |= uint64(1) << uint(i%64)
					group.Points[i] = store.Point{Timestamp: ts, Value: math.Float64frombits(bits[rng.Intn(len(bits))])}
				}
				if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, gi), &group); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := fs.flush(); err != nil {
			t.Fatal(err)
		}
		file.Close()
		windows := [][2]int{{0, 0}, {-1000, 1000}, {0, 1000}, {999, 999}, {1000, 1000}, {1001, 1002}, {1000, 900}, {-3000, -2900}}
		for range 200 {
			from := rng.Intn(3500) - 2300
			windows = append(windows, [2]int{from, from + rng.Intn(2500)})
		}
		for _, window := range windows {
			fs := newContinuationFiles(kv, true)
			w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
			if err != nil {
				t.Fatal(err)
			}
			want, wantErr := w.Fetch(window[0], window[1])
			w.Close()
			got, gotErr := (&ContinuationCodec{}).Fetch(context.Background(), &snapshotProbeIO{mapContinuationIO: kv}, m, window[0], window[1], clock)
			if (wantErr == nil) != (gotErr == nil) || (want == nil) != (got == nil) {
				t.Fatalf("cycle %d window %v: %v/%v vs %v/%v", cycle, window, got, gotErr, want, wantErr)
			}
			if want == nil {
				continue
			}
			if got.FromTime != want.FromTime() || got.UntilTime != want.UntilTime() || got.Step != want.Step() || len(got.Values) != len(want.Values()) {
				t.Fatalf("cycle %d window %v shape differs: %d/%d/%d/%d vs %d/%d/%d/%d", cycle, window, got.FromTime, got.UntilTime, got.Step, len(got.Values), want.FromTime(), want.UntilTime(), want.Step(), len(want.Values()))
			}
			for i, v := range want.Values() {
				if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
					t.Fatalf("cycle %d window %v slot %d: %x vs %x", cycle, window, i, math.Float64bits(got.Values[i]), math.Float64bits(v))
				}
			}
		}
		for _, window := range [][2]int{{0, 0}, {1, 900}, {751, 998}, {0, 60}} {
			want, _ := sumOracle(t, kv, m, 1000, window[0], window[1])
			got, err := (&ContinuationCodec{}).SumDigest(context.Background(), &snapshotProbeIO{mapContinuationIO: kv}, m, window[0], window[1], clock)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("cycle %d digest %v: %+v/%v vs %+v", cycle, window, got, err, want)
			}
		}
	}
}

func BenchmarkClassicCanonicalFetch(b *testing.B) {
	const now = 1700000000
	clock := func() time.Time { return time.Unix(now, 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "bench.fetch", Retentions: []store.Retention{{Step: 1, Count: 172800}}, AggregationMethod: store.Average}}
	kv := mapContinuationIO{}
	snapshot := store.Snapshot{Metadata: m, Archives: []store.Archive{{Retention: m.Retentions[0]}}}
	for ts := now - 100000; ts <= now; ts++ {
		if ts%7 != 0 {
			snapshot.Archives[0].Points = append(snapshot.Archives[0].Points, store.Point{Timestamp: int64(ts), Value: float64(ts % 97)})
		}
	}
	if err := (&ContinuationCodec{}).RestoreArchives(context.Background(), kv, snapshot, clock); err != nil {
		b.Fatal(err)
	}
	for _, typed := range []bool{false, true} {
		name := "reference"
		if typed {
			name = "typed"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				fs := newContinuationFiles(kv, true)
				w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
				if err != nil {
					b.Fatal(err)
				}
				if typed {
					_, err = fetchClassic(context.Background(), w, m, now, now-86400, now)
				} else {
					_, err = w.Fetch(now-86400, now)
				}
				w.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
