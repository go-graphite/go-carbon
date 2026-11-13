package whisperio

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func prependFixture(t testing.TB, count, step, start, size int) (mapContinuationIO, store.Snapshot) {
	t.Helper()
	r := store.Retention{Step: step, Count: count}
	slots := map[int]store.Point{}
	for j := 0; j < size; j++ {
		slots[j] = store.Point{Timestamp: int64(start + j*step), Value: math.Float64frombits([]uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x4024000000000000}[j%6])}
	}
	kv, m := placedClassicImage(t, []store.Retention{r}, []map[int]store.Point{slots})
	snap, err := (&ContinuationCodec{}).Snapshot(context.Background(), kv, m, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	return kv, snap
}
func prependDigest(s store.Snapshot) (string, []int) {
	h := sha256.New()
	var w [16]byte
	counts := make([]int, len(s.Archives))
	for ai, a := range s.Archives {
		counts[ai] = len(a.Points)
		binary.BigEndian.PutUint64(w[:8], uint64(len(a.Points)))
		h.Write(w[:8])
		for _, p := range a.Points {
			binary.BigEndian.PutUint64(w[:8], uint64(p.Timestamp))
			binary.BigEndian.PutUint64(w[8:], math.Float64bits(p.Value))
			h.Write(w[:])
		}
	}
	return hex.EncodeToString(h.Sum(nil)), counts
}
func TestPrependArchivesDifferential(t *testing.T) {
	for _, count := range []int{1, 2, 127, 128, 129, 341, 513, 1000} {
		for _, step := range []int{1, 10, 60, 3600} {
			for _, kind := range []string{"empty", "older", "alias", "overlap", "nan"} {
				t.Run(fmt.Sprintf("%d/%d/%s", count, step, kind), func(t *testing.T) {
					targetSize := min(count, 31)
					if kind == "empty" {
						targetSize = 0
					}
					target, ts := prependFixture(t, count, step, 100000*step, targetSize)
					start := 100000 - min(count, 41)
					if kind == "alias" {
						start = 100000 - count
					}
					if kind == "overlap" {
						start = 100000
					}
					_, source := prependFixture(t, count, step, start*step, min(count, 17))
					if kind == "nan" {
						source.Archives[0].Points[0].Value = math.NaN()
					}
					dst := mapContinuationIO{}
					c := &ContinuationCodec{}
					digest, counts, ok, err := c.prependClassicTarget(context.Background(), target, dst, source, ts.Metadata, classicTestClock)
					if err != nil {
						t.Fatal(err)
					}
					if kind == "overlap" || kind == "nan" {
						if ok || len(dst) > 0 {
							t.Fatal("expected unchanged fallback")
						}
						return
					}
					if !ok {
						t.Fatal("fast path declined")
					}
					// Real imports using fast/old codecs supply the independent finite-slot oracle.
					var snapshots []store.Snapshot
					for _, old := range []bool{true, false} {
						opts := coldOptions(&ContinuationCodec{keepPrependArchives: old})
						opts.Now = classicTestClock
						db, err := store.Open(t.TempDir(), opts)
						if err != nil {
							t.Fatal(err)
						}
						db.StopBackground()
						if _, err = db.ImportTransfer(context.Background(), []store.TransferMetric{classicTransfer(target, ts)}); err != nil {
							t.Fatal(err)
						}
						sourceKV := compressedPrependFixture(t, source)
						if _, err = db.ImportTransfer(context.Background(), []store.TransferMetric{classicTransfer(sourceKV, source)}); err != nil {
							t.Fatal(err)
						}
						snap, err := db.Snapshot(context.Background(), ts.Metadata.Name)
						if err != nil {
							t.Fatal(err)
						}
						snapshots = append(snapshots, snap)
						db.Close()
					}
					got, err := c.Snapshot(context.Background(), dst, ts.Metadata, classicTestClock)
					if err != nil {
						t.Fatal(err)
					}
					requireSnapshotPointBits(t, snapshots[0], got)
					requireSnapshotPointBits(t, snapshots[0], snapshots[1])
					wantDigest, wantCounts := prependDigest(got)
					if digest != wantDigest || counts[0] != wantCounts[0] {
						t.Fatal("receipt mismatch")
					}
				})
			}
		}
	}
}
func BenchmarkPrependImport(b *testing.B) {
	target, ts := prependFixture(b, 50400, 60, 100000*60, 2000)
	_, source := prependFixture(b, 50400, 60, 50000*60, 40000)
	sourceKV := compressedPrependFixture(b, source)
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprintf("reference=%t", old), func(b *testing.B) {
			opts := coldOptions(&ContinuationCodec{keepPrependArchives: old})
			opts.Now = classicTestClock
			db, err := store.Open(b.TempDir(), opts)
			if err != nil {
				b.Fatal(err)
			}
			db.StopBackground()
			defer db.Close()
			b.ReportAllocs()
			var cpuNS int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ts.Metadata.Name = fmt.Sprintf("bench.%d", i)
				source.Metadata.Name = ts.Metadata.Name
				if _, err = db.ImportTransfer(context.Background(), []store.TransferMetric{classicTransfer(target, ts)}); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				beganCPU := prependProcessCPU()
				if _, err = db.ImportTransfer(context.Background(), []store.TransferMetric{classicTransfer(sourceKV, source)}); err != nil {
					b.Fatal(err)
				}
				cpuNS += prependProcessCPU() - beganCPU
			}
			b.ReportMetric(float64(cpuNS)/float64(b.N), "CPU-ns/name")
		})
	}
}

func compressedPrependFixture(t testing.TB, source store.Snapshot) mapContinuationIO {
	t.Helper()
	kv := mapContinuationIO{}
	c := &ContinuationCodec{}
	var at int64
	for _, a := range source.Archives {
		for _, p := range a.Points {
			at = max(at, p.Timestamp)
		}
	}
	clock := func() time.Time { return time.Unix(at, 0) }
	if err := c.Create(context.Background(), kv, source.Metadata, clock); err != nil {
		t.Fatal(err)
	}
	fs := newContinuationFiles(kv, false).withClock(clock)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		t.Fatal(err)
	}
	for ai, a := range source.Archives {
		points := make([]whisper.TimeSeriesPoint, len(a.Points))
		for j, p := range a.Points {
			points[j] = whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value}
		}
		if err := w.AppendArchivePoints(ai, points); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.flush(); err != nil {
		t.Fatal(err)
	}

	got, err := c.Snapshot(context.Background(), kv, source.Metadata, clock)
	if err != nil {
		t.Fatal(err)
	}
	requireSnapshotPointBits(t, source, got)
	return kv
}

func TestPrependArchivesLayouts(t *testing.T) {
	layouts := [][]store.Retention{
		{{Step: 1, Count: 129}, {Step: 10, Count: 341}, {Step: 60, Count: 1000}},
		{{Step: 60, Count: 1440}, {Step: 600, Count: 1008}, {Step: 3600, Count: 8760}},
	}
	for li, rets := range layouts {
		for _, wrapped := range []bool{false, true} {
			for _, overlap := range []bool{false, true} {
				t.Run(fmt.Sprintf("layout%d/wrap%t/overlap%t", li, wrapped, overlap), func(t *testing.T) {
					slots := make([]map[int]store.Point, len(rets))
					srcSlots := make([]map[int]store.Point, len(rets))
					for ai, r := range rets {
						base := int64(1_800_000_000)
						slots[ai] = map[int]store.Point{0: {Timestamp: base, Value: math.Copysign(0, -1)}, 1: {Timestamp: base + int64(r.Step), Value: math.Inf(1)}}
						if wrapped {
							slots[ai][r.Count-2] = store.Point{Timestamp: base - int64(2*r.Step), Value: 12.5}
						}
						srcSlots[ai] = map[int]store.Point{}
						for j := 0; j < min(r.Count, 150); j++ {
							ts := base - int64(10*rets[len(rets)-1].Step) - int64((min(r.Count, 150)-1-j)*r.Step)
							if overlap && ai == len(rets)-1 {
								ts = base + int64(j*r.Step)
							}
							srcSlots[ai][j] = store.Point{Timestamp: ts, Value: float64(j) + .125}
						}
					}
					target, m := placedClassicImage(t, rets, slots)
					sourceKV, sm := placedClassicImage(t, rets, srcSlots)
					c := &ContinuationCodec{}
					source, err := c.Snapshot(context.Background(), sourceKV, sm, classicTestClock)
					if err != nil {
						t.Fatal(err)
					}
					sourceKV = compressedPrependFixture(t, source)
					dst := mapContinuationIO{"sentinel": {1}}
					_, _, ok, err := c.PrependArchives(context.Background(), sourceKV, target, dst, source, m, classicTestClock)
					if err != nil {
						t.Fatal(err)
					}
					if ok == overlap {
						t.Fatal("wrong fast-path eligibility")
					}
					if overlap {
						if len(dst) != 1 || dst["sentinel"][0] != 1 {
							t.Fatal("fallback mutated destination")
						}
						return
					}
					targetSnap, err := c.Snapshot(context.Background(), target, m, classicTestClock)
					if err != nil {
						t.Fatal(err)
					}
					var want store.Snapshot
					for _, old := range []bool{true, false} {
						opts := coldOptions(&ContinuationCodec{keepPrependArchives: old})
						opts.Now = classicTestClock
						db, err := store.Open(t.TempDir(), opts)
						if err != nil {
							t.Fatal(err)
						}
						db.StopBackground()
						for _, record := range []store.TransferMetric{classicTransfer(target, targetSnap), classicTransfer(sourceKV, source)} {
							if _, err = db.ImportTransfer(context.Background(), []store.TransferMetric{record}); err != nil {
								t.Fatal(err)
							}
						}
						snap, err := db.Snapshot(context.Background(), m.Name)
						if err != nil {
							t.Fatal(err)
						}
						db.Close()
						if old {
							want = snap
						} else {
							requireSnapshotPointBits(t, want, snap)
						}
					}
					got, err := c.Snapshot(context.Background(), dst, m, classicTestClock)
					if err != nil {
						t.Fatal(err)
					}
					requireSnapshotPointBits(t, want, got)
				})
			}
		}
	}
}
