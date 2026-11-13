package chunkstore_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// mergeBenchConfig is a prod-like three-archive schema: 1-minute points for
// 30 days, 10-minute points for 180 days and hourly points for 2 years.
func mergeBenchConfig(name string) store.MetricConfig {
	return store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 43200}, {Step: 600, Count: 25920}, {Step: 3600, Count: 17520}}, AggregationMethod: store.Average}
}

type mergeBenchFixture struct {
	now     time.Time
	handoff int64
	source  []store.TransferMetric
}

func openMergeBenchStore(tb testing.TB, root string, clock func() time.Time) *store.Store {
	tb.Helper()
	if err := os.MkdirAll(filepath.Join(root, "originals"), 0750); err != nil {
		tb.Fatal(err)
	}
	s, err := store.Open(filepath.Join(root, "pool"), store.Options{ExactBlocks: true, CacheSize: 64 << 20, MemTableSize: 64 << 20, SyncInterval: time.Hour, Now: clock, Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true}, IncrementalSource: filepath.Join(root, "originals"), TimeBuckets: true})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

// newMergeBenchFixture writes names whose history ends at the input handoff
// (now-6h, overlap=false) or at now (overlap=true) and exports them.
func newMergeBenchFixture(tb testing.TB, names int, overlap bool) mergeBenchFixture {
	tb.Helper()
	ctx := context.Background()
	now := time.Unix(1763000000, 0)
	end := now.Unix() - now.Unix()%3600
	handoff := end - 6*3600
	last := handoff
	if overlap {
		last = end
	}
	clock := time.Unix(last, 0)
	source := openMergeBenchStore(tb, filepath.Join(tb.TempDir(), "source"), func() time.Time { return clock })
	base := make([]string, names)
	for i := range base {
		base[i] = fmt.Sprintf("bench.merge.host%04d.if%02d.octets", i, i%16)
		if _, err := source.Create(ctx, mergeBenchConfig(base[i])); err != nil {
			tb.Fatal(err)
		}
		r := rand.New(rand.NewSource(int64(i)))
		level := 1e6 * r.Float64()
		var points []store.Point
		for ts := last - 2*365*86400; ts < last; ts += 60 {
			// Coarse history keeps one point per retained step, as a
			// downsampled real archive does.
			switch age := last - ts; {
			case age > 180*86400 && ts%3600 != 0, age > 30*86400 && ts%600 != 0:
				continue
			}
			level = math.Max(0, level+1000*r.NormFloat64())
			points = append(points, store.Point{Timestamp: ts, Value: math.Round(level)})
		}
		if err := source.UpdateMany(ctx, base[i], points); err != nil {
			tb.Fatal(err)
		}
	}
	exported, err := source.ExportTransfer(ctx, base)
	if err != nil {
		tb.Fatal(err)
	}
	return mergeBenchFixture{now: now, handoff: handoff, source: exported}
}

// clone renames the exported records so each round imports fresh names.
func (f mergeBenchFixture) clone(prefix string) []store.TransferMetric {
	out := make([]store.TransferMetric, len(f.source))
	for i, r := range f.source {
		r.Config.Name = prefix + "." + r.Config.Name
		r.Version.Name = r.Config.Name
		r.Keys = slices.Clone(r.Keys)
		out[i] = r
	}
	return out
}

// seedTarget makes every name live on the target with 1-minute points since
// the handoff, as live input produces after the store-input handoff.
func (f mergeBenchFixture) seedTarget(tb testing.TB, target *store.Store, records []store.TransferMetric) {
	tb.Helper()
	ctx := context.Background()
	for _, r := range records {
		if _, err := target.Create(ctx, mergeBenchConfig(r.Config.Name)); err != nil {
			tb.Fatal(err)
		}
		var points []store.Point
		for ts := f.handoff; ts < f.now.Unix(); ts += 60 {
			points = append(points, store.Point{Timestamp: ts, Value: float64(ts % 977)})
		}
		if err := target.UpdateMany(ctx, r.Config.Name, points); err != nil {
			tb.Fatal(err)
		}
	}
}

// BenchmarkTransferImportPresent measures ImportTransfer CPU per name for
// names absent from the target, present with live points after the source's
// last point, and present with live points overlapping the source.
func BenchmarkTransferImportPresent(b *testing.B) {
	const names = 32
	for _, mode := range []struct {
		name             string
		present, overlap bool
	}{{"absent", false, false}, {"present", true, false}, {"overlap", true, true}} {
		b.Run(mode.name, func(b *testing.B) {
			fixture := newMergeBenchFixture(b, names, mode.overlap)
			now := fixture.now
			target := openMergeBenchStore(b, filepath.Join(b.TempDir(), "target"), func() time.Time { return now })
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				records := fixture.clone(fmt.Sprintf("r%06d", i))
				if mode.present {
					fixture.seedTarget(b, target, records)
				}
				b.StartTimer()
				if _, err := target.ImportTransfer(ctx, records); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/1e3/float64(b.N*names), "ms/name")
		})
	}
}

// BenchmarkTransferImportPresentStreams imports present (disjoint) names over
// 16, 32 and 64 concurrent transfers and reports names/s. Each stream imports
// its own fresh names, seeded live on the target first.
func BenchmarkTransferImportPresentStreams(b *testing.B) {
	const names = 32
	fixture := newMergeBenchFixture(b, names, false)
	now := fixture.now
	target := openMergeBenchStore(b, filepath.Join(b.TempDir(), "target"), func() time.Time { return now })
	ctx := context.Background()
	round := 0
	for _, streams := range []int{16, 32, 64} {
		b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
			var total time.Duration
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				batches := make([][]store.TransferMetric, streams)
				for s := range batches {
					round++
					batches[s] = fixture.clone(fmt.Sprintf("s%06d", round))
					fixture.seedTarget(b, target, batches[s])
				}
				b.StartTimer()
				start := time.Now()
				errs := make(chan error, streams)
				for _, records := range batches {
					go func(records []store.TransferMetric) {
						_, err := target.ImportTransfer(ctx, records)
						errs <- err
					}(records)
				}
				for range batches {
					if err := <-errs; err != nil {
						b.Fatal(err)
					}
				}
				total += time.Since(start)
			}
			b.ReportMetric(float64(b.N*streams*names)/total.Seconds(), "names/s")
		})
	}
}
