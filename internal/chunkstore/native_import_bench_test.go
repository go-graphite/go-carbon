package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Force the existing full-archive fallback on the same physical fixture. This
// isolates its cost from splice eligibility; it is not a production trace.
type materializingBenchCodec struct{ *whisperio.ContinuationCodec }

func (*materializingBenchCodec) CombineNative(context.Context, store.ContinuationIO, store.ContinuationIO, store.Metadata, func() time.Time) ([]store.RelayoutKV, store.NativeCombineClass, bool, error) {
	return nil, store.NativeCombineClass{}, false, nil
}
func (*materializingBenchCodec) NativeClassicBlocks(context.Context, store.ContinuationIO, store.Metadata, func() time.Time) ([]store.RelayoutKV, bool, error) {
	return nil, false, nil
}

// Compare identical 55,296-slot names with full overlap and one foreground tie.
// Source encoding, fixture setup, full bit checks and closing are outside timing.
// Timing includes staging, preparation, SST ingestion, publication and retirement.
// No production source is opened. Run -benchtime=1x -count=3; b.TempDir cleans all
// data. Throughput is target-only: serialized donor export is not in this timer.
func BenchmarkNativeImportConcurrency(b *testing.B) {
	const count, nameCount = 55_296, 256
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(2*count, 0) }
	open := func(b *testing.B, imports, workers int, materialize bool) *store.Store {
		b.Helper()
		var codec store.ContinuationCodec = &whisperio.ContinuationCodec{}
		if materialize {
			codec = &materializingBenchCodec{&whisperio.ContinuationCodec{}}
		}
		s, err := store.Open(b.TempDir(), store.Options{NativeImportConcurrency: imports, NativeCombineWorkers: workers, NativeCombineMemory: int64(imports*workers) << 30, ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: clock, CacheSize: 8 << 20, MemTableSize: 8 << 20})
		if err != nil {
			b.Fatal(err)
		}
		s.StopBackground()
		return s
	}
	source := open(b, 1, 2, false)
	defer source.Close()
	names := make([]string, nameCount)
	for i := range names {
		names[i] = fmt.Sprintf("import.fixture.%03d", i)
		cfg := store.MetricConfig{Name: names[i], AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 1, Count: count}}}
		if err := seedLargeContinuation(source, cfg, clock); err != nil {
			b.Fatal(err)
		}
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		b.Fatal(err)
	}
	defer pin.Close()
	for _, materialize := range []bool{false, true} {
		for _, shape := range [][2]int{{1, 4}, {1, 8}, {1, 16}, {1, 32}, {2, 4}, {4, 4}, {4, 8}, {8, 8}} {
			imports, workers := shape[0], shape[1]
			b.Run(fmt.Sprintf("materialize=%t/imports=%d/workers=%d", materialize, imports, workers), func(b *testing.B) {
				b.StopTimer()
				wires := make([][]byte, imports)
				requests := make([]store.NativeRange, imports)
				for j := range imports {
					group := names[j*nameCount/imports : (j+1)*nameCount/imports]
					var buf bytes.Buffer
					if err := pin.Write(ctx, group, &buf); err != nil {
						b.Fatal(err)
					}
					wires[j] = buf.Bytes()
					requests[j] = store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: fmt.Sprintf("%064x", j+3), Source: "donor", End: int64(len(group)), Names: group, Combine: true}
				}
				var elapsed time.Duration
				for range b.N {
					target := open(b, imports, workers, materialize)
					for _, name := range names {
						cfg := store.MetricConfig{Name: name, AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 1, Count: count}}}
						if err := seedLargeContinuation(target, cfg, clock); err != nil {
							b.Fatal(err)
						}
						if err := target.UpdateMany(ctx, name, []store.Point{{Timestamp: count, Value: 42}}); err != nil {
							b.Fatal(err)
						}
					}
					errs := make(chan error, imports)
					var wg sync.WaitGroup
					b.StartTimer()
					start := time.Now()
					for j := range imports {
						wg.Add(1)
						go func(j int) {
							defer wg.Done()
							r, err := target.NativeImport(ctx, requests[j], bytes.NewReader(wires[j]))
							if err == nil && (r.CopiedCount != len(requests[j].Names) || r.DeferredCount != 0) {
								err = fmt.Errorf("incomplete receipt: %+v", r)
							}
							errs <- err
						}(j)
					}
					wg.Wait()
					elapsed += time.Since(start)
					b.StopTimer()
					for range imports {
						if err := <-errs; err != nil {
							b.Fatal(err)
						}
					}
					// Every point of every name, including the target-wins tie.
					for _, name := range names {
						snap, err := target.Snapshot(ctx, name)
						if err != nil {
							b.Fatal(err)
						}
						if len(snap.Archives) != 1 || len(snap.Archives[0].Points) != count {
							b.Fatal("lost history", name)
						}
						for i, p := range snap.Archives[0].Points {
							want := float64(1)
							if i == 0 {
								want = 42
							}
							if p.Timestamp != int64(count+i) || math.Float64bits(p.Value) != math.Float64bits(want) {
								b.Fatal("changed point", name, i)
							}
						}
						if _, err := target.VerifyTransferFetch(ctx, snap); err != nil {
							b.Fatal(name, err)
						}
					}
					if err := target.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(nameCount*b.N)/elapsed.Seconds(), "names/s")
			})
		}
	}
}
