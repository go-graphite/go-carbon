package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Counts encoded point payload bytes presented to the codec, including repeated
// reads, on BOTH donor and target. This is logical decode input, not device I/O
// or network bytes. An in-memory filesystem deliberately isolates receiver CPU.
type repairReadIO struct {
	store.ContinuationIO
	read *uint64
}

func (v repairReadIO) count(k, b []byte) {
	if len(k) == 20 && k[0] == 'f' && k[9] == 'p' {
		*v.read += uint64(len(b))
	}
}
func (v repairReadIO) Get(k []byte) ([]byte, error) {
	b, e := v.ContinuationIO.Get(k)
	if e == nil {
		v.count(k, b)
	}
	return b, e
}
func (v repairReadIO) Probe(k []byte) ([]byte, error) { return v.Get(k) }
func (v repairReadIO) Visit(p []byte, f func([]byte, []byte) error) error {
	return v.ContinuationIO.Visit(p, func(k, b []byte) error { v.count(k, b); return f(k, b) })
}
func (v repairReadIO) VisitRange(start, end []byte, f func([]byte, []byte) error) error {
	visit := func(k, b []byte) error { v.count(k, b); return f(k, b) }
	if r, ok := v.ContinuationIO.(store.ContinuationRangeReader); ok {
		return r.VisitRange(start, end, visit)
	}
	return v.ContinuationIO.Visit(nil, func(k, b []byte) error {
		if bytes.Compare(k, start) >= 0 && bytes.Compare(k, end) < 0 {
			return visit(k, b)
		}
		return nil
	})
}
func (v repairReadIO) ContinuationRevision() uint64 {
	if r, ok := v.ContinuationIO.(store.ContinuationRevision); ok {
		return r.ContinuationRevision()
	}
	return 0
}
func (v repairReadIO) TimeBuckets() bool {
	r, ok := v.ContinuationIO.(store.ContinuationTimeBuckets)
	return ok && r.TimeBuckets()
}

type repairBenchCodec struct {
	*whisperio.ContinuationCodec
	read uint64
}

func (c *repairBenchCodec) LeaseSnapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (store.Snapshot, func(), error) {
	return c.ContinuationCodec.LeaseSnapshot(ctx, repairReadIO{kv, &c.read}, m, clock)
}
func (c *repairBenchCodec) Snapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (store.Snapshot, error) {
	return c.ContinuationCodec.Snapshot(ctx, repairReadIO{kv, &c.read}, m, clock)
}
func (c *repairBenchCodec) MergeArchives(ctx context.Context, source, target, dst store.ContinuationIO, want, src, tgt store.Snapshot, clock func() time.Time) (string, bool, error) {
	return c.ContinuationCodec.MergeArchives(ctx, repairReadIO{source, &c.read}, repairReadIO{target, &c.read}, dst, want, src, tgt, clock)
}
func (c *repairBenchCodec) PlanSparseRepair(ctx context.Context, kv store.ContinuationIO, m store.Metadata, p store.SparseRepair, clock func() time.Time) (store.SparseRepairPlan, error) {
	return c.ContinuationCodec.PlanSparseRepair(ctx, repairReadIO{kv, &c.read}, m, p, clock)
}

func BenchmarkSparseRepair(b *testing.B) {
	for _, days := range []int{30, 14} {
		b.Run(fmt.Sprintf("%dd", days), func(b *testing.B) { benchmarkSparseRepair(b, days) })
	}
}

// Whisper disallows duplicate precision within one name: 14d and 30d are
// separate fine-archive populations, each with the same two coarse archives.
func benchmarkSparseRepair(b *testing.B, days int) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1795003200, 0) }
	cfg := store.MetricConfig{Name: "repair.benchmark", Retentions: []store.Retention{{Step: 60, Count: days * 24 * 60}, {Step: 1800, Count: 2 * 365 * 48}, {Step: 3600, Count: 10 * 365 * 24}}, AggregationMethod: store.Sum}
	source := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: make([]store.Archive, 3)}
	target := store.Snapshot{Metadata: source.Metadata, Archives: make([]store.Archive, 3)}
	until := clock().Unix() - 3*86400
	from := until - 3*86400
	for ai, r := range cfg.Retentions {
		source.Archives[ai].Retention, target.Archives[ai].Retention = r, r
		end := clock().Unix() / int64(r.Step) * int64(r.Step)
		for i := range r.Count {
			ts := end - int64((r.Count-1-i)*r.Step)
			value := math.Float64frombits(0x3ff0000000000000 + uint64(i)*1234567)
			source.Archives[ai].Points = append(source.Archives[ai].Points, store.Point{Timestamp: ts, Value: value})
			if ts < from || ts >= until {
				target.Archives[ai].Points = append(target.Archives[ai].Points, store.Point{Timestamp: ts, Value: -value})
			}
		}
	}
	record := func(snap store.Snapshot) store.TransferMetric {
		kv := largeContinuationMemory{}
		snap.Metadata.Revision = 1
		if err := (&whisperio.ContinuationCodec{}).RestoreArchives(ctx, kv, snap, clock); err != nil {
			b.Fatal(err)
		}
		r := store.TransferMetric{Config: cfg, Version: store.TransferVersion{Name: cfg.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
		for k, v := range kv {
			r.Keys = append(r.Keys, store.RelayoutKV{Key: []byte(k), Value: v})
		}
		slices.SortFunc(r.Keys, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
		return r
	}
	src, dst := record(source), record(target)
	missing := repairMissing(source, target, store.TransferVersion{})
	for _, mode := range []string{"ordinary", "sparse"} {
		b.Run(mode, func(b *testing.B) {
			codec := &repairBenchCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
			s, err := store.Open("/bench", (store.Options{ExactBlocks: true, Now: clock, CacheSize: 8 << 20, MemTableSize: 16 << 20, Continuation: codec}).WithFS(vfs.NewMem()))
			if err != nil {
				b.Fatal(err)
			}
			s.StopBackground()
			defer s.Close()
			b.ReportAllocs()
			b.ResetTimer()
			var read uint64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if i > 0 {
					if err := s.Delete(ctx, cfg.Name); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := s.ImportTransfer(ctx, []store.TransferMetric{dst}); err != nil {
					b.Fatal(err)
				}
				p := missing
				p.Expected = repairVersion(b, s, cfg.Name)
				codec.read = 0
				b.StartTimer()
				if mode == "ordinary" {
					_, err = s.ImportTransfer(ctx, []store.TransferMetric{src})
				} else {
					_, err = s.RepairSparse(ctx, p)
				}
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				read += codec.read
				// One untimed complete archive comparison proves the benchmark does repair.
				if i == 0 {
					got, err := s.Snapshot(ctx, cfg.Name)
					if err != nil {
						b.Fatal(err)
					}
					want := target
					want.Archives = make([]store.Archive, len(target.Archives))
					for ai, a := range source.Archives {
						want.Archives[ai].Retention = a.Retention
						for _, v := range a.Points {
							if v.Timestamp < from || v.Timestamp >= until {
								v.Value = -v.Value
							}
							want.Archives[ai].Points = append(want.Archives[ai].Points, v)
						}
					}
					assertRepairArchives(b, want, got)
				}
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(read)/float64(b.N), "codec-read-B/op")
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "names/s")
		})
	}
}
