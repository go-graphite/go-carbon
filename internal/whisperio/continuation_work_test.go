package whisperio

import (
	"context"
	"encoding/binary"
	"fmt"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type propagationWorkIO struct {
	*appendTailKV
	writes [3]int
}

func (w *propagationWorkIO) Set(key, value []byte) error {
	if len(key) == 20 && key[0] == 'f' && key[9] == 'c' {
		archive := int(binary.BigEndian.Uint16(key[10:12]))
		if archive < len(w.writes) {
			w.writes[archive]++
		}
	}
	return w.appendTailKV.Set(key, value)
}

func TestPerCallPropagationWork(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{ReuseBlockKeys: true}
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "propagation", Retentions: productionSchemas[0].retentions, AggregationMethod: store.Average, XFilesFactor: .5}}
	for _, spacing := range []int64{1, 10, 60} {
		for _, buffered := range []bool{false, true} {
			now := time.Unix(1790000000, 0)
			clock := func() time.Time { return now }
			kv := &propagationWorkIO{appendTailKV: &appendTailKV{data: make(map[string][]byte)}}
			if err := codec.Create(ctx, kv, m, clock); err != nil {
				t.Fatal(err)
			}
			original := coalescePointWrites
			coalescePointWrites = buffered
			func() {
				defer func() { coalescePointWrites = original }()
				input := make([]store.Point, 210)
				for round := range 12 {
					now = now.Add(time.Duration(210*spacing) * time.Second)
					for i := range input {
						input[i] = store.Point{Timestamp: now.Unix() - int64(210-i)*spacing, Value: float64(i%17) + .5}
					}
					if round == 4 {
						kv.writes = [3]int{}
					}
					if err := codec.Update(ctx, kv, m, input, -1, clock, nil); err != nil {
						t.Fatal(err)
					}
				}
			}()
			t.Logf("spacing=%ds buffer=%v group writes/update primary=%.2f 5m=%.2f 60m=%.2f", spacing, buffered, float64(kv.writes[0])/8, float64(kv.writes[1])/8, float64(kv.writes[2])/8)
		}
	}
}

func BenchmarkPerCallColdTail(b *testing.B) {
	for _, tails := range []bool{false, true} {
		label := "database"
		if tails {
			label = "cached"
		}
		b.Run(label, func(b *testing.B) {
			ctx := context.Background()
			now := time.Unix(1790000000, 0)
			codec := &ContinuationCodec{ReuseBlockKeys: true}
			var continuation store.ContinuationCodec = codec
			if !tails {
				continuation = noTailSlotCodec{codec}
			}
			db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, Continuation: continuation, TimeBuckets: true, Now: func() time.Time { return now }, SyncInterval: time.Hour, MetadataCacheSize: 128 << 20, CacheSize: 64 << 10, CacheShards: 1, MemTableSize: 1 << 20, BloomFilterBits: 10})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			const metrics = 128
			names := make([]string, metrics)
			for i := range names {
				names[i] = fmt.Sprintf("cold.tail.%d", i)
				if _, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
					b.Fatal(err)
				}
			}
			input := make([]store.Point, 32)
			base := now.Unix()
			run := func(round int) {
				now = time.Unix(base+int64(round+1)*32*60, 0)
				for i := range input {
					input[i] = store.Point{Timestamp: now.Unix() - int64(32-i)*60, Value: float64(round+i) + .25}
				}
				for _, name := range names {
					if err := db.UpdateMany(ctx, name, input); err != nil {
						b.Fatal(err)
					}
				}
			}
			for round := range 4 {
				run(round)
			}
			if err := db.Compact(); err != nil {
				b.Fatal(err)
			}
			before := db.Stats()
			var usageBefore, usageAfter syscall.Rusage
			syscall.Getrusage(syscall.RUSAGE_SELF, &usageBefore)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				run(i + 4)
				// Force the next round's tails out of the memtable, as at high cardinality.
				if err := db.Flush(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			syscall.Getrusage(syscall.RUSAGE_SELF, &usageAfter)
			after := db.Stats()
			n := float64(b.N * metrics)
			cpu := func(u syscall.Rusage) float64 {
				return float64(u.Utime.Sec+u.Stime.Sec)*1e6 + float64(u.Utime.Usec+u.Stime.Usec)
			}
			b.ReportMetric((cpu(usageAfter)-cpu(usageBefore))/n, "cpu-us/update")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/n, "ns/update")
			b.ReportMetric(float64(after.CacheHits+after.CacheMisses-before.CacheHits-before.CacheMisses)/n, "blocks/update")
			b.ReportMetric(float64(after.CacheMisses-before.CacheMisses)/n, "misses/update")
		})
	}
}
