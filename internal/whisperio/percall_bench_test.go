package whisperio

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

var percallSchemas = []struct {
	name       string
	retentions []store.Retention
}{
	{"secondly", []store.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}},
	{"ten_secondly", []store.Retention{{Step: 10, Count: 17280}, {Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}},
	{"minutely", []store.Retention{{Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}},
	{"five_minutely", []store.Retention{{Step: 300, Count: 105120}}},
	{"default", []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}},
}

var representativeSchemas = []struct {
	name       string
	retentions []store.Retention
	points     int
}{
	{"D1_queue", []store.Retention{{Step: 60, Count: 20160}, {Step: 300, Count: 17280}, {Step: 3600, Count: 17520}}, 210},
	{"D1_queue_single_archive", []store.Retention{{Step: 60, Count: 20160}}, 210},
	{"D1_queue_single_archive_raw10s", []store.Retention{{Step: 60, Count: 20160}}, 210},
	{"D1_queue_raw1s", []store.Retention{{Step: 60, Count: 20160}, {Step: 300, Count: 17280}, {Step: 3600, Count: 17520}}, 210},
	{"D1_queue_raw10s", []store.Retention{{Step: 60, Count: 20160}, {Step: 300, Count: 17280}, {Step: 3600, Count: 17520}}, 210},
	{"D1_hourly", []store.Retention{{Step: 3600, Count: 2160}, {Step: 86400, Count: 730}}, 210},
	{"D2_default", []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, 32},
	{"D2_secondly", []store.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}, 32},
	{"D3_secondly", []store.Retention{{Step: 1, Count: 172800}}, 210},
	{"D3_minutely", []store.Retention{{Step: 60, Count: 50400}}, 210},
	{"D3_hourly", []store.Retention{{Step: 3600, Count: 17520}}, 210},
	{"D3_daily", []store.Retention{{Step: 86400, Count: 3650}}, 210},
}

// Override only the tail-slot policy, keeping the same codec for paired A/B.
type noTailSlotCodec struct{ *ContinuationCodec }

func (noTailSlotCodec) StateSlot([]byte) []byte { return nil }

func BenchmarkPerCallRepresentative(b *testing.B) {
	for _, schema := range representativeSchemas {
		for _, mode := range []string{"baseline", "buffer", "tails"} {
			b.Run(schema.name+"/"+mode, func(b *testing.B) {
				previous := coalescePointWrites
				coalescePointWrites = mode != "baseline"
				defer func() { coalescePointWrites = previous }()
				const metrics = 32
				ctx := context.Background()
				now := time.Unix(1790000000, 0)
				codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
				var continuation store.ContinuationCodec = codec
				if mode != "tails" {
					continuation = noTailSlotCodec{codec}
				}
				db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, Continuation: continuation, TimeBuckets: true, Now: func() time.Time { return now }, SyncInterval: time.Hour, CacheSize: 8 << 20, MemTableSize: 4 << 20, MetadataCacheSize: 128 << 20})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				names := make([]string, metrics)
				for i := range names {
					names[i] = fmt.Sprintf("production.percall.%d", i)
					if _, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: schema.retentions, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
						b.Fatal(err)
					}
				}
				input := make([]store.Point, schema.points)
				step := int64(schema.retentions[0].Step)
				if schema.name == "D1_queue_raw1s" {
					step = 1
				}
				if schema.name == "D1_queue_raw10s" || schema.name == "D1_queue_single_archive_raw10s" {
					step = 10
				}
				base := now.Unix() / step * step
				update := func(i int) {
					stamp := base + int64(i/metrics+1)*int64(len(input))*step
					now = time.Unix(stamp+step, 0)
					for j := range input {
						input[j] = store.Point{Timestamp: stamp - int64(len(input)-j)*step, Value: float64((i+j)%97) + .125}
					}
					ctx, timer := stagetime.Start(ctx)
					if err := db.UpdateMany(ctx, names[i%metrics], input); err != nil {
						b.Fatal(err)
					}
					timer.Finish()
				}
				for i := range metrics * 4 {
					update(i)
				}
				if err := db.Flush(); err != nil {
					b.Fatal(err)
				}
				reads := store.CountStoreReadsForTest()
				stages := stagetime.Snapshot()
				var before, after syscall.Rusage
				syscall.Getrusage(syscall.RUSAGE_SELF, &before)
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					update(i + metrics*4)
				}
				b.StopTimer()
				syscall.Getrusage(syscall.RUSAGE_SELF, &after)
				end := stagetime.Snapshot()
				for _, stage := range []stagetime.Stage{stagetime.Route, stagetime.Encode, stagetime.TailRead, stagetime.Commit} {
					b.ReportMetric((end[stage].Seconds-stages[stage].Seconds)*1e6/float64(b.N), stage.String()+"-us/update")
				}
				cpu := func(u syscall.Rusage) float64 {
					return float64(u.Utime.Sec+u.Stime.Sec)*1e6 + float64(u.Utime.Usec+u.Stime.Usec)
				}
				b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-us/update")
				b.ReportMetric(float64(reads())/float64(b.N), "reads/update")
				stats := db.Stats().StateCache
				b.ReportMetric(float64(stats.Bytes)/float64(stats.Entries), "cache-B/metric")
			})
		}
	}
}

func BenchmarkPerCallSchemas(b *testing.B) {
	for _, schema := range percallSchemas {
		for _, count := range []int{32, 210} {
			b.Run(fmt.Sprintf("%s/points=%d", schema.name, count), func(b *testing.B) {
				const metrics = 32
				ctx := context.Background()
				now := time.Unix(1790000000, 0)
				db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, TimeBuckets: true, Now: func() time.Time { return now }, SyncInterval: time.Hour, CacheSize: 8 << 20, MemTableSize: 4 << 20, MetadataCacheSize: 128 << 20})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				names := make([]string, metrics)
				for i := range names {
					names[i] = fmt.Sprintf("bench.percall.%d", i)
					_, err = db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: schema.retentions, AggregationMethod: store.Average, XFilesFactor: .5})
					if err != nil {
						b.Fatal(err)
					}
				}
				input := make([]store.Point, count)
				step := int64(schema.retentions[0].Step)
				base := now.Unix() / step * step
				update := func(i int) {
					stamp := base + int64(i/metrics+1)*int64(count)*step
					now = time.Unix(stamp+step, 0)
					for j := range input {
						input[j] = store.Point{Timestamp: stamp - int64(count-j)*step, Value: float64((i+j)%97) + .125}
					}
					timed, timer := stagetime.Start(ctx)
					if err := db.UpdateMany(timed, names[i%metrics], input); err != nil {
						b.Fatal(err)
					}
					timer.Finish()
				}
				// Populate the active tails and coarse buffers before measuring.
				for i := range metrics * 4 {
					update(i)
				}
				if err := db.Flush(); err != nil {
					b.Fatal(err)
				}
				reads := store.CountStoreReadsForTest()
				stages := stagetime.Snapshot()
				before := db.Stats().StateCache
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					update(i + metrics*4)
				}
				b.StopTimer()
				after := stagetime.Snapshot()
				for _, stage := range []stagetime.Stage{stagetime.Route, stagetime.Encode, stagetime.TailRead, stagetime.Commit} {
					b.ReportMetric((after[stage].Seconds-stages[stage].Seconds)*1e6/float64(b.N), stage.String()+"-us/update")
				}
				b.ReportMetric(float64(reads())/float64(b.N), "reads/update")
				stats := db.Stats().StateCache
				b.ReportMetric(float64(stats.Misses-before.Misses)/float64(b.N), "misses/update")
				b.ReportMetric(float64(stats.Bytes)/float64(stats.Entries), "cache-B/metric")
			})
		}
	}
}
