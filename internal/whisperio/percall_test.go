package whisperio

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestPerCallBatchMatchesSerial(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1790000000, 0)
	root := t.TempDir()
	serial := stateCacheStore(t, filepath.Join(root, "serial"), true, 128<<20, func() time.Time { return now })
	grouped := stateCacheStore(t, filepath.Join(root, "grouped"), true, 128<<20, func() time.Time { return now })
	defer serial.Close()
	defer func() { grouped.Close() }()
	names := []string{"batch.a", "batch.b", "batch.c"}
	for _, db := range []*store.Store{serial, grouped} {
		for _, name := range names {
			if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: percallSchemas[1].retentions, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for round := range 20 {
		now = now.Add(2100 * time.Second)
		var updates []store.MetricUpdate
		for i := range 5 {
			input := make([]store.Point, 210)
			for j := range input {
				input[j] = store.Point{Timestamp: now.Unix() - int64(210-j)*10, Value: float64(round+i+j) + .125}
			}
			updates = append(updates, store.MetricUpdate{Name: names[i%len(names)], Points: input})
		}
		for _, update := range updates {
			if err := serial.UpdateMany(ctx, update.Name, update.Points); err != nil {
				t.Fatal(err)
			}
		}
		if err := grouped.UpdateBatch(ctx, updates, nil); err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			want, err := serial.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := grouped.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			// LastUpdate uses the local clock and monotonic sequence independently.
			got.Metadata.LastUpdate = want.Metadata.LastUpdate
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("round %d %s snapshots differ", round, name)
			}
		}
	}
	if err := grouped.Close(); err != nil {
		t.Fatal(err)
	}
	grouped = stateCacheStore(t, filepath.Join(root, "grouped"), true, 128<<20, func() time.Time { return now })
	for _, name := range names {
		want, _ := serial.Snapshot(ctx, name)
		got, err := grouped.Snapshot(ctx, name)
		got.Metadata.LastUpdate = want.Metadata.LastUpdate
		if err != nil || !reflect.DeepEqual(want, got) {
			t.Fatalf("reopen %s: %v", name, err)
		}
	}
}

func BenchmarkPerCallGroup(b *testing.B) {
	for _, count := range []int{32, 210} {
		for _, size := range []int{1, 8} {
			b.Run(fmt.Sprintf("points=%d/group=%d", count, size), func(b *testing.B) {
				ctx := context.Background()
				now := time.Unix(1790000000, 0)
				db := stateCacheStore(b, b.TempDir(), true, 128<<20, func() time.Time { return now })
				defer db.Close()
				updates := make([]store.MetricUpdate, 8)
				for i := range updates {
					name := fmt.Sprintf("group.%d", i)
					if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: percallSchemas[0].retentions, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
						b.Fatal(err)
					}
					updates[i] = store.MetricUpdate{Name: name, Points: make([]store.Point, count)}
				}
				base := now.Unix()
				apply := func(i, n int) {
					stamp := base + int64(i/8+1)*int64(count)
					now = time.Unix(stamp+1, 0)
					group := updates[i%8 : i%8+n]
					for index, update := range group {
						for j := range update.Points {
							update.Points[j] = store.Point{Timestamp: stamp - int64(count-j), Value: float64((i/8+i%8+index+j)%97) + .125}
						}
					}
					if size == 1 {
						if err := db.UpdateMany(ctx, group[0].Name, group[0].Points); err != nil {
							b.Fatal(err)
						}
					} else if err := db.UpdateBatch(ctx, group, nil); err != nil {
						b.Fatal(err)
					}
				}
				for i := 0; i < 128; i += size {
					apply(i, size)
				}
				var before, after syscall.Rusage
				syscall.Getrusage(syscall.RUSAGE_SELF, &before)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i += size {
					apply(i+128, min(size, b.N-i))
				}
				b.StopTimer()
				syscall.Getrusage(syscall.RUSAGE_SELF, &after)
				cpu := func(u syscall.Rusage) float64 {
					return float64(u.Utime.Sec+u.Stime.Sec)*1e6 + float64(u.Utime.Usec+u.Stime.Usec)
				}
				b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-us/update")
			})
		}
	}
}
