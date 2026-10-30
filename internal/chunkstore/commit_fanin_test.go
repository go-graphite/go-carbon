package chunkstore

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// Isolate submission/apply cost with six writers per CPU (96 on -cpu 16).
// Each metric changes a header/group and a revision; grouping keeps the same
// mutations and WAL policy while reducing commit-pipeline entries.
func BenchmarkCommitFanIn(b *testing.B) {
	for _, size := range []int{1, 4, 8} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			db, err := Open(b.TempDir(), Options{SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			var worker atomic.Int64
			b.SetParallelism(6)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := worker.Add(1)
				keys := make([][]byte, size*2)
				for i := range keys {
					keys[i] = []byte(fmt.Sprintf("fanin/%03d/%02d", id, i))
				}
				var header [512]byte
				var revision [8]byte
				for pb.Next() {
					batch := db.db.NewIndexedBatch()
					for i := range size {
						if err := batch.Set(keys[i*2], header[:], nil); err != nil {
							b.Fatal(err)
						}
						if err := batch.Set(keys[i*2+1], revision[:], nil); err != nil {
							b.Fatal(err)
						}
					}
					if err := batch.Commit(pebble.NoSync); err != nil {
						b.Fatal(err)
					}
					batch.Close()
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/update")
		})
	}
}
