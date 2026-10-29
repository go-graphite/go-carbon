package chunkstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Opt-in geometry experiment. Incompressible values and a memory filesystem
// isolate table write amplification; they do not model production read p99 or
// physical disk IOPS. Each trial flushes the same hot/cold key sequence.
func BenchmarkCompactionGeometry(b *testing.B) {
	if os.Getenv("GO_CARBON_BENCH_COMPACTION_GEOMETRY") != "1" {
		b.Skip("set GO_CARBON_BENCH_COMPACTION_GEOMETRY=1")
	}
	const metrics, hot, rounds = 8192, 1024, 12
	keys := make([][]byte, metrics)
	values := make([][]byte, metrics)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "geometry.%08d", i)
		values[i] = make([]byte, 1024)
		for offset := 8; offset < 1024; offset += 8 {
			binary.LittleEndian.PutUint64(values[i][offset:], rng.Uint64())
		}
	}
	cpu := func() float64 {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			b.Fatal(err)
		}
		return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	}
	for _, base := range []int64{1 << 20, 8 << 20} {
		for _, target := range []int64{128 << 10, 1 << 20} {
			b.Run(fmt.Sprintf("base=%d/target=%d", base, target), func(b *testing.B) {
				var writes, ops, compacted uint64
				var seconds, reads float64
				b.ReportAllocs()
				for range b.N {
					db, err := Open(b.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 4 << 20, MemTableSize: 1 << 20,
						LBaseMaxBytes: base, TargetFileSize: target, L0CompactionThreshold: 2, CompactionWorkers: 1})
					if err != nil {
						b.Fatal(err)
					}
					before := cpu()
					for round := 0; round <= rounds; round++ {
						batch := db.db.NewBatch()
						count := hot
						if round == 0 {
							count = metrics
						}
						for i := range count {
							binary.LittleEndian.PutUint64(values[i], uint64(round))
							if err := batch.Set(keys[i], values[i], nil); err != nil {
								b.Fatal(err)
							}
						}
						if err := batch.Commit(pebble.NoSync); err != nil {
							b.Fatal(err)
						}
						batch.Close()
						if err := db.Flush(); err != nil {
							b.Fatal(err)
						}
					}
					if err := db.Compact(); err != nil {
						b.Fatal(err)
					}
					seconds += cpu() - before
					stats := db.Stats()
					writes += stats.FS.Bytes[FSWrite][FSTable]
					ops += stats.FS.Ops[FSWrite][FSTable]
					compacted += stats.CompactionWriteBytes
					start := time.Now()
					for i := range 1024 {
						index := i * 7919 % metrics
						value, closer, err := db.db.Get(keys[index])
						if err != nil {
							b.Fatal(err)
						}
						want := uint64(0)
						if index < hot {
							want = rounds
						}
						if binary.LittleEndian.Uint64(value) != want || !bytes.Equal(value[8:], values[index][8:]) {
							b.Fatal("read differs after compaction")
						}
						closer.Close()
					}
					reads += float64(time.Since(start).Nanoseconds()) / 1024
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(seconds/float64(b.N), "cpu-s/trial")
				b.ReportMetric(float64(writes)/float64(b.N), "table-B/trial")
				b.ReportMetric(float64(compacted)/float64(b.N), "compact-B/trial")
				b.ReportMetric(float64(ops)/float64(b.N), "table-writes/trial")
				b.ReportMetric(reads/float64(b.N), "ns/read")
			})
		}
	}
}
