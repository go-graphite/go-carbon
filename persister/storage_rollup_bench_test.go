package persister

import (
	"fmt"
	"os"
	"testing"

	whisper "github.com/go-graphite/go-whisper"
)

// Multi-archive write and pending-correction read workloads retain a classic
// oracle outside the timed loop, including after compaction and reopen.
func BenchmarkStorageRollup(b *testing.B) {
	storageBenchmarkLogs(b)
	for _, workload := range []string{"ordered-rollup", "late-holes-rollup", "coarse-clean", "coarse-corrected"} {
		for _, kind := range storageBackends {
			if kind == "cwhisper" && (workload == "late-holes-rollup" || workload == "coarse-corrected") {
				continue
			}
			b.Run(workload+"/"+kind, func(b *testing.B) { benchmarkStorageRollupCase(b, workload, kind) })
		}
	}
}

func benchmarkStorageRollupCase(b *testing.B, workload, kind string) {
	now := storageTestClock(b)
	s := newStorageBackend(b, kind, now)
	oracle := newStorageBackend(b, "classic", now)
	isRead := workload == "coarse-clean" || workload == "coarse-corrected"
	late := workload == "late-holes-rollup"
	c, seed := storageRollupConfig(isRead)
	for _, engine := range []*storageBackend{oracle, s} {
		storageMust(b, engine.create(c))
		storageMust(b, engine.update(c.Name, seed))
		storageMust(b, engine.compact([]string{c.Name}))
	}
	if workload == "coarse-corrected" {
		applyStorageRollupCorrection(b, kind, s, oracle, c)
	}
	from, until := storageRollupRange(isRead)
	want := storageRollupOracle(b, oracle, s, c.Name, from, until)
	batch := make([]whisper.TimeSeriesPoint, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if isRead {
			storageRollupRead(b, s, c.Name, from, until, len(want.values))
			continue
		}
		now.Store(int64(storageWriteBatch(batch, i, late)))
		storageMust(b, s.update(c.Name, batch))
	}
	b.StopTimer()
	if isRead {
		storageRollupMatches(b, want, s, c.Name, from, until)
		return
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*8), "ns/point")
	for i := 0; i < b.N; i++ {
		now.Store(int64(storageWriteBatch(batch, i, late)))
		storageMust(b, oracle.update(c.Name, batch))
	}
	storageCompare(b, oracle, s, c, "rollup-before-compaction")
	storageMust(b, s.compact([]string{c.Name}))
	storageMust(b, s.reopen())
	storageCompare(b, oracle, s, c, "rollup-compacted")
}

func storageRollupConfig(isRead bool) (storageMetricConfig, []whisper.TimeSeriesPoint) {
	if isRead {
		return storageConfig("metric", "1s:30m,60s:1d", whisper.Average, 0.5), storagePoints(storageEpoch-1200, 1200, 1)
	}
	return storageConfig("metric", "1s:10m,10s:1h,60s:6h", whisper.Average, 0.5), storagePoints(storageEpoch-600, 600, 1)
}

func applyStorageRollupCorrection(b *testing.B, kind string, s, oracle *storageBackend, c storageMetricConfig) {
	correction := storagePoints(storageEpoch-1000, 120, 1)
	for i := range correction {
		correction[i].Value += 1000
	}
	for _, engine := range []*storageBackend{oracle, s} {
		storageMust(b, engine.update(c.Name, correction))
	}
	if kind == "cwhisper-ooo" {
		if _, err := os.Stat(s.path(c.Name) + ".ooo"); err != nil {
			b.Fatal(err)
		}
	}
}

func storageRollupRange(isRead bool) (int, int) {
	if isRead {
		return storageEpoch - 21600, storageEpoch - 120
	}
	return storageEpoch - 600, storageEpoch
}

func storageRollupOracle(b *testing.B, oracle, candidate *storageBackend, name string, from, until int) *storageSeries {
	want, err := oracle.fetch(name, from, until)
	storageMust(b, err)
	storageRollupMatches(b, want, candidate, name, from, until)
	return want
}

func storageRollupMatches(b *testing.B, want *storageSeries, candidate *storageBackend, name string, from, until int) {
	got, err := candidate.fetch(name, from, until)
	storageMust(b, err)
	if diff := storageSeriesDiff(want, got); diff != "" {
		b.Fatal(diff)
	}
}

func storageRollupRead(b *testing.B, s *storageBackend, name string, from, until, wantLen int) {
	got, err := s.fetch(name, from, until)
	storageMust(b, err)
	if got == nil || len(got.values) != wantLen {
		b.Fatal("read grid changed")
	}
}

func TestStorageLateRollupRetentionWrap(t *testing.T) {
	checkStorageLateRollupRetentionWrap(t, "cwhisper-ooo", whisper.Average, 0.5)
}

func TestStorageChunkLateRollupRetentionWrap(t *testing.T) {
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.Last, whisper.Max, whisper.Min, whisper.First} {
		for _, xff := range []float32{0, 0.5, 1} {
			t.Run(fmt.Sprintf("%s/xff=%g", method, xff), func(t *testing.T) { checkStorageLateRollupRetentionWrap(t, "pebble-chunk", method, xff) })
		}
	}
}

func checkStorageLateRollupRetentionWrap(t *testing.T, kind string, method whisper.AggregationMethod, xff float32) {
	now := storageTestClock(t)
	oracle := newStorageBackend(t, "classic", now)
	s := newStorageBackend(t, kind, now)
	c := storageConfig("metric", "1s:10m,10s:1h,60s:6h", method, xff)
	for _, engine := range []*storageBackend{oracle, s} {
		storageMust(t, engine.create(c))
		storageMust(t, engine.update(c.Name, storagePoints(storageEpoch-600, 600, 1)))
		storageMust(t, engine.compact([]string{c.Name}))
	}
	batch := make([]whisper.TimeSeriesPoint, 8)
	for i := 0; i < 4096; i++ {
		now.Store(int64(storageWriteBatch(batch, i, true)))
		storageMust(t, oracle.update(c.Name, batch))
		storageMust(t, s.update(c.Name, batch))
		if i < 128 || i%32 == 0 || i == 4095 {
			storageCompare(t, oracle, s, c, fmt.Sprintf("round=%d before-compaction", i))
		}
	}
	storageMust(t, s.compact([]string{c.Name}))
	storageMust(t, s.reopen())
	storageCompare(t, oracle, s, c, "final-compaction")
}
