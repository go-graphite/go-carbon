package persister

import (
	"fmt"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	whisper "github.com/go-graphite/go-whisper"
)

func storageBenchmarkLogs(b *testing.B) {
	b.Helper()
	// Pebble's default logger uses the standard logger. Replay messages printed
	// between the benchmark name and its results break benchstat's line format.
	file, err := os.Create(filepath.Join(b.TempDir(), "engine.log"))
	storageMust(b, err)
	previous := log.Writer()
	log.SetOutput(file)
	b.Cleanup(func() {
		log.SetOutput(previous)
		storageMust(b, file.Close())
		if b.Failed() {
			data, err := os.ReadFile(file.Name())
			storageMust(b, err)
			b.Logf("engine log:\n%s", data)
		}
	})
}

func storageBenchmarkNames(count int) []string {
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("metric-%04d", i)
	}
	return names
}

func storageReportFootprint(b *testing.B, s *storageBackend, metrics int) {
	b.Helper()
	// Quiesce background deletion of obsolete Pebble manifests/SSTables before
	// walking the directory; otherwise the sampled files may vanish mid-walk.
	storageMust(b, s.close())
	var size, files int64
	storageMust(b, filepath.WalkDir(s.dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		size += info.Size()
		files++
		return nil
	}))
	// These are apparent file lengths, including sparse holes, WAL and metadata;
	// they are neither allocated disk blocks nor process RSS.
	b.ReportMetric(float64(size)/float64(metrics), "logical-B/metric")
	b.ReportMetric(float64(files), "files")
}

func storageWriteBatch(batch []whisper.TimeSeriesPoint, round int, late bool) int {
	end := storageEpoch + (round+1)*len(batch)
	if late {
		end = storageEpoch + (round/2+1)*2*len(batch)
	}
	start := end - len(batch)
	if late && round%2 == 1 {
		start -= len(batch) // Fill the hole left by the previous committed batch.
	}
	for j := range batch {
		timestamp := start + j
		batch[j] = whisper.TimeSeriesPoint{Time: timestamp, Value: float64(timestamp % 101)}
	}
	return end
}

func storageCheckWrittenWindow(b *testing.B, s *storageBackend, names []string, rounds []int, batchSize int, late bool, seed []whisper.TimeSeriesPoint) {
	b.Helper()
	oracle := newStorageBackend(b, "classic", s.now)
	expected := make(map[int]*storageSeries)
	from, until := int(s.now.Load())-600, int(s.now.Load())
	for i, name := range names {
		round := rounds[i]
		want, exists := expected[round]
		if !exists {
			c := storageConfig(fmt.Sprintf("round-%d", round), "1s:10m", whisper.Average, 0.5)
			storageMust(b, oracle.create(c))
			// These single-archive workloads have distinct timestamps. Combining
			// the bounded live suffix into one oracle batch preserves their write
			// semantics, including holes from an unfinished late-write pair.
			input := append([]whisper.TimeSeriesPoint(nil), seed...)
			batch := make([]whisper.TimeSeriesPoint, batchSize)
			for replay := max(0, round-600/batchSize-2); replay <= round; replay++ {
				storageWriteBatch(batch, replay, late)
				input = append(input, batch...)
			}
			storageMust(b, oracle.update(c.Name, input))
			var err error
			want, err = oracle.fetch(c.Name, from, until)
			storageMust(b, err)
			expected[round] = want
		}
		got, err := s.fetch(name, from, until)
		storageMust(b, err)
		if diff := storageSeriesDiff(want, got); diff != "" {
			b.Fatalf("post-benchmark %s round=%d: %s", name, round, diff)
		}
	}
}

func storageCheckWriteBenchmark(b *testing.B, kind string, batchSize int, late bool) {
	b.Helper()
	now := storageTestClock(b)
	oracle := newStorageBackend(b, "classic", now)
	candidate := newStorageBackend(b, kind, now)
	c := storageConfig("preflight", "1s:10m", whisper.Average, 0.5)
	storageMust(b, oracle.create(c))
	storageMust(b, candidate.create(c))
	batch := make([]whisper.TimeSeriesPoint, batchSize)
	// At least two wraps and several late batches; a faster drop path must never
	// be mistaken for a successful write benchmark.
	for round := 0; round < 1200/batchSize+4; round++ {
		now.Store(int64(storageWriteBatch(batch, round, late)))
		storageMust(b, oracle.update(c.Name, batch))
		storageMust(b, candidate.update(c.Name, batch))
	}
	storageCompare(b, oracle, candidate, c, "benchmark-preflight")
	storageMust(b, candidate.compact([]string{c.Name}))
	storageMust(b, candidate.reopen())
	storageCompare(b, oracle, candidate, c, "benchmark-preflight-compacted")
}

// One op is one batch for one metric. File opens, lock acquisition and closes
// are timed, as are Pebble WAL syncs. Creation and final maintenance are not.
func BenchmarkStorageWrite(b *testing.B) {
	storageBenchmarkLogs(b)
	for _, late := range []bool{false, true} {
		workload := "ordered"
		if late {
			workload = "late-holes"
		}
		for _, batchSize := range []int{1, 8, 64} {
			for _, metrics := range []int{1, 128} {
				for _, kind := range storageBackends {
					b.Run(fmt.Sprintf("%s/batch=%d/metrics=%d/%s", workload, batchSize, metrics, kind), func(b *testing.B) {
						if late && kind == "cwhisper" {
							b.Skip("plain cwhisper drops late points; its throughput would count lost data")
						}
						storageCheckWriteBenchmark(b, kind, batchSize, late)
						now := storageTestClock(b)
						s := newStorageBackend(b, kind, now)
						names := storageBenchmarkNames(metrics)
						seed := storagePoints(storageEpoch-600, 600, 1)
						for _, name := range names {
							storageMust(b, s.create(storageConfig(name, "1s:10m", whisper.Average, 0.5)))
							storageMust(b, s.update(name, seed))
						}
						storageMust(b, s.compact(names))
						batch := make([]whisper.TimeSeriesPoint, batchSize)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							now.Store(int64(storageWriteBatch(batch, i/metrics, late)))
							storageMust(b, s.update(names[i%metrics], batch))
						}
						b.StopTimer()
						points := float64(b.N) * float64(batchSize)
						b.ReportMetric(float64(b.Elapsed().Nanoseconds())/points, "ns/point")
						b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
						b.ReportMetric(float64(batchSize), "points/op")
						storageMust(b, s.compact(names))
						storageMust(b, s.reopen())
						rounds := make([]int, metrics)
						for metric := range rounds {
							rounds[metric] = -1
							if metric < b.N {
								rounds[metric] = (b.N - 1 - metric) / metrics
							}
						}
						storageCheckWrittenWindow(b, s, names, rounds, batchSize, late, seed)
						storageReportFootprint(b, s, metrics)
					})
				}
			}
		}
	}
}

func BenchmarkStorageRead(b *testing.B) {
	storageBenchmarkLogs(b)
	for _, query := range []struct {
		name        string
		from, until int
	}{
		{"recent", storageEpoch - 121, storageEpoch - 1},
		{"fine-full", storageEpoch - 1800, storageEpoch},
		{"coarse", storageEpoch - 21600, storageEpoch - 120},
	} {
		for _, kind := range storageBackends {
			b.Run(query.name+"/"+kind, func(b *testing.B) {
				now := storageTestClock(b)
				s := newStorageBackend(b, kind, now)
				oracle := newStorageBackend(b, "classic", now)
				c := storageConfig("metric", "1s:30m,60s:1d", whisper.Average, 0.5)
				input := storagePoints(storageEpoch-1200, 1200, 1)
				for _, engine := range []*storageBackend{oracle, s} {
					storageMust(b, engine.create(c))
					storageMust(b, engine.update(c.Name, input))
					storageMust(b, engine.compact([]string{c.Name}))
				}
				want, err := oracle.fetch(c.Name, query.from, query.until)
				storageMust(b, err)
				got, err := s.fetch(c.Name, query.from, query.until)
				storageMust(b, err)
				if diff := storageSeriesDiff(want, got); diff != "" {
					b.Fatalf("read benchmark preflight: %s", diff)
				}
				if got == nil || len(got.values) == 0 {
					b.Fatal("read workload is empty")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := s.fetch(c.Name, query.from, query.until)
					storageMust(b, err)
					if result == nil || len(result.values) != len(want.values) {
						b.Fatal("read grid changed")
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(got.values)), "values/op")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(got.values)), "ns/value")
				storageReportFootprint(b, s, 1)
			})
		}
	}
}

// One op is a round of concurrent eight-point writes to distinct metrics. A
// barrier keeps the simulated clock fixed until every writer finishes: a fast
// worker must not age out a slow worker's data. Scheduling is included equally
// for all engines, and Pebble can group simultaneous WAL syncs.
func BenchmarkStorageConcurrentWrite(b *testing.B) {
	storageBenchmarkLogs(b)
	for _, workers := range []int{1, 8, 32} {
		for _, kind := range storageBackends {
			b.Run(fmt.Sprintf("workers=%d/%s", workers, kind), func(b *testing.B) {
				now := storageTestClock(b)
				s := newStorageBackend(b, kind, now)
				names := storageBenchmarkNames(workers)
				seed := storagePoints(storageEpoch-600, 600, 1)
				for _, name := range names {
					storageMust(b, s.create(storageConfig(name, "1s:10m", whisper.Average, 0.5)))
					storageMust(b, s.update(name, seed))
				}
				storageMust(b, s.compact(names))
				jobs := make([]chan int, workers)
				done := make(chan error, workers)
				var wg sync.WaitGroup
				for i, name := range names {
					jobs[i] = make(chan int, 1)
					wg.Go(func() {
						batch := make([]whisper.TimeSeriesPoint, 8)
						for round := range jobs[i] {
							storageWriteBatch(batch, round, false)
							done <- s.update(name, batch)
						}
					})
				}
				stop := func() {
					for _, job := range jobs {
						close(job)
					}
					wg.Wait()
				}
				stop = sync.OnceFunc(stop)
				b.Cleanup(stop)
				b.ReportAllocs()
				b.ResetTimer()
				for round := 0; round < b.N; round++ {
					now.Store(int64(storageEpoch + (round+1)*8))
					for _, job := range jobs {
						job <- round
					}
					for range workers {
						storageMust(b, <-done)
					}
				}
				b.StopTimer()
				stop()
				points := float64(b.N) * float64(workers*8)
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/points, "ns/point")
				b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
				b.ReportMetric(float64(workers*8), "points/op")
				storageMust(b, s.compact(names))
				storageMust(b, s.reopen())
				rounds := make([]int, workers)
				for i := range rounds {
					rounds[i] = b.N - 1
				}
				storageCheckWrittenWindow(b, s, names, rounds, 8, false, seed)
				storageReportFootprint(b, s, workers)
			})
		}
	}
}

// One op is maintenance on the same bounded corpus, reseeded outside the timer.
// Report maintenance separately: it must be included when evaluating a workload
// with a particular compaction cadence, not hidden inside write throughput.
func BenchmarkStorageMaintenance(b *testing.B) {
	storageBenchmarkLogs(b)
	for _, kind := range []string{"cwhisper-ooo"} {
		b.Run(kind, func(b *testing.B) {
			now := storageTestClock(b)
			s := newStorageBackend(b, kind, now)
			c := storageConfig("metric", "1s:1h", whisper.Average, 0.5)
			storageMust(b, s.create(c))
			names := []string{c.Name}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				end := storageEpoch + (i+1)*1024
				now.Store(int64(end))
				storageMust(b, s.update(c.Name, storagePoints(end-512, 512, 1)))
				storageMust(b, s.update(c.Name, storagePoints(end-1024, 512, 1)))
				before, err := s.fetch(c.Name, end-1024, end)
				storageMust(b, err)
				if before == nil || len(before.values) != 1024 || math.IsNaN(before.values[0]) {
					b.Fatal("maintenance workload did not persist late points")
				}
				b.StartTimer()
				storageMust(b, s.compact(names))
				b.StopTimer()
				after, err := s.fetch(c.Name, end-1024, end)
				storageMust(b, err)
				if diff := storageSeriesDiff(before, after); diff != "" {
					b.Fatalf("maintenance changed data: %s", diff)
				}
			}
			storageReportFootprint(b, s, 1)
		})
	}
}

func BenchmarkStorageReopen(b *testing.B) {
	storageBenchmarkLogs(b)
	for _, kind := range storageBackends {
		b.Run(kind, func(b *testing.B) {
			now := storageTestClock(b)
			s := newStorageBackend(b, kind, now)
			c := storageConfig("metric", "1s:10m", whisper.Average, 0.5)
			storageMust(b, s.create(c))
			storageMust(b, s.update(c.Name, storagePoints(storageEpoch-100, 100, 1)))
			want, err := s.fetch(c.Name, storageEpoch-100, storageEpoch)
			storageMust(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				storageMust(b, s.reopen())
				got, err := s.fetch(c.Name, storageEpoch-100, storageEpoch)
				storageMust(b, err)
				if diff := storageSeriesDiff(want, got); diff != "" {
					b.Fatal(diff)
				}
			}
		})
	}
}
