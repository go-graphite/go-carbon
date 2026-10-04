package persister

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// TestStorageScale is opt-in: each invocation runs one engine so process CPU and
// lifetime RSS are attributable. Correctness is checked outside the timed loop.
func TestStorageScale(t *testing.T) {
	kind, count, rounds, workers, late := storageScaleSettings(t)
	now := storageTestClock(t)
	s := newStorageBackend(t, kind, now)
	names := storageBenchmarkNames(count)
	config := storageConfig("metric", "1s:10m,10s:1h,60s:6h", whisper.Average, 0.5)
	seed := storagePoints(storageEpoch-120, 120, 1)
	setupStorageScale(t, s, names, config, seed, workers, rounds, late)
	oracle := newStorageBackend(t, "classic", now)
	storageMust(t, oracle.create(config))
	storageMust(t, oracle.update(config.Name, seed))
	elapsed, before, after, memBefore, memAfter, batchLatencies, readLatencies := runStorageScale(t, s, oracle, names, config, rounds, workers, late, now)
	logStorageScale(t, kind, count, rounds, elapsed, before, after, memBefore, memAfter, batchLatencies, readLatencies)
	verifyStorageScale(t, s, oracle, names, config.Name, workers, now)
}

func storageScaleSettings(t *testing.T) (string, int, int, int, bool) {
	kind := os.Getenv("STORAGE_SCALE_ENGINE")
	if kind == "" {
		t.Skip("set STORAGE_SCALE_ENGINE to run storage qualification")
	}
	if !slices.Contains(storageBackends, kind) {
		t.Fatalf("unknown engine %q", kind)
	}
	return kind, scaleInt(t, "STORAGE_SCALE_METRICS", 100000), scaleInt(t, "STORAGE_SCALE_ROUNDS", 3), scaleInt(t, "STORAGE_SCALE_WORKERS", 32), os.Getenv("STORAGE_SCALE_LATE") == "1"
}

func setupStorageScale(t *testing.T, s *storageBackend, names []string, config storageMetricConfig, seed []whisper.TimeSeriesPoint, workers, rounds int, late bool) {
	started := time.Now()
	storageScaleParallel(t, workers, len(names), func(i int) error {
		c := config
		c.Name = names[i]
		if err := s.create(c); err != nil {
			return err
		}
		return s.update(c.Name, seed)
	})
	t.Logf("engine=%s metrics=%d workers=%d rounds=%d late=%v setup=%s", s.kind, len(names), workers, rounds, late, time.Since(started))
}

func runStorageScale(t *testing.T, s, oracle *storageBackend, names []string, config storageMetricConfig, rounds, workers int, late bool, now *atomic.Int64) (time.Duration, syscall.Rusage, syscall.Rusage, runtime.MemStats, runtime.MemStats, []int64, []int64) {
	count := len(names)
	batchLatencies := make([]int64, count*rounds)
	readLatencies := make([]int64, ((count+9)/10)*rounds)
	var before, after syscall.Rusage
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	profile := storageScaleProfile(t)
	if profile != nil {
		defer func() { pprof.StopCPUProfile(); storageMust(t, profile.Close()) }()
	}
	storageMust(t, syscall.Getrusage(syscall.RUSAGE_SELF, &before))
	started := time.Now()
	for round := 0; round < rounds; round++ {
		batch := make([]whisper.TimeSeriesPoint, 8)
		now.Store(int64(storageWriteBatch(batch, round, late)))
		storageScaleParallel(t, workers, count, func(i int) error {
			start := time.Now()
			if err := s.update(names[i], batch); err != nil {
				return err
			}
			batchLatencies[round*count+i] = time.Since(start).Nanoseconds()
			if i%10 == 0 {
				start = time.Now()
				if _, err := s.fetch(names[i], int(now.Load())-120, int(now.Load())); err != nil {
					return err
				}
				readLatencies[round*((count+9)/10)+i/10] = time.Since(start).Nanoseconds()
			}
			return nil
		})
		storageMust(t, oracle.update(config.Name, batch))
	}
	elapsed := time.Since(started)
	storageMust(t, syscall.Getrusage(syscall.RUSAGE_SELF, &after))
	runtime.ReadMemStats(&memAfter)
	if profile != nil {
		pprof.StopCPUProfile()
	}
	return elapsed, before, after, memBefore, memAfter, batchLatencies, readLatencies
}

func storageScaleProfile(t *testing.T) *os.File {
	path := os.Getenv("STORAGE_SCALE_CPU_PROFILE")
	if path == "" {
		return nil
	}
	profile, err := os.Create(path)
	storageMust(t, err)
	storageMust(t, pprof.StartCPUProfile(profile))
	return profile
}

func logStorageScale(t *testing.T, kind string, count, rounds int, elapsed time.Duration, before, after syscall.Rusage, memBefore, memAfter runtime.MemStats, batchLatencies, readLatencies []int64) {
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	slices.Sort(batchLatencies)
	slices.Sort(readLatencies)
	points := count * rounds * 8
	rss := after.Maxrss
	if runtime.GOOS == "darwin" {
		rss /= 1024
	}
	t.Logf("engine=%s points=%d reads=%d wall=%s cpu-us/point=%.3f allocated-B/point=%.1f batch-p95-us=%.3f batch-p99-us=%.3f read-p99-us=%.3f maxrss-KiB=%d", kind, points, len(readLatencies), elapsed, (cpu(after)-cpu(before))*1e6/float64(points), float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/float64(points), float64(batchLatencies[len(batchLatencies)*95/100])/1000, float64(batchLatencies[len(batchLatencies)*99/100])/1000, float64(readLatencies[len(readLatencies)*99/100])/1000, rss)
}

func verifyStorageScale(t *testing.T, s, oracle *storageBackend, names []string, configName string, workers int, now *atomic.Int64) {
	storageMust(t, s.reopen())
	storageScaleCompare(t, s, oracle, names, configName, workers, now, 600)
	sample := storageScaleSample(names)
	verify := func() { storageScaleVerifySample(t, s, oracle, sample, configName, now) }
	verify()
	started := time.Now()
	storageMust(t, s.compact(sample))
	storageMust(t, s.reopen())
	verify()
	storageMust(t, s.close()) // Stabilize obsolete-file deletion before counting disk.
	files, logical, allocated := storageScaleFootprint(t, s.dir)
	t.Logf("engine=%s verified-metrics=%d maintenance-sample=%d maintenance=%s files=%d logical-bytes=%d allocated-bytes=%d", s.kind, len(names), len(sample), time.Since(started), files, logical, allocated)
}

func storageScaleCompare(t *testing.T, s, oracle *storageBackend, names []string, configName string, workers int, now *atomic.Int64, age int) {
	want, err := oracle.fetch(configName, int(now.Load())-age, int(now.Load()))
	storageMust(t, err)
	storageScaleParallel(t, workers, len(names), func(i int) error {
		got, err := s.fetch(names[i], int(now.Load())-age, int(now.Load()))
		if err != nil {
			return err
		}
		if diff := storageSeriesDiff(want, got); diff != "" {
			return fmt.Errorf("%s: %s", names[i], diff)
		}
		return nil
	})
}

func storageScaleSample(names []string) []string {
	sample := make([]string, 0, 256)
	for i := 0; i < len(names); i += max(1, len(names)/252) {
		sample = append(sample, names[i])
	}
	return sample
}

func storageScaleVerifySample(t *testing.T, s, oracle *storageBackend, sample []string, configName string, now *atomic.Int64) {
	for _, age := range []int{600, 3600, 21600} {
		want, err := oracle.fetch(configName, int(now.Load())-age, int(now.Load()))
		storageMust(t, err)
		for _, name := range sample {
			got, err := s.fetch(name, int(now.Load())-age, int(now.Load()))
			storageMust(t, err)
			if diff := storageSeriesDiff(want, got); diff != "" {
				t.Fatalf("%s age=%d: %s", name, age, diff)
			}
		}
	}
}

func storageScaleFootprint(t *testing.T, dir string) (int, int64, int64) {
	var allocated, logical int64
	files := 0
	storageMust(t, filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		files++
		logical += info.Size()
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			allocated += st.Blocks * 512
		}
		return nil
	}))
	return files, logical, allocated
}

func storageScaleParallel(t *testing.T, workers, count int, fn func(int) error) {
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < count; i += workers {
				if err := fn(i); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func scaleInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return n
}
