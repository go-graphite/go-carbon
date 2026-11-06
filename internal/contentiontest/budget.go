// Package contentiontest provides opt-in production-concurrency regression guards.
package contentiontest

import (
	"os"
	"runtime"
	"runtime/metrics"
	"sync"
	"testing"
	"time"
)

func WaitSeconds() float64 {
	s := []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}
	metrics.Read(s)
	return s[0].Value.Float64()
}

// Run measures completed mutex waits process-wide, so callers must not run in
// parallel with unrelated work. Wall time includes join, but excludes setup.
// Run these in dedicated CI jobs: GO_CARBON_CONTENTION=1 go test -p=1 ...
func Run(t *testing.T, workers, rounds int, budget float64, work func(worker, round int)) {
	t.Helper()
	if os.Getenv("GO_CARBON_CONTENTION") != "1" {
		t.Skip("set GO_CARBON_CONTENTION=1 for production-concurrency wait budgets")
	}
	previous := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(previous)
	runtime.GC()
	wait, elapsed := Measure(workers, rounds, work)
	rate := wait / elapsed
	t.Logf("workers=%d ops=%d wall=%.4fs wait=%.4fs wait/wall=%.3f budget=%.1f", workers, workers*rounds, elapsed, wait, rate, budget)
	if rate > budget {
		t.Errorf("mutex wait %.3f s/s exceeds %.1f s/s budget", rate, budget)
	}
}

// Measure is separated so a controlled global-lock workload can verify that
// the regression guard really detects serialization on the installed runtime.
func Measure(workers, rounds int, work func(worker, round int)) (wait, elapsed float64) {
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(workers)
	for w := range workers {
		done.Go(func() {
			ready.Done()
			<-start
			for i := range rounds {
				work(w, i)
			}
		})
	}
	ready.Wait()
	before, at := WaitSeconds(), time.Now()
	close(start)
	done.Wait()
	elapsed = time.Since(at).Seconds()
	wait = WaitSeconds() - before
	return
}
