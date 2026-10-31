//go:build darwin || linux

package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

var backgroundSink atomic.Uint64

// Simulate mandatory receivers/GC inside this process, so the controller
// charges it. Fractional cores use 20ms duty cycles; zero starts no work.
func backgroundCPU(cores float64) func() {
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	for cores > 0 {
		share := min(cores, 1)
		cores -= share
		workers.Go(func() {
			x := uint64(1)
			for ctx.Err() == nil {
				started := time.Now()
				until := started.Add(time.Duration(share * float64(20*time.Millisecond)))
				for time.Now().Before(until) {
					for range 4096 {
						x ^= x << 13
						x ^= x >> 7
						x ^= x << 17
					}
				}
				backgroundSink.Store(x)
				if delay := 20*time.Millisecond - time.Since(started); delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
					}
				}
			}
		})
	}
	return func() { cancel(); workers.Wait() }
}
