package v2convert

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ScanResult is an independent physical-table inventory, not a resolved view
// of a database. Results retain the input path order at every worker count.
type ScanResult struct {
	Stats   ScanStats
	Seconds float64
	Err     error
}

// ScanFiles reads each SST forwards with at most workers tables open. Every
// worker owns its decoder, digest and counters; there is no shared record lock.
// Memory scales with the worker count and SST block size, not source bytes.
// Overlapping files, merge operands and range deletions remain unresolved.
func ScanFiles(ctx context.Context, paths []string, workers int) ([]ScanResult, error) {
	if workers < 1 {
		return nil, errors.New("scan workers must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	results := make([]ScanResult, len(paths))
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(workers, len(paths)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(paths) {
					return
				}
				start := time.Now()
				if err := ctx.Err(); err != nil {
					results[i].Err = err
					continue
				}
				results[i].Stats, results[i].Err = ScanSST(ctx, paths[i], nil)
				results[i].Seconds = time.Since(start).Seconds()
			}
		}()
	}
	wg.Wait()
	return results, nil
}
