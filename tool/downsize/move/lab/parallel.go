package main

import (
	"context"
	"sync"
	"sync/atomic"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Phase measurements use the same bounded concurrency as the HTTP driver.
// Each worker owns its decoder arena until its import and proofs finish.
func parallel(ctx context.Context, workers, count int, work func(context.Context, int, *store.TransferBuffer) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for range min(workers, count) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buffer store.TransferBuffer
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= count {
					return
				}
				if err := work(ctx, i, &buffer); err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
