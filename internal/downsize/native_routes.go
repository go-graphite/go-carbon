package downsize

import (
	"context"
	"sync"
)

// Do not reorder committed streams or mint new range identities. With the
// source barrier, only a contiguous run from one donor can execute together.
// A later donor may disagree in a coarse archive even when fine points differ.
func runNativeRoutes(ctx context.Context, routes []route, workers int, sourceBarrier bool, run func(context.Context, route) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan route)
	var running, outstanding sync.WaitGroup
	var first error
	var once sync.Once
	for range workers {
		running.Add(1)
		go func() {
			defer running.Done()
			for r := range jobs {
				err := run(ctx, r)
				if err != nil {
					once.Do(func() { first = err; cancel() })
				}
				outstanding.Done()
				if err != nil {
					return
				}
			}
		}()
	}
	var previous string
enqueue:
	for _, r := range routes {
		if sourceBarrier && previous != "" && previous != r.sourceURL {
			outstanding.Wait()
		}
		if ctx.Err() != nil {
			break
		}
		outstanding.Add(1)
		select {
		case jobs <- r:
			previous = r.sourceURL
		case <-ctx.Done():
			outstanding.Done()
			break enqueue
		}
	}
	close(jobs)
	running.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
