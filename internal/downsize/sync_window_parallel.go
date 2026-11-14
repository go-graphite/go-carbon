package downsize

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type syncWindowProgressKey struct{}

// Fixed workers and an unbuffered queue bound live range data by streams, not
// catalog size. Every exit joins workers before journals or source pins close.
func runSyncWindowRanges(ctx context.Context, names, streams int, run func(context.Context, int, int) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range min(max(1, streams), (names+chunkstore.NativeMaxRangeNames-1)/chunkstore.NativeMaxRangeNames) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for start := range jobs {
				if ctx.Err() != nil {
					continue
				}
				if err := run(ctx, start, min(start+chunkstore.NativeMaxRangeNames, names)); err != nil {
					cancel(err)
				}
			}
		}()
	}
feed:
	for start := 0; start < names; start += chunkstore.NativeMaxRangeNames {
		select {
		case jobs <- start:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	return context.Cause(ctx)
}

// Digest responses are bounded to one range and reduced immediately. A fill
// may cover millions of names; retaining their full digest maps defeats streaming.
func (b *syncLiveBackend) windowPointTotal(ctx context.Context, target string, names []string, w syncRange) (int64, error) {
	var total atomic.Int64
	err := runSyncWindowRanges(ctx, len(names), b.o.copyStreams, func(ctx context.Context, start, end int) error {
		d, err := b.Digest(ctx, target, names[start:end], w.From, w.Until)
		if err == nil {
			total.Add(countWindowPoints(d))
		}
		return err
	})
	return total.Load(), err
}

// Admission rejection applies no data. Retry the same durable identity, within
// the host wait budget, so using more streams than receiver slots cannot abort
// an otherwise healthy fill. Ambiguous transport errors remain journal recovery.
func (b *syncLiveBackend) executeWindowRange(ctx context.Context, c *nativeController, t task, names []string) error {
	wait := b.o.hostWait
	if wait <= 0 {
		wait = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for attempt := 0; ; attempt++ {
		err := c.executeRange(ctx, t, names)
		var api *apiError
		if !errors.As(err, &api) || api.Status != http.StatusTooManyRequests {
			return err
		}
		b.busy.Add(1)
		if err := sleepContext(ctx, syncBusyDelay(attempt)); err != nil {
			return err
		}
	}
}
