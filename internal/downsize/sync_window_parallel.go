package downsize

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type syncWindowProgressKey struct{}
type syncWindowAttemptKey struct{}

// A backend can retry a donor from its receipts within the same invocation.
// Publish each completed range once across those attempts.
func syncWindowProgressContext(ctx context.Context, report func(int, bool)) context.Context {
	var seen sync.Map
	return context.WithValue(ctx, syncWindowProgressKey{}, func(key string, n int, replayed bool) {
		if _, loaded := seen.LoadOrStore(key, true); !loaded {
			report(n, replayed)
		}
	})
}

// Fixed workers and an unbuffered queue bound live range data by streams, not
// catalog size. Every exit joins workers before journals or source pins close.
func runSyncWindowRanges(ctx context.Context, names, streams int, run func(context.Context, int, int) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// The outer CopyWindow owns restart retries. Nested splits and optional
	// accounting must return unavailable ranges to this pass, releasing their
	// pins and worker slots so unrelated ranges can finish first.
	ctx = context.WithValue(ctx, syncWindowAttemptKey{}, true)
	rangeNames := min(chunkstore.NativeMaxRangeNames, max(1, int(nativeRangeNames)))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var retryMu sync.Mutex
	var retry error
	for range min(max(1, streams), (names+rangeNames-1)/rangeNames) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for start := range jobs {
				if ctx.Err() != nil {
					continue
				}
				if err := run(ctx, start, min(start+rangeNames, names)); err != nil {
					if ctx.Err() == nil && restartCopyError(err) {
						retryMu.Lock()
						if retry == nil {
							retry = err
						}
						retryMu.Unlock()
					} else {
						cancel(err)
					}
				}
			}
		}()
	}
feed:
	for start := 0; start < names; start += rangeNames {
		select {
		case jobs <- start:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return retry
}

// Admission rejection applies no data. Retry the same durable identity, within
// the host wait budget, so using more streams than receiver slots cannot abort
// an otherwise healthy fill. Ambiguous transport errors remain journal recovery.
func (b *syncLiveBackend) executeWindowRange(ctx context.Context, c *nativeController, t task, names []string) error {
	wait := b.o.hostWait
	if wait <= 0 {
		wait = 30 * time.Minute
	}
	deadline := time.Now().Add(wait)
	transfers := 0
	for attempt := 0; ; attempt++ {
		started := time.Now()
		if err := b.health.wait(ctx); err != nil {
			return err
		}
		deadline = deadline.Add(time.Since(started))
		// Replaying the initial receipt cannot make deferred ordinals progress.
		// Reuse the native deferred protocol, whose chained receipt identities
		// retain lost-ack safety while allowing a fresh source snapshot.
		c.stateMu.Lock()
		if saved, ok := c.journal.tasks[taskKey(t)]; ok {
			t = saved
		}
		c.stateMu.Unlock()
		var err error
		if t.Native != nil && !t.Native.Intent && !t.Native.complete() {
			// The scan's borrowed pin can predate a source flush. Own a fresh
			// pin for this attempt without closing any concurrent borrower's pin.
			err = func() error {
				next := &nativeController{mover: c.mover, committedWindow: c.committedWindow, sessions: map[string]*nativeRemoteSession{}}
				defer next.release()
				return next.executeDeferred(ctx, t, names)
			}()
		} else {
			err = c.executeRange(ctx, t, names)
		}
		if err == nil {
			transfers++
			c.stateMu.Lock()
			t = c.journal.tasks[taskKey(t)]
			c.stateMu.Unlock()
			if t.Native == nil || t.Native.complete() {
				return nil
			}
			exhausted := transfers >= max(1, b.o.copyAttempts) || time.Now().After(deadline)
			reportWindowDeferred(t, names, transfers, exhausted)
			if exhausted {
				// The caller retains these exact names in Failed. A durable HTTP
				// acknowledgement never completes an uncopied ordinal.
				return nil
			}
			delay := b.retryTick
			if delay <= 0 {
				delay = 2 * time.Second
			}
			waitCtx, cancel := context.WithDeadline(ctx, deadline)
			err = sleepContext(waitCtx, delay)
			cancel()
			if err != nil {
				if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
					// This is exhausted deferral work, not a transport timeout
					// for the outer restart waiter to retry indefinitely.
					reportWindowDeferred(t, names, transfers, true)
					return nil
				}
				return err
			}
			continue
		}
		var api *apiError
		if !errors.As(err, &api) || api.Status != http.StatusTooManyRequests {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		b.busy.Add(1)
		waitCtx, cancel := context.WithDeadline(ctx, deadline)
		err = sleepContext(waitCtx, syncBusyDelay(attempt))
		cancel()
		if err != nil {
			return err
		}
	}
}

func reportWindowDeferred(t task, names []string, attempt int, exhausted bool) {
	level := "warn"
	if exhausted {
		level = "ERROR"
	}
	receipt, reasons := windowDeferredReasons(t, names)
	for i, name := range names {
		if reason, ok := reasons[i]; ok {
			event("sync_window_deferred", map[string]any{"level": level, "metric": name, "source": t.Source, "target": t.Target, "receipt": receipt.Identity, "attempt": attempt, "reason": reason, "exhausted": exhausted})
		}
	}
}

// windowDeferredReasons maps each ordinal the latest receipt left undone to
// the receiver's skip reason, or to the generic omission reason.
func windowDeferredReasons(t task, names []string) (chunkstore.NativeReceipt, map[int]string) {
	receipt := t.Native.Receipt
	if n := len(t.Native.Combines); n > 0 && t.Native.Combines[n-1].Receipt != nil {
		receipt = *t.Native.Combines[n-1].Receipt
	}
	skipped := make(map[int]string, len(receipt.Skipped))
	for _, skip := range receipt.Skipped {
		skipped[skip.Ordinal] = skip.Reason
	}
	reasons := map[int]string{}
	for i := range names {
		if t.Native.done(i) {
			continue
		}
		reason := skipped[i]
		if reason == "" {
			reason = "native receipt omitted name without a reason (source absent, pending or changed; legacy target creation fence)"
		}
		reasons[i] = reason
	}
	return receipt, reasons
}
