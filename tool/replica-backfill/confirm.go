package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"golang.org/x/time/rate"
)

var errUnconfirmed = errors.New("receiver confirmation failed")

// writeConfirmed holds the batch checkpoint until all submitted values can be
// read back. TCP success alone cannot distinguish admitted points from quota
// drops. Reads may include the input cache, so this is not a durability check.
func (e *engine) writeConfirmed(ctx context.Context, pts []point) (err error) {
	if e.cfg.confirmTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.cfg.confirmTimeout)
		defer cancel()
	}
	e.stats.submitted.Add(int64(len(pts)))
	defer func() {
		if err != nil {
			e.stats.confirmationFailures.Add(1)
			err = fmt.Errorf("%w: %w", errUnconfirmed, err)
		}
	}()
	delay := e.cfg.confirmDelay
	for attempt := 0; ; attempt++ {
		if err := e.out.write(ctx, pts); err != nil {
			return err
		}
		// Allow the receiver to consume its socket and input queue before
		// reading. Cancellation leaves the batch outside the checkpoint.
		if err := waitConfirmation(ctx, delay); err != nil {
			return err
		}
		names := make([]string, 0)
		seen := make(map[string]bool)
		for _, p := range pts {
			if !seen[p.name] {
				names = append(names, p.name)
				seen[p.name] = true
			}
		}
		got, err := e.local.fetchFresh(ctx, names, e.cfg.from-1, e.cfg.until)
		if err != nil {
			return fmt.Errorf("receiver confirmation: %w", err)
		}
		missing := pts[:0]
		for _, p := range pts {
			v, ok := got[p.name].at(p.ts)
			if !ok || math.Float64bits(v) != math.Float64bits(p.value) {
				missing = append(missing, p)
			}
		}
		e.stats.confirmed.Add(int64(len(pts) - len(missing)))
		pts = missing
		if len(pts) == 0 {
			return nil
		}
		e.stats.unconfirmedObservations.Add(int64(len(pts)))
		log.Printf("ERROR: receiver has not confirmed %d points (possible quota drops); batch is not checkpointed, retry=%d/%d", len(pts), attempt, e.cfg.confirmRetries)
		if attempt >= e.cfg.confirmRetries {
			return fmt.Errorf("receiver confirmation exhausted: %d points still unconfirmed; lower -write-rate and resume with the same checkpoint", len(pts))
		}
		limit := e.out.reduceRate()
		delay = min(max(2*delay, time.Millisecond), e.cfg.maxBackoff)
		log.Printf("receiver confirmation: retrying %d points in %s at %.0f points/s", len(pts), delay, limit)
		// Back off before resending as well as before the following read.
		if err := waitConfirmation(ctx, delay); err != nil {
			return err
		}
		e.stats.resentBatches.Add(1)
		e.stats.resentPoints.Add(int64(len(pts)))
	}
}

func waitConfirmation(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// rate.Limiter is shared by workers and safe to update while the writer waits.
// Serialize the read/halve/set so simultaneous failures each reduce the rate.
func (w *writer) reduceRate() rate.Limit {
	w.rateMu.Lock()
	defer w.rateMu.Unlock()
	limit := w.limiter.Limit()
	if limit == rate.Inf {
		limit = 100000
	}
	limit = max(1, limit/2)
	w.limiter.SetLimit(limit)
	return limit
}
