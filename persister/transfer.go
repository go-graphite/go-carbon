package persister

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/lockset"
	"github.com/go-graphite/go-carbon/points"
)

// FlushForTransfer persists queued points of existing shared metrics before a
// raw export. It bypasses sparse writeout delays, preserves confirmation/input
// log tracking, and shares each commit among at most eight metrics. Receivers
// keep accepting new points; the final move still needs its input fence/delta.
func (p *Whisper) FlushForTransfer(ctx context.Context, names []string) error {
	db, ok := p.metricStore.(batchMetricStore)
	if !ok || p.requeue == nil || p.confirm == nil || p.mirrorFiles || p.mirrorImportMissing || p.incrementalStore != nil && !p.incrementalComplete.Load() {
		return errors.New("transfer flush requires completed shared storage and cache confirmation")
	}
	for start := 0; start < len(names); start += 8 {
		if err := ctx.Err(); err != nil {
			return err
		}
		group := names[start:min(start+8, len(names))]
		stripes := make([]uint64, 0, len(group))
		for _, name := range group {
			stripes = append(stripes, storeMutexIndex(name))
		}
		slices.Sort(stripes)
		stripes = slices.Compact(stripes)
		lockset.Lock(p.storeMutex[:], stripes)
		err := func() (result error) {
			defer func() {
				for i := len(stripes) - 1; i >= 0; i-- {
					p.storeMutex[stripes[i]].Unlock()
				}
			}()
			var values []*points.Points
			var updates []store.MetricUpdate
			// The popped batches remain owned here until confirmation. In
			// particular, cancellation after commit must still confirm success.
			pending := 0
			defer func() {
				if pending == len(values) {
					return
				}
				names := make([]string, 0, len(values)-pending)
				pointCount := 0
				for _, batch := range values[pending:] {
					p.requeue(batch)
					names = append(names, batch.Metric)
					pointCount += len(batch.Data)
				}
				reason := transferFlushReason(result)
				p.writeStats.transferRequeued[reason].Add(1)
				p.logger.Warn("transfer flush requeued", zap.String("reason", transferFlushReasons[reason]), zap.Strings("metrics", names), zap.Int("points", pointCount), zap.Error(result))
			}()
			for _, name := range group {
				if batch, ok := p.pop(name); ok {
					values = append(values, batch)
					updates = append(updates, store.MetricUpdate{Name: name, Points: batch.Data})
				}
			}
			if len(values) == 0 {
				return nil
			}
			confirm := func(batch *points.Points) {
				if guarded, ok := p.metricStore.(interface{ SharedWriteGuard(string) func() }); ok {
					release := guarded.SharedWriteGuard(batch.Metric)
					p.confirm(batch)
					release()
				} else {
					p.confirm(batch)
				}
				p.recordCommittedPoints(len(batch.Data))
				atomic.AddUint32(&p.updateOperations, 1)
				pending++
			}
			started := time.Now()
			err := db.UpdateBatch(ctx, updates, nil)
			if errors.Is(err, store.ErrFormat) {
				// Native chunk stores have no continuation batch writer.
				for _, batch := range values {
					started := time.Now()
					err := p.metricStore.UpdateMany(ctx, batch.Metric, batch.Data)
					p.observeTransferWrite(batch, time.Since(started), err)
					if err != nil {
						return err
					}
					confirm(batch)
				}
				return nil
			}
			for _, batch := range values {
				p.observeTransferWrite(batch, time.Since(started), err)
			}
			if err != nil {
				return err
			}
			for _, batch := range values {
				confirm(batch)
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

// Expected request/absence errors are retries, not storage failures. Inspect all
// leaves so a joined storage error and cancellation still fail closed.
func transferFlushReason(err error) int {
	if group, ok := err.(interface{ Unwrap() []error }); ok {
		reason := -1
		for _, child := range group.Unwrap() {
			next := transferFlushReason(child)
			if reason != -1 && reason != next {
				return 3
			}
			reason = next
		}
		if reason >= 0 {
			return reason
		}
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return transferFlushReason(wrapped.Unwrap())
	}
	switch {
	case errors.Is(err, context.Canceled):
		return 0
	case errors.Is(err, context.DeadlineExceeded):
		return 1
	case errors.Is(err, store.ErrNotFound):
		return 2
	default:
		return 3
	}
}

func (p *Whisper) observeTransferWrite(batch *points.Points, took time.Duration, err error) {
	if err == nil || transferFlushReason(err) == 3 {
		p.writeStats.shared.observe(len(batch.Data), took, err)
	}
}
