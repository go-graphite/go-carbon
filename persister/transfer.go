package persister

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
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
		for _, stripe := range stripes {
			p.storeMutex[stripe].Lock()
		}
		err := func() error {
			defer func() {
				for i := len(stripes) - 1; i >= 0; i-- {
					p.storeMutex[stripes[i]].Unlock()
				}
			}()
			var values []*points.Points
			var updates []store.MetricUpdate
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
				atomic.AddUint32(&p.committedPoints, uint32(len(batch.Data)))
				atomic.AddUint32(&p.updateOperations, 1)
			}
			started := time.Now()
			err := db.UpdateBatch(ctx, updates, nil)
			for _, batch := range values {
				p.writeStats.shared.observe(len(batch.Data), time.Since(started), err)
			}
			if errors.Is(err, store.ErrFormat) {
				// Native chunk stores have no continuation batch writer.
				for i, batch := range values {
					if err := p.metricStore.UpdateMany(ctx, batch.Metric, batch.Data); err != nil {
						for _, pending := range values[i:] {
							p.requeue(pending)
						}
						return err
					}
					confirm(batch)
				}
				return nil
			}
			if err != nil {
				for _, batch := range values {
					p.requeue(batch)
				}
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
