package persister

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

type batchMetricStore interface {
	UpdateBatch(context.Context, []store.MetricUpdate, *store.ContinuationMaintenance) error
}

// SetWriteoutBatchReceiver supplies a nonblocking read of already queued work.
func (p *Whisper) SetWriteoutBatchReceiver(recv func() string) { p.tryRecv = recv }

// SetStoreUpdateBatchSize bounds the queued updates sharing one store commit.
func (p *Whisper) SetStoreUpdateBatchSize(size int) { p.storeUpdateBatchSize = max(1, min(size, 8)) }

func (p *Whisper) canBatchShared() bool {
	if p.storeUpdateBatchSize <= 1 {
		return false
	}
	_, ok := p.metricStore.(batchMetricStore)
	return ok && p.tryRecv != nil && p.requeue != nil && !p.mirrorFiles && !p.mirrorImportMissing && (p.incrementalStore == nil || p.incrementalComplete.Load())
}

func (p *Whisper) takeBatchToken() bool {
	if p.backlogged != nil && p.backlogged() {
		return true
	}
	select {
	case <-p.updateTokens():
		return true
	default:
		return false
	}
}

func (p *Whisper) storeSharedBatch(metrics []string) {
	start := time.Now()
	ctx, timer := stagetime.Start(context.Background())
	defer timer.Finish()
	timer.Enter(stagetime.LockWait)
	stripes := make([]uint64, 0, len(metrics))
	for _, metric := range metrics {
		stripes = append(stripes, storeMutexIndex(metric))
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	for _, stripe := range stripes {
		p.storeMutex[stripe].Lock()
	}
	unlock := func() {
		for i := len(stripes) - 1; i >= 0; i-- {
			p.storeMutex[stripes[i]].Unlock()
		}
	}
	// Release all group stripes before a missing/native metric uses the ordinary
	// create/update route. Requeue retains the input log's original batch identity.
	fallback := false
	func() {
		defer unlock()
		values := make([]*points.Points, 0, len(metrics))
		updates := make([]store.MetricUpdate, 0, len(metrics))
		seen := make(map[string]bool, len(metrics))
		for _, metric := range metrics {
			if seen[metric] {
				continue
			}
			seen[metric] = true
			if _, converting := p.converting.Load(metric); converting {
				continue
			}
			if p.writeoutReady != nil && !p.writeoutReady(metric) {
				continue
			}
			if batch, ok := p.pop(metric); ok {
				values = append(values, batch)
				updates = append(updates, store.MetricUpdate{Name: metric, Points: batch.Data})
			}
		}
		if len(values) == 0 {
			return
		}
		var maintenance *store.ContinuationMaintenance
		if p.outOfOrder.enabled {
			m := p.sharedMaintenance()
			maintenance = &m
		}
		err := p.metricStore.(batchMetricStore).UpdateBatch(ctx, updates, maintenance)
		timer.Leave()
		for _, batch := range values {
			p.writeStats.shared.observe(len(batch.Data), time.Since(start), err)
		}
		if err != nil {
			for _, batch := range values {
				p.requeue(batch)
			}
			fallback = errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrFormat)
			if !fallback {
				p.logger.Error("write shared batch", zap.Error(err))
			}
			return
		}
		for _, batch := range values {
			atomic.AddUint32(&p.committedPoints, uint32(len(batch.Data)))
			p.recordUpdate()
			if p.confirm != nil {
				if guarded, ok := p.metricStore.(interface{ SharedWriteGuard(string) func() }); ok {
					func() {
						release := guarded.SharedWriteGuard(batch.Metric)
						defer release()
						p.confirm(batch)
					}()
				} else {
					p.confirm(batch)
				}
			}
			if p.tagsEnabled && p.taggedFn != nil && strings.Contains(batch.Metric, ";") {
				p.taggedFn(batch.Metric, false)
			}
			stagetime.ObserveUpdate(time.Since(start))
		}
	}()
	if fallback {
		for _, metric := range metrics {
			p.store(metric)
		}
	}
}
