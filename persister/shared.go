package persister

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
	"go.uber.org/zap"
)

// MetricStore is the persister's write surface. The app owns its lifetime.
type MetricStore interface {
	Metadata(context.Context, string) (store.Metadata, error)
	Create(context.Context, store.MetricConfig) (store.Metadata, error)
	UpdateMany(context.Context, string, []points.Point) error
}

func (p *Whisper) SetMetricStore(metricStore MetricStore) { p.metricStore = metricStore }

// SetMirrorFiles retains the file backend as a current rollback copy during a
// bounded shared-store canary. Configure it before starting persister workers.
func (p *Whisper) SetMirrorFiles(enabled bool) {
	p.mirrorFiles = enabled
	if enabled && p.mirrorState == nil {
		p.mirrorState = &MirrorState{}
	}
}

func (p *Whisper) storeShared(ctx context.Context, metric string) {
	// store entered the route stage; metadata preparation continues it.
	timer := stagetime.FromContext(ctx)
	// Readers hold the stripe shared across their persistent snapshot and cache
	// read. Popped points stay visible in the cache until confirmed, so only the
	// confirmation must exclude them: a read then sees the batch in the cache,
	// in the store, or both. Holding the stripe across the store commit made
	// reads wait behind every write-path delay. The rollback mirror also writes
	// the original inside the window and keeps the full span.
	var confirmGuard func(string) func()
	if coordinated, ok := p.metricStore.(interface{ SharedWriteGuard(string) func() }); ok {
		if p.mirrorFiles {
			release := coordinated.SharedWriteGuard(metric)
			defer release()
		} else {
			confirmGuard = coordinated.SharedWriteGuard
		}
	}
	var path string
	if p.mirrorFiles {
		var ok bool
		path, ok = p.whisperPath(metric)
		if !ok {
			return
		}
	}
	// Without a rollback mirror, the write itself looks the metric up and
	// reports a missing one, so a separate lookup only to choose between create
	// and update would double the catalog reads of every write; create on
	// ErrNotFound instead. The mirror modes need the metadata up front.
	direct := !p.mirrorFiles && !p.mirrorImportMissing
	var metadata store.Metadata
	var err error
	if direct {
		metadata.Continuation = true // UpdateManyMaintained also handles chunk metrics
	} else if metadata, err = p.prepareSharedMetric(ctx, metric); err != nil && !errors.Is(err, store.ErrNotFound) {
		p.logger.Error("prepare shared metric", zap.String("metric", metric), zap.Error(err))
		return
	}
	// Acquire the original writer before popping: an open failure must leave
	// the batch available, and successful shared commits must have a file copy.
	var mirror *whisper.Whisper
	var newFile bool
	var mirrorSecond int64
	var archiveValues []store.ArchiveValue
	var windowStore mirrorWindowStore
	if p.mirrorFiles {
		var compatible bool
		windowStore, compatible = p.metricStore.(mirrorWindowStore)
		if !compatible {
			p.logger.Error("rollback mirror requires atomic archive-window updates")
			return
		}
		mirror, newFile = p.openOrCreateWhisper(metric, path)
		if mirror == nil {
			return
		}
		defer func() {
			if mirror != nil {
				_ = mirror.Close()
			}
		}()
	}
	if err == nil && mirror != nil && (newFile || !mirrorPolicyMatches(mirror, metadata)) {
		if p.mirrorImportMissing {
			// A live original may have been recreated or changed policy since the
			// baseline. Release its inode before the coherent capture reacquires it.
			err = mirror.Close()
			mirror = nil
			if err == nil {
				err = p.repairMirror(ctx, metric, path)
			}
			if err != nil {
				p.mirrorFailed(metric)
				p.logger.Error("prepare shared metric", zap.String("metric", metric), zap.Error(err))
				return
			}
			mirror, newFile = p.openOrCreateWhisper(metric, path)
			if mirror == nil {
				return
			}
			metadata, err = p.metricStore.Metadata(ctx, metric)
		}
		if err == nil && (newFile || !mirrorPolicyMatches(mirror, metadata)) {
			p.logger.Error("rollback mirror does not match shared metric", zap.String("metric", metric))
			return
		}
	}
	if errors.Is(err, store.ErrNotFound) {
		if mirror != nil && !newFile {
			p.logger.Error("existing rollback metric requires conversion before shared writes", zap.String("metric", metric))
			return
		}
		if !p.createSharedMetric(ctx, metric) {
			return
		}
		err = nil
	}
	if err != nil {
		p.logger.Error("prepare shared metric", zap.String("metric", metric), zap.Error(err))
		return
	}
	values, exists := p.pop(metric)
	if !exists {
		return
	}
	if mirror != nil {
		mirrorSecond = whisper.Now().Unix()
		input := make([]*whisper.TimeSeriesPoint, len(values.Data))
		for i, point := range values.Data {
			input[i] = &whisper.TimeSeriesPoint{Time: int(point.Timestamp), Value: point.Value}
		}
		storedBefore := mirror.StoredArchiveBounds()
		monotonic := mirror.CanMirrorMonotonicBatch(input, mirrorSecond)
		var windows []whisper.ArchiveWindow
		if !monotonic {
			windows = mirrorInputWindows(mirror, input, mirrorSecond)
		}
		hadSidecar := mirror.OutOfOrderPath() != ""
		rewriteRevision := mirror.RewriteRevision()
		err := p.updateMany(mirror, path, input)
		if err == nil && hadSidecar && mirror.RewriteRevision() != rewriteRevision {
			// UpdateMany can fold an existing sidecar while rewriting a large
			// gap or rollup. Its changes extend beyond this input's windows.
			p.mirrorFailed(metric)
		}
		var compactionWindows []whisper.ArchiveWindow
		if err == nil && p.outOfOrder.enabled && p.outOfOrder.ticker != nil {
			var repairRequired bool
			mirror, repairRequired, compactionWindows = p.compactOutOfOrderResult(mirror, metric, path)
			if repairRequired {
				p.mirrorFailed(metric)
			}
		}
		windows = append(windows, compactionWindows...)
		if err == nil && mirror != nil {
			sourceSecond := whisper.Now().Unix()
			if monotonic && len(compactionWindows) == 0 && sourceSecond == mirrorSecond && mirror.OutOfOrderPoints == 0 && mirror.GetDiscardedPointsSinceOpen() == 0 && len(mirror.NonFatalErrors) == 0 {
				step := mirror.Retentions()[0].SecondsPerPoint()
				archiveValues = make([]store.ArchiveValue, len(input))
				for i, point := range input {
					archiveValues[i] = store.ArchiveValue{Archive: 0, Timestamp: int64(point.Time - point.Time%step), Value: point.Value, Present: true}
				}
			} else {
				if monotonic || sourceSecond != mirrorSecond {
					// A single archive has no virtual lower-buffer windows to save
					// before the write. A clock crossing also invalidates the fast
					// admission proof. Include both clocks' affected archives so a
					// backwards step can admit a previously too-old primary point.
					windows = append(windows, mirrorInputWindows(mirror, input, sourceSecond)...)
				}
				archiveValues, err = readMirrorWindows(mirror, windows)
			}
			if err == nil {
				retired, fullRepair := mirrorRetiredWindows(mirror, storedBefore)
				if fullRepair {
					p.mirrorFailed(metric)
				} else if len(retired) != 0 {
					var corrections []store.ArchiveValue
					corrections, err = readMirrorWindows(mirror, retired)
					archiveValues = append(archiveValues, corrections...)
				}
			}
		}
		if mirror != nil {
			closeErr := mirror.Close()
			mirror = nil
			err = errors.Join(err, closeErr)
		} else {
			err = errors.Join(err, errors.New("rollback mirror could not be reopened after OOO compaction"))
		}
		if err != nil {
			p.mirrorFailed(metric)
			p.logger.Error("write rollback mirror", zap.String("metric", metric), zap.Error(err))
			if p.requeue != nil {
				p.requeue(values)
			}
			return
		}
	}
	repaired := false
	if windowStore != nil {
		_, pending := p.mirrorState.pending.Load(metric)
		if pending {
			err = p.repairMirror(ctx, metric, path)
			repaired = err == nil
		} else {
			// The original's windows include primary slots and every affected
			// rollup. Apply that authoritative view directly: replaying input
			// first repeats aggregation and can displace a newer circular owner.
			err = windowStore.UpdateManyWithArchiveValues(ctx, metric, nil, archiveValues)
		}
	} else {
		start := time.Now()
		update := func() error {
			if maintained, ok := p.metricStore.(interface {
				UpdateManyMaintained(context.Context, string, []store.Point, store.ContinuationMaintenance) error
			}); ok && metadata.Continuation && p.outOfOrder.enabled {
				return maintained.UpdateManyMaintained(ctx, metric, values.Data, p.sharedMaintenance())
			}
			return p.metricStore.UpdateMany(ctx, metric, values.Data)
		}
		err = update()
		if direct && errors.Is(err, store.ErrNameTooLong) {
			if p.confirm != nil {
				p.confirm(values)
			}
			p.countNameTooLong(metric, len(values.Data))
			return
		}
		if direct && errors.Is(err, store.ErrNotFound) {
			if !p.createAfterPop(ctx, metric, values) {
				return
			}
			err = update()
		}
		p.writeStats.shared.observe(len(values.Data), time.Since(start), err)
	}
	timer.Leave()
	if err != nil {
		p.mirrorFailed(metric)
		p.logger.Error("write shared metric", zap.String("metric", metric), zap.Error(err))
		if p.requeue != nil {
			p.requeue(values)
		}
		return
	}
	// Corrections contain the source's actual archive values and are not
	// re-admitted using the shared clock. Routine second crossings therefore
	// need no full snapshot; retain repair for a genuinely pending mirror.
	if p.mirrorFiles {
		_, pending := p.mirrorState.pending.Load(metric)
		if !repaired && pending {
			if err := p.repairMirror(ctx, metric, path); err != nil {
				p.mirrorFailed(metric)
				p.logger.Error("reconcile mirrored batch", zap.String("metric", metric), zap.Error(err))
				if p.requeue != nil {
					p.requeue(values)
				}
				return
			}
		}
		p.mirrorState.pending.Delete(metric)
	}
	// Periodic-sync stores can confirm updates before the next WAL sync.
	// Confirmed points are then subject to the configured crash-loss window.
	atomic.AddUint32(&p.committedPoints, uint32(len(values.Data)))
	atomic.AddUint32(&p.updateOperations, 1)
	if p.confirm != nil {
		if confirmGuard != nil {
			func() {
				release := confirmGuard(metric)
				defer release()
				p.confirm(values)
			}()
		} else {
			p.confirm(values)
		}
	}
	if p.tagsEnabled && p.taggedFn != nil && strings.Contains(metric, ";") {
		p.taggedFn(metric, false)
	}
}

func (p *Whisper) sharedMaintenance() store.ContinuationMaintenance {
	return store.ContinuationMaintenance{MinBytes: p.outOfOrder.threshold,
		MinPoints: p.outOfOrder.policy.MinPoints, MaxPointAge: p.outOfOrder.policy.MaxPointAge, RetentionMargin: p.outOfOrder.policy.RetentionMargin,
		Allow: func() bool {
			if p.outOfOrder.ticker == nil {
				return false
			}
			select {
			case capacity := <-p.outOfOrder.ticker.C:
				return capacity
			default:
				return false
			}
		},
		Result: func(merged bool, err error) {
			if p.outOfOrder.policy.MinPoints > 0 {
				atomic.AddUint32(&p.oooCompactionChecks, 1)
			}
			if err != nil {
				atomic.AddUint32(&p.oooCompactErrors, 1)
			} else if merged {
				atomic.AddUint32(&p.oooCompactions, 1)
			} else {
				atomic.AddUint32(&p.oooCompactionDeferred, 1)
			}
		},
	}
}

func mirrorPolicyMatches(w *whisper.Whisper, metadata store.Metadata) bool {
	retentions := w.Retentions()
	if len(retentions) != len(metadata.Retentions) || store.AggregationMethod(w.AggregationMethod()) != metadata.AggregationMethod || w.XFilesFactor() != metadata.XFilesFactor {
		return false
	}
	for i, r := range retentions {
		if r.SecondsPerPoint() != metadata.Retentions[i].Step || r.NumberOfPoints() != metadata.Retentions[i].Count {
			return false
		}
	}
	return true
}

var errNoSchema = errors.New("no storage schema or aggregation for metric")

func (p *Whisper) createSharedMetric(ctx context.Context, metric string) bool {
	err := p.createShared(ctx, metric)
	if errors.Is(err, store.ErrNameTooLong) {
		p.dropNameTooLong(metric)
	}
	return err == nil
}

// createShared creates metric in the store from its schema and aggregation,
// logging every failure except a refused name.
func (p *Whisper) createShared(ctx context.Context, metric string) error {
	if err := store.ValidateMetricName(metric); err != nil {
		return err
	}
	schema, ok := p.schemas.Match(metric)
	if !ok {
		p.logger.Error("no storage schema defined for metric", zap.String("metric", metric))
		return errNoSchema
	}
	aggr := p.aggregation.Match(metric)
	if aggr == nil {
		p.logger.Error("no storage aggregation defined for metric", zap.String("metric", metric))
		return errNoSchema
	}
	retentions := make([]store.Retention, len(schema.Retentions))
	for i, retention := range schema.Retentions {
		retentions[i] = store.Retention{Step: retention.SecondsPerPoint(), Count: retention.NumberOfPoints()}
	}
	config := store.MetricConfig{
		Name: metric, Retentions: retentions,
		AggregationMethod: store.AggregationMethod(aggr.aggregationMethod), XFilesFactor: float32(aggr.xFilesFactor),
	}
	_, err := p.metricStore.Create(ctx, config)
	if err == nil && !p.mirrorFiles {
		atomic.AddUint32(&p.created, 1)
		if p.tagsEnabled && p.taggedFn != nil && strings.Contains(metric, ";") {
			p.taggedFn(metric, true)
		}
	} else if errors.Is(err, store.ErrExists) {
		err = p.discardStaleStage(ctx, metric, config)
	}
	if err != nil && !errors.Is(err, store.ErrNameTooLong) {
		p.logger.Error("prepare shared metric", zap.String("metric", metric), zap.Error(err))
	}
	return err
}

// createAfterPop creates metric for points the caller already popped and
// reports whether to retry the write. Otherwise the points are dropped (name
// refused) or requeued.
func (p *Whisper) createAfterPop(ctx context.Context, metric string, values *points.Points) bool {
	err := p.createShared(ctx, metric)
	switch {
	case err == nil:
		return true
	case errors.Is(err, store.ErrNameTooLong):
		if p.confirm != nil {
			p.confirm(values)
		}
		p.countNameTooLong(metric, len(values.Data))
	case p.requeue != nil:
		p.requeue(values)
	}
	return false
}

// dropNameTooLong discards the cached points of a metric whose name the store
// refuses, as a Whisper backend does when it cannot create the file. Left in
// the cache they would be retried on every writeout forever.
func (p *Whisper) dropNameTooLong(metric string) {
	n := 0
	if values, ok := p.popConfirm(metric); ok {
		n = len(values.Data)
	}
	p.countNameTooLong(metric, n)
}

func (p *Whisper) countNameTooLong(metric string, points int) {
	atomic.AddUint32(&p.nameTooLong, uint32(points))
	now := time.Now().Unix()
	if last := p.nameTooLongLogged.Load(); now-last >= 60 && p.nameTooLongLogged.CompareAndSwap(last, now) {
		p.logger.Warn("metric name too long for the store; points dropped", zap.Int("length", len(metric)), zap.String("prefix", metric[:min(len(metric), 200)]))
	}
}

// discardStaleStage handles Create finding the name taken. Usually another
// writer created it first. After a completed migration the write route no
// longer checks the original, so the taker can also be the staged import of an
// original removed before publication; discard that and create afresh, as the
// route did when it found the original missing.
func (p *Whisper) discardStaleStage(ctx context.Context, metric string, config store.MetricConfig) error {
	if p.incrementalStore == nil || !p.incrementalComplete.Load() {
		return nil
	}
	if err := p.incrementalStore.DiscardStagedFile(ctx, metric); errors.Is(err, store.ErrConflict) {
		return nil
	} else if err != nil {
		return err
	}
	if _, err := p.metricStore.Create(ctx, config); err != nil && !errors.Is(err, store.ErrExists) {
		return err
	}
	return nil
}
