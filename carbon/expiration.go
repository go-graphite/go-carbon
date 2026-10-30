package carbon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const expirationPageSize = 1024
const expirationDeleteBatchSize = 512

type expirationStore interface {
	ListPage(context.Context, string, string, int) ([]store.Metadata, error)
	InitializeActivity(context.Context, string, store.Metadata) error
	DeleteIfUnchanged(context.Context, string, store.Metadata) error
	AsyncFlush() (<-chan struct{}, error)
}

type expirationStats struct {
	examined, initialized, deleted, conflicts, pending, errors atomic.Uint64
	duration, lastSuccess                                      atomic.Int64
	wouldExpire, wouldBytes, guarded, limited                  atomic.Uint64
	mu                                                         sync.Mutex
	rules                                                      map[string]*expirationRuleStats
}

func (s *expirationStats) Stat(send helper.StatCallback) {
	send("examined", float64(s.examined.Swap(0)))
	send("initialized", float64(s.initialized.Swap(0)))
	send("deleted", float64(s.deleted.Swap(0)))
	send("conflicts", float64(s.conflicts.Swap(0)))
	send("pending", float64(s.pending.Swap(0)))
	send("errors", float64(s.errors.Swap(0)))
	send("sweepDurationNs", float64(s.duration.Load()))
	send("lastSuccessTime", float64(s.lastSuccess.Load()))
	send("wouldExpire", float64(s.wouldExpire.Swap(0)))
	send("wouldExpireLogicalBytes", float64(s.wouldBytes.Swap(0)))
	send("guarded", float64(s.guarded.Swap(0)))
	send("limited", float64(s.limited.Swap(0)))
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, rule := range s.rules {
		rule.Stat(func(key string, value float64) { send("rules."+name+"."+key, value) })
	}
}

// An expirer owns one immutable policy. Reload and shutdown join its worker
// before replacing the policy or closing the store and catalog refresher.
type metricExpirer struct {
	db                                           expirationStore
	rules                                        persister.WhisperExpirationRules
	expiration                                   time.Duration
	interval                                     time.Duration
	rate                                         int
	now                                          func() time.Time
	pending                                      func(string) bool
	notify                                       func()
	stats                                        *expirationStats
	cancel                                       context.CancelFunc
	done                                         chan struct{}
	restored                                     <-chan struct{}
	dryRun                                       bool
	deleteRate, deleteWorkers, maxDeletes, maxL0 int
	maxDebt                                      uint64
	ruleStats                                    []*expirationRuleStats
}

func validateExpirationConfig(cfg *Config) error {
	if cfg.Whisper.StoreExpiration == nil || cfg.Whisper.StoreExpiration.Value() < 0 {
		return errors.New("whisper.store-expiration must be a non-negative duration")
	}
	if cfg.Whisper.StoreExpirationCheckInterval == nil || cfg.Whisper.StoreExpirationCheckInterval.Value() <= 0 {
		return errors.New("whisper.store-expiration-check-interval must be a positive duration")
	}
	if cfg.Whisper.StoreExpirationScanRate <= 0 {
		return errors.New("whisper.store-expiration-scan-rate must be positive")
	}
	if cfg.Whisper.StoreExpirationDeleteWorkers <= 0 {
		return errors.New("whisper.store-expiration-delete-workers must be positive")
	}
	if cfg.Whisper.StoreExpirationDeleteRate <= 0 || cfg.Whisper.StoreExpirationMaxDeletes <= 0 || cfg.Whisper.StoreExpirationMaxL0 <= 0 || cfg.Whisper.StoreExpirationMaxDebt <= 0 {
		return errors.New("store expiration requires positive delete-rate, max-deletes, max-l0-sublevels and max-compaction-debt")
	}
	return nil
}

func loadExpirationConfig(cfg *Config) error {
	if cfg.Whisper.StorageBackend != "pebble-chunk" || cfg.Whisper.StoreExpirationFilename == "" {
		return nil
	}
	rules, err := persister.ReadWhisperExpiration(cfg.Whisper.StoreExpirationFilename)
	if err != nil {
		return fmt.Errorf("read expiration policy %s: %w", cfg.Whisper.StoreExpirationFilename, err)
	}
	cfg.Whisper.ExpirationRules = rules
	return nil
}

func (app *App) startExpiration() {
	cfg := app.Config.Whisper
	if app.MetricStore == nil || (cfg.StoreExpiration.Value() == 0 && !cfg.ExpirationRules.Enabled()) {
		return
	}
	core := app.Cache
	r := &metricExpirer{
		db: app.MetricStore, rules: cfg.ExpirationRules,
		expiration: cfg.StoreExpiration.Value(), interval: cfg.StoreExpirationCheckInterval.Value(),
		rate: cfg.StoreExpirationScanRate, now: time.Now,
		dryRun: cfg.StoreExpirationDryRun, deleteRate: cfg.StoreExpirationDeleteRate, deleteWorkers: cfg.StoreExpirationDeleteWorkers,
		maxDeletes: cfg.StoreExpirationMaxDeletes, maxL0: cfg.StoreExpirationMaxL0, maxDebt: uint64(cfg.StoreExpirationMaxDebt),
		pending:  core.Has,
		stats:    app.expirationStats,
		restored: app.storeRestoreDone,
	}
	if app.metricStoreIndex != nil {
		r.notify = app.metricStoreIndex.notify
	}
	r.start()
	app.expirer = r
}

func (app *App) stopExpiration() {
	if app.expirer != nil {
		app.expirer.close()
		app.expirer = nil
	}
}

func (r *metricExpirer) start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.done = cancel, make(chan struct{})
	go func() {
		defer close(r.done)
		// Dumps contain pending points that are invisible to the cache check
		// until replay loads them. Keep reads and receivers available meanwhile.
		if r.restored != nil {
			select {
			case <-ctx.Done():
				return
			case <-r.restored:
			}
		}
		for {
			if ctx.Err() != nil {
				return
			}
			if err := r.sweep(ctx); err != nil && !errors.Is(err, context.Canceled) {
				zapwriter.Logger("storage").Error("expire shared metrics", zap.Error(err))
			}
			timer := time.NewTimer(r.interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (r *metricExpirer) close() {
	r.cancel()
	<-r.done
}

func (r *metricExpirer) sweep(ctx context.Context) (err error) {
	started := time.Now()
	var examined, deleted uint64
	var reports []expirationReport
	defer func() {
		// Flush tombstones even after a partial sweep so idle stores can
		// reclaim data through compaction. Do not wait for the flush here.
		// A flush failure is counted and logged on its own so a cancelled
		// sweep cannot hide it; it is never folded into the sweep error.
		var flushErr error
		if deleted > 0 {
			if _, flushErr = r.db.AsyncFlush(); flushErr != nil {
				r.stats.errors.Add(1)
				zapwriter.Logger("storage").Error("flush expired shared metrics", zap.Error(flushErr))
			}
		}
		r.stats.duration.Store(int64(time.Since(started)))
		for i, stats := range r.ruleStats {
			stats.duration.Store(int64(time.Since(started)))
			if err == nil {
				stats.end.Store(r.now().Unix())
			}
			name := "default"
			if i > 0 {
				name = r.rules[i-1].Name
			}
			zapwriter.Logger("storage").Info("shared metric expiration rule", zap.String("rule", name), zap.Bool("dryRun", r.dryRun), zap.Uint64("scanned", reports[i].scanned), zap.Uint64("expired", reports[i].expired), zap.Uint64("kept", reports[i].kept), zap.Uint64("wouldExpire", reports[i].would), zap.Uint64("wouldExpireLogicalBytes", reports[i].bytes), zap.Strings("samples", reports[i].samples))
		}
		if err == nil && flushErr == nil {
			r.stats.lastSuccess.Store(r.now().Unix())
		} else if err != nil && !errors.Is(err, context.Canceled) {
			r.stats.errors.Add(1)
		}
		zapwriter.Logger("storage").Info("shared metric expiration sweep",
			zap.Uint64("examined", examined), zap.Uint64("deleted", deleted),
			zap.Duration("runtime", time.Since(started)), zap.Error(err))
	}()
	r.setupRuleStats()
	reports = make([]expirationReport, len(r.ruleStats))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var deletesPaused atomic.Bool
	var reserved atomic.Int64
	var deleteCount atomic.Uint64
	var work, workers sync.WaitGroup
	var failure error
	var failureOnce sync.Once
	var notifyMu sync.Mutex
	workerCount := max(r.deleteWorkers, 1)
	if r.dryRun {
		workerCount = 0
	}
	workerReports := make([][]expirationReport, workerCount)
	jobs := make(chan expirationBatch)
	for i := range workerCount {
		workerReports[i] = make([]expirationReport, len(reports))
		workers.Go(func() {
			for batch := range jobs {
				func() {
					defer work.Done()
					// Reservations include queued candidates and successful deletes.
					// Conflicts and pending writes return their slots to the scan.
					defer func() { reserved.Add(-int64(len(batch.metrics))) }()
					if ctx.Err() != nil {
						return
					}
					if !deletesPaused.Load() && r.expirationPressure() {
						deletesPaused.Store(true)
					}
					if deletesPaused.Load() {
						r.stats.guarded.Add(uint64(len(batch.metrics)))
						return
					}
					n := 0
					for j, m := range batch.metrics {
						if r.pending != nil && r.pending(m.Name) {
							r.stats.pending.Add(1)
							r.ruleStats[batch.indexes[j]].kept.Add(1)
							workerReports[i][batch.indexes[j]].kept++
							continue
						}
						batch.metrics[n], batch.indexes[n] = m, batch.indexes[j]
						n++
					}
					outcomes, err := r.deleteBatch(ctx, batch.metrics[:n])
					notified := false
					for j, outcome := range outcomes {
						idx := batch.indexes[j]
						if errors.Is(outcome, store.ErrConflict) || errors.Is(outcome, store.ErrNotFound) {
							r.stats.conflicts.Add(1)
							r.ruleStats[idx].kept.Add(1)
							workerReports[i][idx].kept++
						} else if outcome != nil {
							err = errors.Join(err, outcome)
						} else {
							deleteCount.Add(1)
							reserved.Add(1)
							r.stats.deleted.Add(1)
							r.ruleStats[idx].expired.Add(1)
							workerReports[i][idx].expired++
							notified = true
						}
					}
					if notified && r.notify != nil {
						notifyMu.Lock()
						r.notify()
						notifyMu.Unlock()
					}
					if err != nil {
						failureOnce.Do(func() { failure = err; cancel() })
					}
				}()
			}
		})
	}
	// Join before flushing, publishing rule reports, or closing the store.
	defer func() {
		close(jobs)
		workers.Wait()
		deleted = deleteCount.Load()
		for _, local := range workerReports {
			for i := range reports {
				reports[i].kept += local[i].kept
				reports[i].expired += local[i].expired
			}
		}
		if failure != nil {
			err = failure
		}
	}()
	scanBudget := expirationBudget{rate: r.rate}
	deleteBudget := expirationBudget{rate: r.deleteRate}
	now := r.now()
	return store.EachPage(ctx, r.db.ListPage, "", expirationPageSize, func(page []store.Metadata) error {
		if err := scanBudget.wait(ctx, len(page)); err != nil {
			return err
		}
		batch := expirationBatch{}
		flush := func() error {
			if len(batch.metrics) == 0 {
				return nil
			}
			if err := deleteBudget.wait(ctx, len(batch.metrics)); err != nil {
				return err
			}
			work.Add(1)
			select {
			case jobs <- batch:
				batch = expirationBatch{}
				return nil
			case <-ctx.Done():
				work.Done()
				return ctx.Err()
			}
		}
		for _, m := range page {
			if err := ctx.Err(); err != nil {
				return err
			}
			examined++
			r.stats.examined.Add(1)
			idx := r.rules.MatchIndex(m.Name, now) + 1
			stats := r.ruleStats[idx]
			stats.scanned.Add(1)
			reports[idx].scanned++
			ttl := r.expiration
			if idx > 0 {
				ttl = r.rules[idx-1].TTL()
			}
			if m.LastUpdate.IsZero() {
				stats.kept.Add(1)
				reports[idx].kept++
				if !r.dryRun {
					if _, err := r.expire(ctx, m); err != nil {
						return err
					}
				}
				continue
			}
			if ttl == 0 || now.Sub(m.LastUpdate) < ttl {
				stats.kept.Add(1)
				reports[idx].kept++
				continue
			}
			if r.pending != nil && r.pending(m.Name) {
				r.stats.pending.Add(1)
				stats.kept.Add(1)
				reports[idx].kept++
				continue
			}
			size := uint64(16 + 12*len(m.Retentions))
			for _, retention := range m.Retentions {
				size += uint64(retention.Count) * 12
			}
			stats.would.Add(1)
			stats.bytes.Add(size)
			reports[idx].would++
			if len(reports[idx].samples) < 10 {
				reports[idx].samples = append(reports[idx].samples, m.Name)
			}
			reports[idx].bytes += size
			r.stats.wouldExpire.Add(1)
			r.stats.wouldBytes.Add(size)
			if r.dryRun {
				continue
			}
			if deletesPaused.Load() {
				r.stats.guarded.Add(1)
				continue
			}
			if r.maxDeletes > 0 && reserved.Load() >= int64(r.maxDeletes) {
				if err := flush(); err != nil {
					return err
				}
				work.Wait()
				if err := ctx.Err(); err != nil {
					return err
				}
				if reserved.Load() >= int64(r.maxDeletes) {
					r.stats.limited.Add(1)
					continue
				}
			}
			reserved.Add(1)
			if batch.metrics == nil {
				batch.metrics = make([]store.Metadata, 0, expirationDeleteBatchSize)
				batch.indexes = make([]int, 0, expirationDeleteBatchSize)
			}
			batch.metrics = append(batch.metrics, m)
			batch.indexes = append(batch.indexes, idx)
			if len(batch.metrics) == expirationDeleteBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return flush()
	})
}

type expirationBatch struct {
	metrics []store.Metadata
	indexes []int
}

func (r *metricExpirer) expirationPressure() bool {
	pressure, ok := r.db.(interface{ ExpirationPressure() (int64, uint64, bool) })
	if !ok {
		return false
	}
	l0, debt, stalled := pressure.ExpirationPressure()
	return stalled || r.maxL0 > 0 && l0 >= int64(r.maxL0) || r.maxDebt > 0 && debt >= r.maxDebt
}

func (r *metricExpirer) deleteBatch(ctx context.Context, candidates []store.Metadata) ([]error, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	if batch, ok := r.db.(interface {
		DeleteBatchIfUnchanged(context.Context, []store.Metadata) ([]error, error)
	}); ok {
		return batch.DeleteBatchIfUnchanged(ctx, candidates)
	}
	outcomes := make([]error, 0, len(candidates))
	for _, m := range candidates {
		if err := ctx.Err(); err != nil {
			return outcomes, err
		}
		outcome := r.db.DeleteIfUnchanged(ctx, m.Name, m)
		outcomes = append(outcomes, outcome)
		if outcome != nil && !errors.Is(outcome, store.ErrConflict) && !errors.Is(outcome, store.ErrNotFound) {
			return outcomes, nil
		}
	}
	return outcomes, nil
}

// Page-sized reservations amortize timer wakeups. Processing time pays down
// the next reservation; an idle worker can never accumulate unbounded credit.
type expirationBudget struct {
	rate int
	next time.Time
}

func (b *expirationBudget) wait(ctx context.Context, count int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.rate <= 0 || count == 0 {
		return nil
	}
	now := time.Now()
	cost := time.Duration(float64(time.Second) * float64(count) / float64(b.rate))
	if b.next.IsZero() {
		b.next = now
	} else if b.next.Before(now.Add(-cost)) {
		b.next = now.Add(-cost)
	}
	b.next = b.next.Add(cost)
	if !b.next.After(now) {
		return nil
	}
	timer := time.NewTimer(time.Until(b.next))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type expirationReport struct {
	scanned, expired, kept, would, bytes uint64
	samples                              []string
}

type expirationRuleStats struct {
	scanned, expired, kept, would, bytes atomic.Uint64
	duration, end                        atomic.Int64
}

func (s *expirationRuleStats) Stat(send helper.StatCallback) {
	send("scanned", float64(s.scanned.Swap(0)))
	send("expired", float64(s.expired.Swap(0)))
	send("kept", float64(s.kept.Swap(0)))
	send("wouldExpire", float64(s.would.Swap(0)))
	send("wouldExpireLogicalBytes", float64(s.bytes.Swap(0)))
	send("sweepDurationNs", float64(s.duration.Load()))
	send("lastSweepEnd", float64(s.end.Load()))
}
func (r *metricExpirer) setupRuleStats() {
	r.stats.mu.Lock()
	defer r.stats.mu.Unlock()
	if r.stats.rules == nil {
		r.stats.rules = make(map[string]*expirationRuleStats)
	}
	r.ruleStats = make([]*expirationRuleStats, len(r.rules)+1)
	for i := range r.ruleStats {
		name := "default"
		if i > 0 {
			name = fmt.Sprintf("%d_%s", i, strings.Map(func(c rune) rune {
				if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' {
					return c
				}
				return '_'
			}, r.rules[i-1].Name))
		}
		stats := r.stats.rules[name]
		if stats == nil {
			stats = &expirationRuleStats{}
			r.stats.rules[name] = stats
		}
		r.ruleStats[i] = stats
	}
}

func (r *metricExpirer) expire(ctx context.Context, m store.Metadata) (bool, error) {
	if m.LastUpdate.IsZero() {
		if r.dryRun {
			return false, nil
		}
		err := r.db.InitializeActivity(ctx, m.Name, m)
		if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
			r.stats.conflicts.Add(1)
			return false, nil
		}
		if err == nil {
			r.stats.initialized.Add(1)
		}
		return false, err
	}
	ttl := r.expiration
	if idx := r.rules.MatchIndex(m.Name, r.now()); idx >= 0 {
		ttl = r.rules[idx].TTL()
	}
	if ttl == 0 || r.now().Sub(m.LastUpdate) < ttl {
		return false, nil
	}
	if r.pending != nil && r.pending(m.Name) {
		r.stats.pending.Add(1)
		return false, nil
	}
	if r.dryRun {
		return false, nil
	}
	// New arrivals after the cache check may race this deletion. Store revision
	// validation protects committed writes; the persister requeues a missing
	// metric and recreates it if deletion wins before the new write.
	err := r.db.DeleteIfUnchanged(ctx, m.Name, m)
	if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
		r.stats.conflicts.Add(1)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	r.stats.deleted.Add(1)
	return true, nil
}
