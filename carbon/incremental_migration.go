package carbon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const incrementalReceiptName = "INCREMENTAL-STORE-MIGRATION.json"

// migrationWalkWorkers read the original tree's directories. Every restart
// walks the whole tree again, and with one reader the conversion workers sat
// idle for most of that re-walk; directory reads on store hosts stop scaling
// at about 16 to 24 threads.
const migrationWalkWorkers = 16

type incrementalStoreMigration struct {
	cancel                             context.CancelFunc
	done                               chan struct{}
	started                            time.Time
	maximum                            int
	indexReady                         func() bool
	priority                           *migrationPriority
	budget                             *migrationBudget
	metrics, points, resumed, unlinked atomic.Uint64
	archived, skipped                  atomic.Uint64
	active, working, largest           atomic.Int64
	spaceWaiting                       atomic.Int64
	sourceComplete                     atomic.Bool
	diskMu                             sync.Mutex
	diskReserved                       uint64
	receiptMu                          sync.Mutex
	mu                                 sync.Mutex
	phase, failure                     string
	skippedMetrics                     map[string]string
	lastReceipt                        time.Time
}

func (m *incrementalStoreMigration) receipt() map[string]interface{} {
	m.mu.Lock()
	phase, failure := m.phase, m.failure
	skippedMetrics := maps.Clone(m.skippedMetrics)
	m.mu.Unlock()
	workers, active, generation := m.maximum, int(m.active.Load()), uint64(0)
	if m.budget != nil {
		workers, active, generation = m.budget.status()
	}
	return map[string]interface{}{"mode": "incremental", "phase": phase, "started": m.started,
		"metrics": m.metrics.Load(), "points": m.points.Load(), "audited": uint64(0),
		"resumed_cleanup": m.resumed.Load(), "unlinked_bytes": m.unlinked.Load(),
		"archived_bytes": m.archived.Load(), "skipped": m.skipped.Load(),
		"skipped_metrics": skippedMetrics,
		"source_complete": m.sourceComplete.Load(),
		"workers":         workers, "in_flight": active, "control_generation": generation,
		"working_bytes_reserved": m.working.Load(), "largest_working_estimate": m.largest.Load(),
		"waiting_for_disk_space": m.spaceWaiting.Load() > 0, "error": failure,
		"counter_scope": "this process; durable ownership and removed sources retain completed work across restarts"}
}

func (m *incrementalStoreMigration) progress(directory string, force bool) error {
	m.receiptMu.Lock()
	defer m.receiptMu.Unlock()
	m.mu.Lock()
	if !force && time.Since(m.lastReceipt) < 10*time.Second {
		m.mu.Unlock()
		return nil
	}
	m.lastReceipt = time.Now()
	m.mu.Unlock()
	return writeMigrationStatus(directory, incrementalReceiptName, m.receipt())
}

func completedIncrementalMigration(conf *Config) bool {
	if !conf.Whisper.StoreIncrementalMigration {
		return false
	}
	data, err := os.ReadFile(filepath.Join(sharedStorePath(conf), incrementalReceiptName))
	if err != nil {
		return false
	}
	var receipt struct {
		Mode, Phase, Error string
		Skipped            *uint64
		InFlight           *int `json:"in_flight"`
		SourceComplete     bool `json:"source_complete"`
	}
	if json.Unmarshal(data, &receipt) != nil || receipt.Mode != "incremental" || receipt.Skipped == nil || *receipt.Skipped != 0 {
		return false
	}
	// Preserve terminal source completion through a restart that is resuming
	// cleanup. Its runtime phase/error can change without making files unowned.
	return receipt.SourceComplete || receipt.Phase == "shared" && receipt.Error == "" && receipt.InFlight != nil && *receipt.InFlight == 0
}

func (app *App) startIncrementalStoreMigration() {
	if !app.Config.Whisper.StoreIncrementalMigration || app.incrementalMigration.Load() != nil || app.Persister == nil || app.MetricStore == nil {
		return
	}
	conf, p, db := app.Config, app.Persister, app.MetricStore
	maximum := conf.Whisper.StoreMigrationWorkers
	if maximum == 0 {
		maximum = 4
	}
	ctx, cancel := context.WithCancel(context.Background())
	burst := migrationPriorityBurst
	if conf.Whisper.StoreMigrationHintBurst != nil {
		burst = *conf.Whisper.StoreMigrationHintBurst
	}
	m := &incrementalStoreMigration{cancel: cancel, done: make(chan struct{}), started: time.Now(), maximum: maximum, phase: "files", priority: newMigrationPriorityBurst(burst)}
	p.SetIncrementalMigrationPriority(m.priority.offer)
	if path := conf.Whisper.StoreMigrationControlFile; path != "" {
		m.budget = newMigrationBudget(maximum)
		m.budget.refresh(path, time.Now())
	}
	if cs := app.Carbonserver; cs != nil {
		m.indexReady = func() bool { return cs.CurrentFileIndex() != nil }
	}
	app.incrementalMigration.Store(m)
	db.SetCompactionPacingConversionSource(func() (int64, uint64) {
		return m.active.Load(), m.metrics.Load()
	})
	go func() {
		defer close(m.done)
		defer cancel()
		defer p.SetIncrementalMigrationPriority(nil)
		err := m.run(ctx, p, db, conf)
		m.mu.Lock()
		switch {
		case err == nil:
			m.phase = "shared"
			if m.skipped.Load() == 0 {
				m.sourceComplete.Store(true)
				p.SetIncrementalMigrationComplete()
				if cs := app.Carbonserver; cs != nil {
					cs.SetMetricStoreFileIndex(false)
					cs.RequestFileScan()
				}
			}
		case errors.Is(err, context.Canceled):
			m.phase = "cancelled"
		default:
			m.phase = "failed"
			m.failure = err.Error()
		}
		m.mu.Unlock()
		if statusErr := m.progress(sharedStorePath(conf), true); statusErr != nil {
			zapwriter.Logger("storage").Error("write incremental migration receipt", zap.Error(statusErr))
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			zapwriter.Logger("storage").Error("incremental storage migration stopped; owned data retained", zap.Error(err))
		}
	}()
}

func (app *App) stopIncrementalStoreMigration() {
	if m := app.incrementalMigration.Load(); m != nil {
		m.cancel()
		<-m.done
		app.incrementalMigration.CompareAndSwap(m, nil)
	}
}

// pendingCleanupConcurrency resumes published owners after a restart. Each
// cleanup mostly waits on a directory sync and a catalog commit, which batch
// across callers; at 256 a restart that left a million owners pending spent
// ~50 minutes at ~310/s before converting anything new. Conversion I/O slots
// (2048) bound it anyway.
const pendingCleanupConcurrency = 2048

// maxSkippedConversions stops a run whose rejected sources look systemic
// rather than a few unusual files.
const maxSkippedConversions = 1000

// Bounds conversion threads well below Go's 10,000-thread limit.
const defaultConversionIOSlots = 2048

func (m *incrementalStoreMigration) converted(result persister.FileConversion) {
	m.metrics.Add(1)
	m.points.Add(uint64(result.Points))
	m.unlinked.Add(uint64(result.UnlinkedBytes))
	m.archived.Add(uint64(result.ArchivedBytes))
	if result.PreviouslyOwned {
		m.resumed.Add(1)
	}
}

func (m *incrementalStoreMigration) run(ctx context.Context, p *persister.Whisper, db *store.Store, conf *Config) error {
	phasecpu.Set(phasecpu.Backfill)
	defer phasecpu.Set(phasecpu.Other)
	// The resource waiter must observe worker failures as well as shutdown.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, workCtx := errgroup.WithContext(ctx)
	if m.budget != nil {
		done := make(chan struct{})
		go func() { defer close(done); m.budget.run(workCtx, conf.Whisper.StoreMigrationControlFile) }()
		defer func() { cancel(); <-done }()
	}
	completed := completedIncrementalMigration(conf)
	m.sourceComplete.Store(completed)
	directory := sharedStorePath(conf)
	if err := m.progress(directory, true); err != nil {
		return err
	}
	// Reclaim already durable owners even if new admission is paused or disk
	// headroom is exhausted. No full catalog scan or history re-verification.
	for {
		// A restart during high-concurrency conversion leaves up to a million
		// published owners pending; stream them to the workers in one pass.
		resume, resumeCtx := errgroup.WithContext(ctx)
		resume.SetLimit(pendingCleanupConcurrency)
		found := 0
		visitErr := db.VisitPendingFileCleanup(resumeCtx, func(name string) error {
			found++
			resume.Go(func() error {
				m.active.Add(1)
				defer m.active.Add(-1)
				result, err := p.ConvertFile(resumeCtx, name)
				if err != nil {
					return err
				}
				m.converted(result)
				return nil
			})
			return nil
		})
		if err := errors.Join(resume.Wait(), visitErr); err != nil {
			return err
		}
		if found == 0 {
			break
		}
		if err := m.progress(directory, false); err != nil {
			return err
		}
	}
	if completed {
		return nil
	}

	if m.indexReady != nil {
		m.mu.Lock()
		m.phase = "index"
		m.mu.Unlock()
		if err := m.progress(directory, true); err != nil {
			return err
		}
		for !m.indexReady() {
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		m.mu.Lock()
		m.phase = "files"
		m.mu.Unlock()
	}
	jobs := make(chan string, m.maximum*2)
	bytes := semaphore.NewWeighted(conf.Whisper.StoreMigrationBytesInFlight)
	priority := m.priority
	if priority == nil {
		priority = newMigrationPriority()
	}
	admit, release := func() bool { return workCtx.Err() == nil }, func() {}
	if m.budget != nil {
		admit, release = m.budget.acquire, m.budget.release
	}
	for range m.maximum {
		group.Go(func() error {
			return priority.runWorker(workCtx, jobs, admit, release, func(name string) error {
				return m.convertAdmitted(workCtx, p, conf, bytes, name)
			})
		})
	}
	walkErr := whisperio.WalkMetricsParallel(workCtx, conf.Whisper.DataDir, true, migrationWalkWorkers, func(entry whisperio.MetricPath) error {
		canonical := filepath.FromSlash(strings.ReplaceAll(entry.Metric, ".", "/") + ".wsp")
		if canonical != entry.Path {
			return errors.New("incremental migration requires canonical untagged source paths")
		}
		select {
		case jobs <- entry.Metric:
			return nil
		case <-workCtx.Done():
			return workCtx.Err()
		}
	})
	if walkErr != nil {
		cancel()
	}
	close(jobs)
	if err := group.Wait(); err != nil {
		return err
	}
	if err := errors.Join(walkErr, ctx.Err()); err != nil {
		return err
	}
	if conf.Whisper.StoreOOODirectWrites {
		return m.checkNoSidecars(ctx, db, directory)
	}
	return nil
}

func (m *incrementalStoreMigration) checkNoSidecars(ctx context.Context, db *store.Store, directory string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !db.Sidecars().Ready {
		m.mu.Lock()
		m.phase = "sidecars"
		m.mu.Unlock()
		if err := m.progress(directory, true); err != nil {
			return err
		}
		logger := zapwriter.Logger("storage")
		started := time.Now()
		progress := func() {
			logger.Info("waiting for sidecar inventory scan before completing incremental migration",
				zap.Duration("wait", time.Since(started)), zap.Any("sidecars", db.Sidecars()))
		}
		progress()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-db.SidecarsReady():
				logger.Info("sidecar inventory ready; checking incremental migration completion",
					zap.Duration("wait", time.Since(started)), zap.Any("sidecars", db.Sidecars()))
				return db.CheckNoSidecars()
			case <-ticker.C:
				progress()
			}
		}
	}
	return db.CheckNoSidecars()
}

func (m *incrementalStoreMigration) convertAdmitted(ctx context.Context, p *persister.Whisper, conf *Config, bytes *semaphore.Weighted, name string) error {
	working, err := p.ConversionWorkingBytes(ctx, name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return m.skip(ctx, name, err)
	}
	for previous := m.largest.Load(); working > previous && !m.largest.CompareAndSwap(previous, working); previous = m.largest.Load() {
	}
	// One oversized metric may run alone; it still must fit the free-space
	// reserve and is reported explicitly, never admitted beside another job.
	reservation := min(working, conf.Whisper.StoreMigrationBytesInFlight)
	if err := bytes.Acquire(ctx, reservation); err != nil {
		return err
	}
	defer bytes.Release(reservation)
	m.working.Add(reservation)
	defer m.working.Add(-reservation)
	releaseDisk, err := m.waitDisk(ctx, sharedStorePath(conf), uint64(conf.Whisper.StoreMigrationMinFreeBytes)+uint64(working), uint64(working))
	if err != nil {
		return err
	}
	defer releaseDisk()
	m.active.Add(1)
	defer m.active.Add(-1)
	result, err := p.ConvertFile(ctx, name)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, persister.ErrConversionInProgress) {
		return nil
	}
	if err != nil && !result.PreviouslyOwned && result.Metadata.Name == "" {
		return m.skip(ctx, name, err)
	}
	if err != nil {
		return fmt.Errorf("convert %s: %w", name, err)
	}
	m.converted(result)
	return m.progress(sharedStorePath(conf), false)
}

// skip handles a conversion that failed before publishing: the original stays
// authoritative, so one source the shared format rejects must not stop every
// other metric. Cancellation and a systemic run of rejections still stop it.
func (m *incrementalStoreMigration) skip(ctx context.Context, name string, err error) error {
	if ctx.Err() != nil {
		return err
	}
	m.mu.Lock()
	if m.skippedMetrics == nil {
		m.skippedMetrics = make(map[string]string)
	}
	m.skippedMetrics[name] = err.Error()
	m.mu.Unlock()
	skipped := m.skipped.Add(1)
	zapwriter.Logger("storage").Warn("conversion skipped; original kept", zap.String("metric", name), zap.Uint64("skipped", skipped), zap.Error(err))
	if skipped > maxSkippedConversions {
		return fmt.Errorf("convert %s: %w", name, err)
	}
	return nil
}

// Reserve headroom atomically across workers: independent free-space checks can
// all pass and then overcommit the same bytes. Only admitted work reserves disk;
// waiters do not reserve it, so a fitting first metric can always make progress.
func (m *incrementalStoreMigration) waitDisk(ctx context.Context, directory string, required, reservation uint64) (func(), error) {
	waiting := false
	defer func() {
		if waiting {
			m.spaceWaiting.Add(-1)
		}
	}()
	for {
		m.diskMu.Lock()
		var info syscall.Statfs_t
		if err := syscall.Statfs(directory, &info); err != nil {
			m.diskMu.Unlock()
			return nil, err
		}
		var available uint64
		if info.Bavail > 0 {
			available = uint64(info.Bavail) * uint64(info.Bsize)
		}
		if required <= available && m.diskReserved <= available-required {
			m.diskReserved += reservation
			m.diskMu.Unlock()
			return func() {
				m.diskMu.Lock()
				m.diskReserved -= reservation
				m.diskMu.Unlock()
			}, nil
		}
		m.diskMu.Unlock()
		if !waiting {
			m.spaceWaiting.Add(1)
			waiting = true
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
