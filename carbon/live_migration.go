package carbon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const liveMigrationReceiptName = "LIVE-STORE-MIGRATION.json"

type liveStoreMigration struct {
	cancel                   context.CancelFunc
	done                     chan struct{}
	started                  time.Time
	metrics, points, audited atomic.Uint64
	baselineSkipped          *atomic.Uint64
	workerMaximum            atomic.Int64
	budget                   atomic.Pointer[migrationBudget]
}

type liveMigrationReceipt struct {
	Phase             string    `json:"phase"`
	PID               int       `json:"pid"`
	StartTicks        string    `json:"start_ticks"`
	Started           time.Time `json:"started"`
	Updated           time.Time `json:"updated"`
	Metrics           uint64    `json:"metrics"`
	Points            uint64    `json:"points"`
	Audited           uint64    `json:"audited"`
	BaselineSkipped   uint64    `json:"baseline_skipped"`
	Workers           int       `json:"workers"`
	InFlight          int       `json:"in_flight"`
	ControlGeneration uint64    `json:"control_generation,omitempty"`
	Error             string    `json:"error,omitempty"`
}

func checkStoreStartup(conf *Config) error {
	dir := sharedStorePath(conf)
	if !conf.Whisper.StoreLiveMigration {
		return checkStoreConversion(dir)
	}
	interrupted := false
	for _, marker := range []string{"CONVERSION-IN-PROGRESS", "NEEDS-CATCHUP", "MIRROR-RECONCILE-REQUIRED"} {
		info, err := os.Lstat(filepath.Join(dir, marker))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if marker == "CONVERSION-IN-PROGRESS" || !info.Mode().IsRegular() {
			return fmt.Errorf("live storage migration cannot open: %s", marker)
		}
		interrupted = interrupted || marker == "MIRROR-RECONCILE-REQUIRED"
	}
	receipt, err := validatedLiveBaseline(conf)
	if err != nil {
		return err
	}
	if interrupted && conf.Whisper.StoreMigrationBaselineCopy != "" && receipt.BaselineMaxMetricID == 0 {
		return errors.New("interrupted baseline shortcut requires the original verified catalog ID ceiling")
	}
	return nil
}

func armStoreSession(conf *Config) error {
	dir := sharedStorePath(conf)
	err := armMirrorGuard(dir)
	if err != nil && conf.Whisper.StoreLiveMigration && errors.Is(err, os.ErrExist) {
		info, statErr := os.Lstat(filepath.Join(dir, "MIRROR-RECONCILE-REQUIRED"))
		if statErr == nil && info.Mode().IsRegular() {
			err = nil
		}
	}
	if err != nil {
		return err
	}
	if conf.Whisper.StoreLiveMigration {
		return writeLiveMigrationReceipt(dir, liveMigrationReceipt{Phase: "files", Started: time.Now()})
	}
	return nil
}

func writeLiveMigrationReceipt(dir string, receipt liveMigrationReceipt) error {
	receipt.PID = os.Getpid()
	receipt.Updated = time.Now()
	if raw, err := os.ReadFile("/proc/self/stat"); err == nil {
		if end := strings.LastIndexByte(string(raw), ')'); end >= 0 {
			if fields := strings.Fields(string(raw[end+1:])); len(fields) > 19 {
				receipt.StartTicks = fields[19]
			}
		}
	}
	return writeMigrationStatus(dir, liveMigrationReceiptName, receipt)
}

func writeMigrationStatus(dir, name string, receipt interface{}) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".live-store-migration-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(append(data, '\n'))
	if err = errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func (m *liveStoreMigration) receipt(phase string, err error) liveMigrationReceipt {
	r := liveMigrationReceipt{Phase: phase, Started: m.started, Metrics: m.metrics.Load(), Points: m.points.Load(), Audited: m.audited.Load()}
	r.Workers = int(m.workerMaximum.Load())
	if b := m.budget.Load(); b != nil {
		r.Workers, r.InFlight, r.ControlGeneration = b.status()
	}
	if m.baselineSkipped != nil {
		r.BaselineSkipped = m.baselineSkipped.Load()
	}
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

// startLiveStoreMigration runs under app.Lock after ordinary intake takeover.
// The worker owns captured component pointers until stopLiveStoreMigration joins
// it. Only this atomic lifecycle slot is accessed before taking app.Lock.
func (app *App) startLiveStoreMigration() {
	if app.Config.Whisper.StoreIncrementalMigration {
		app.startIncrementalStoreMigration()
		return
	}
	if !app.Config.Whisper.StoreLiveMigration || app.sharedStoreReads.Load() || app.liveStoreMigration.Load() != nil || app.Persister == nil || app.MetricStore == nil || app.Carbonserver == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &liveStoreMigration{cancel: cancel, done: make(chan struct{}), started: time.Now(), baselineSkipped: &app.mirrorState.BaselineSkipped}
	p, db := app.Persister, app.MetricStore
	conf := app.Config
	app.liveStoreMigration.Store(m)
	go func() {
		defer close(m.done)
		defer cancel()
		err := m.reconcile(ctx, p, db, conf)
		if err == nil {
			err = app.finishLiveStoreMigration(ctx, m, p, db)
		}
		if err != nil {
			phase := "failed"
			if errors.Is(err, context.Canceled) {
				phase = "cancelled"
			}
			_ = writeLiveMigrationReceipt(sharedStorePath(conf), m.receipt(phase, err))
			if phase == "failed" {
				zapwriter.Logger("storage").Error("live storage reconciliation failed", zap.Error(err))
			}
		}
	}()
}

// Stop before acquiring app.Lock: publication takes that lock, and joining
// while holding it would deadlock with a completing worker.
func (app *App) stopLiveStoreMigration() {
	app.stopIncrementalStoreMigration()
	if m := app.liveStoreMigration.Load(); m != nil {
		m.cancel()
		<-m.done
		app.liveStoreMigration.CompareAndSwap(m, nil)
	}
}

type liveMigrationTask struct {
	name       string
	verifiedID uint64
	audit      bool
}

func (m *liveStoreMigration) reconcile(ctx context.Context, p *persister.Whisper, db *store.Store, conf *Config) error {
	phasecpu.Set(phasecpu.Backfill)
	defer phasecpu.Set(phasecpu.Other)
	if p == nil || db == nil {
		return errors.New("live storage migration is missing its writer or database")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := conf.Whisper.StoreMigrationWorkers
	if workers == 0 {
		workers = 4
	}
	m.workerMaximum.Store(int64(workers))
	var budget *migrationBudget
	if path := conf.Whisper.StoreMigrationControlFile; path != "" {
		budget = newMigrationBudget(workers)
		budget.refresh(path, time.Now())
		m.budget.Store(budget)
		pollDone := make(chan struct{})
		go func() { defer close(pollDone); budget.run(ctx, path) }()
		defer func() { cancel(); <-pollDone }()
	}
	jobs := make(chan liveMigrationTask, workers*2)
	firstError := make(chan error, 1)
	var pending, group sync.WaitGroup
	var visited store.IDSet
	for range workers {
		group.Go(func() {
			for task := range jobs {
				if ctx.Err() == nil {
					if budget != nil && !budget.acquire() {
						pending.Done()
						continue
					}
					metadata, points, err := p.ReconcileMirrorMetric(ctx, task.name, task.verifiedID)
					if budget != nil {
						budget.release()
					}
					if err != nil {
						select {
						case firstError <- err:
						default:
						}
						cancel()
					} else {
						visited.Mark(metadata.ID)
						if task.audit {
							m.audited.Add(1)
						} else {
							m.metrics.Add(1)
						}
						m.points.Add(uint64(points))
					}
				}
				pending.Done()
			}
		})
	}
	enqueue := func(task liveMigrationTask) error {
		pending.Add(1)
		select {
		case jobs <- task:
			return nil
		case <-ctx.Done():
			pending.Done()
			return ctx.Err()
		}
	}
	dir := sharedStorePath(conf)
	last := time.Time{}
	progress := func() error {
		if time.Since(last) < 10*time.Second {
			return nil
		}
		last = time.Now()
		return writeLiveMigrationReceipt(dir, m.receipt("files", nil))
	}
	err := whisperio.WalkMetrics(ctx, conf.Whisper.DataDir, true, func(e whisperio.MetricPath) error {
		canonical := filepath.FromSlash(strings.ReplaceAll(e.Metric, ".", "/") + ".wsp")
		if canonical != e.Path {
			return errors.New("live migration requires canonical untagged metric paths")
		}
		if err := progress(); err != nil {
			return err
		}
		return enqueue(liveMigrationTask{name: e.Metric})
	})
	if err != nil {
		cancel()
	}
	pending.Wait()
	if err == nil {
		err = store.EachPage(ctx, db.ListPage, "", 10000, func(page []store.Metadata) error {
			if err := progress(); err != nil {
				return err
			}
			for _, metadata := range page {
				task := liveMigrationTask{name: metadata.Name, audit: true}
				if visited.Contains(metadata.ID) {
					task.verifiedID = metadata.ID
				}
				if err := enqueue(task); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err != nil {
		cancel()
	}
	close(jobs)
	group.Wait()
	select {
	case first := <-firstError:
		return first
	default:
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (app *App) finishLiveStoreMigration(ctx context.Context, m *liveStoreMigration, p *persister.Whisper, db *store.Store) (err error) {
	app.Lock()
	defer app.Unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	if app.Persister != p || app.MetricStore != db || app.Carbonserver == nil {
		return errors.New("live migration components changed before publication")
	}
	// This is the same worker object: keep its counters, Prometheus registration
	// and mirror failure state. Intake/cache and all read listeners stay live.
	started := time.Now()
	p.Stop()
	defer func() { err = errors.Join(err, p.Start()) }()
	if err = ctx.Err(); err != nil {
		return err
	}
	if p.HasPendingMirrors() {
		return errors.New("cannot publish shared reads with unreconciled mirror failures")
	}
	dir := sharedStorePath(app.Config)
	if err = os.Remove(filepath.Join(dir, "NEEDS-CATCHUP")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	app.Carbonserver.SetMetricStore(db)
	app.sharedStoreReads.Store(true)
	p.SetMirrorImportMissing(false)
	if err = writeLiveMigrationReceipt(dir, m.receipt("shared", nil)); err != nil {
		return err
	}
	zapwriter.Logger("storage").Info("live shared storage reads enabled", zap.Duration("writer_barrier", time.Since(started)), zap.Duration("migration_runtime", time.Since(m.started)), zap.Uint64("metrics", m.metrics.Load()), zap.Uint64("archive_points", m.points.Load()), zap.Uint64("catalog_audited", m.audited.Load()))
	return nil
}

// The setting is an explicit operator trust assertion about a completed full
// verification. Bind both trees and the receipt before allowing any row skip.
func validatedLiveBaseline(conf *Config) (store.CaptureReceipt, error) {
	var receipt store.CaptureReceipt
	copyRoot := conf.Whisper.StoreMigrationBaselineCopy
	if copyRoot == "" {
		return receipt, nil
	}
	if !filepath.IsAbs(copyRoot) {
		return receipt, errors.New("migration baseline copy must be absolute")
	}
	dir := sharedStorePath(conf)
	info, err := os.Lstat(filepath.Join(dir, "NEEDS-CATCHUP"))
	if errors.Is(err, os.ErrNotExist) && conf.Whisper.StoreLiveMigration && conf.Whisper.StoreMirrorFiles {
		// Publishing shared reads removes NEEDS-CATCHUP, but the durable mirror
		// guard remains. A successor must reconcile again. The original bound
		// receipt still permits only unchanged baseline IDs/generations/revisions
		// whose source extents match the retained capture; it never marks the
		// current session ready or trusts newly recreated catalog rows.
		info, err = os.Lstat(filepath.Join(dir, "MIRROR-RECONCILE-REQUIRED"))
	}
	if err != nil || !info.Mode().IsRegular() {
		return receipt, errors.New("baseline shortcut requires a completed live baseline awaiting reconciliation")
	}
	read := func(path string, out any) error {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 65536 {
			return errors.New("invalid baseline identity or receipt file")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, out)
	}
	var original, capture store.CaptureIdentity
	if err := read(filepath.Join(dir, "CONVERSION.json"), &original); err != nil {
		return receipt, err
	}
	if err := read(filepath.Join(copyRoot, "CAPTURE.json"), &capture); err != nil {
		return receipt, err
	}
	if err := read(filepath.Join(dir, "VERIFIED-CAPTURES.json"), &receipt); err != nil {
		return receipt, err
	}
	if original != capture || receipt.Identity != original || receipt.Version != 1 || receipt.Verification == "" || original.SourceID == "" || !original.Live {
		return receipt, errors.New("migration baseline receipt does not match both trees")
	}
	for _, pair := range [][2]string{{original.Source, conf.Whisper.DataDir}, {original.Copy, copyRoot}, {original.Destination, dir}} {
		a, err := filepath.EvalSymlinks(pair[0])
		if err != nil {
			return receipt, err
		}
		b, err := filepath.EvalSymlinks(pair[1])
		if err != nil {
			return receipt, err
		}
		if a != b {
			return receipt, errors.New("migration baseline identity differs from configured paths")
		}
	}
	return receipt, nil
}
