package carbon

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
	"github.com/go-graphite/go-carbon/internal/recovery"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

type SyncWriter struct {
	sync.Mutex
	w *bufio.Writer
}

func (s *SyncWriter) Write(p []byte) (n int, err error) {
	s.Lock()
	n, err = s.w.Write(p)
	s.Unlock()
	return
}

func (s *SyncWriter) Flush() error {
	s.Lock()
	defer s.Unlock()
	return s.w.Flush()
}

// DumpStop warms checkpoint names while input and persistence run, then stops
// persistence, diverts input into a WAL and writes a durable cache checkpoint.
// Compatible successors inherit reads and input; other configurations stop
// listeners and restore the same binary files in order.
//
// The input log is retired only once the dump and its input file are durable.
// If the dump fails, the process exits next with the log kept, so what it
// recorded is made durable first; a stop cut off by SIGKILL keeps it too.
func (app *App) DumpStop() (err error) {
	stopStarted := time.Now()
	app.migrationLifecycle.Lock()
	defer app.migrationLifecycle.Unlock()
	app.stopLiveStoreMigration()
	app.Lock()
	defer app.Unlock()
	if app.MetricStore != nil {
		app.MetricStore.StopCompactionPacing()
	}
	// Flush the store's memtables while input still flows: the flush that
	// must finish before reads stop then holds only what the last writes
	// left. See stopFlush.
	flush := app.startStopFlush()
	// The store's background scans would otherwise stop in its close, with
	// input closed: stop them while input flows. See stopStoreBackground.
	backgroundStopped := app.stopStoreBackground()
	defer func() {
		if err == nil {
			return
		}
		if syncErr := app.syncInputLog(); syncErr != nil {
			zapwriter.Logger("input-log").Error("input log not synced after the failed dump", zap.Error(syncErr))
		}
	}()

	if !app.Config.Dump.Enabled {
		// The input log is then the only copy of unpersisted points: stop
		// input and make every accepted point durable before exiting.
		if app.inputLog.Load() != nil {
			app.stopInputListeners()
			app.closeInputLog()
		}
		return nil
	}
	app.resolveSuccessor()
	lateStoreClose := app.Config.Whisper.StoreMirrorFiles && app.Config.Whisper.StoreLiveMigration &&
		!app.Config.Tags.Enabled && app.readHandoffPossible() && handoff.SharedStoreSupported(app.readSuccessor, app.ConfigFilename)
	// Once input is diverted to xlog, pending writes are no longer visible to
	// expiration's cache check. Finish cleanup before entering the dump phase.
	app.stopExpiration()
	_ = app.Cache.SetWriteoutBatching(0, 0)

	// Keep persistence and reads running while the index worker finishes. In
	// particular, quota accounting must not leave incoming points accumulating
	// without either persistence or WAL diversion.
	if app.Carbonserver != nil {
		app.Carbonserver.PauseIndexUpdates()
	}
	app.stopPendingRecovery()
	logger := zapwriter.Logger("dump")
	// Stop persistence alongside the warmup. Its workers finish the update
	// each holds, which took 3.86 s after a 2.41 s warmup on one host, and
	// the dump keeps every point they leave. Under cache pressure keep
	// persisting through the warmup, so input is not refused meanwhile.
	persister, persisterStopped := app.Persister, make(chan struct{})
	stopPersister := func() {
		defer close(persisterStopped)
		if persister != nil {
			started := time.Now()
			persister.Stop()
			logger.Info("persister stopped with input active", zap.Duration("runtime", time.Since(started)))
		}
	}
	persisterAlongsideWarmup := earlyPersisterStop && !app.Cache.Backlogged()
	if persisterAlongsideWarmup {
		go stopPersister()
	}
	// Freeze the read generation first, then warm its name classifications
	// while normal input and persistence still run. The later dump supplies
	// authoritative record counts; names evicted meanwhile need no recovery.
	var builder *recovery.Builder
	checkpointPublished := false
	if cs := app.Carbonserver; cs != nil && app.pendingReadsCompatible() && cs.PrepareReadCheckpoint() {
		lookups, finishWarm := cs.WarmCheckpointMetricLookups()
		builder = recovery.NewConcurrentBuilder(lookups, checkpointWorkers())
		builder.Reserve(int(app.Cache.Len()) + int(app.Cache.NotConfirmedLength()))
		warmStart := time.Now()
		if checkpointWarmHook != nil {
			checkpointWarmHook()
		}
		// ID allocation is sharded, so the cache's shards warm in parallel.
		var visited atomic.Int64
		warmErr := app.Cache.VisitMetricNamesParallel(checkpointWorkers(), func(name string) error {
			visited.Add(1)
			_, err := builder.ID(name)
			return err
		})
		visitRuntime := time.Since(warmStart)
		if warmErr != nil {
			logger.Warn("pending checkpoint warmup unavailable; using legacy dump", zap.Error(warmErr))
			builder = nil
		} else {
			builder.Prepare()
			logger.Info("pending checkpoint names warmed with input active", zap.Int64("names_visited", visited.Load()), zap.Duration("visit_runtime", visitRuntime), zap.Duration("runtime", time.Since(warmStart)))
		}
		finishWarm()
	}
	if !persisterAlongsideWarmup {
		stopPersister()
	}
	<-persisterStopped
	app.Persister = nil
	<-backgroundStopped
	// Nothing writes to the store from here on: flush what persistence wrote
	// since the stop began, alongside the dump.
	flush.again()

	logger.Info("grace stop with dump inited", zap.Duration("since_stop", time.Since(stopStarted)))

	filenamePostfix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	format, err := app.Config.Dump.cacheFormat()
	if err != nil {
		return err
	}
	dumpFilename := path.Join(app.Config.Dump.Path, recovery.CacheName(format, filenamePostfix+".bin"))
	// A compact checkpoint diverts input into a compact input file, whose
	// rows refer to the checkpoint builder's ids.
	walFormat := recovery.FormatLegacy
	if format.Compact() && builder != nil {
		walFormat = recovery.FormatCompact
	}
	xlogFilename := path.Join(app.Config.Dump.Path, recovery.InputName(walFormat, filenamePostfix+".bin"))

	// start dumpers
	logger.Info("start cache dump", zap.String("filename", dumpFilename), zap.Stringer("format", format))
	logger.Info("start wal write", zap.String("filename", xlogFilename))

	// The read generation is frozen and listeners remain available. Checkpoint
	// that exact overlay alongside the dump; later input names are indexed by the
	// pending checkpoint because the stopped worker cannot add them to the overlay.
	dump, err := recovery.NewWriterFormat(dumpFilename, 0, 1<<20, builder, format)
	if err != nil {
		return err
	}
	defer dump.Close()
	xlog, err := recovery.NewWriterFormat(xlogFilename, 1, recovery.InputFileBuffer, builder, walFormat)
	if err != nil {
		return err
	}
	defer xlog.Close()
	divert := &switchWriter{write: xlog.WritePoints}
	app.Cache.DivertToPointWriter(divert.Write)

	// Checkpoint work overlaps the dump: the read-index overlay belongs to the
	// frozen read generation (diverted input cannot add names to it), and the
	// builder classifies metrics as dump segments register them. Only metrics
	// first seen afterwards and the final write remain once input stops.
	var readID string
	var checkpointErr error
	var overlayDone time.Time
	dumpDone := make(chan struct{})
	var checkpointWork sync.WaitGroup
	if builder != nil {
		checkpointWork.Go(func() {
			readID, checkpointErr = app.Carbonserver.CheckpointReadIndex()
			overlayDone = time.Now()
		})
		checkpointWork.Go(func() {
			for {
				select {
				case <-dumpDone:
					return
				case <-time.After(200 * time.Millisecond):
					builder.Prepare()
				}
			}
		})
	}

	dumpStart := time.Now()
	cacheSize := app.Cache.Size()
	// Unclaimed saved metrics (after an interrupted recovery), then cache shard
	// ranges: all encoded concurrently and appended in order, so the file has
	// the same format as a serial dump.
	// One segment per cache shard keeps segments small, so the bounded worker
	// window recycles buffers.
	const segments = cache.ShardCount
	err = dump.WriteSegments(2*segments, dumpWorkers(), func(seg int, out *recovery.Segment) error {
		if seg < segments {
			return app.Cache.DumpPendingRange(seg, segments, out)
		}
		if format.Compact() {
			// Persistence and recovery are stopped and input is diverted:
			// queued point bytes stay immutable through the final block flush.
			return app.Cache.DumpShardsCompactTo(seg-segments, seg-segments+1, out)
		}
		return app.Cache.DumpShards(seg-segments, seg-segments+1, out.WritePoints)
	})
	encoded := time.Since(dumpStart)
	var cacheFile recovery.File
	if err == nil {
		cacheFile, err = dump.Flush()
	}
	close(dumpDone)
	if err != nil {
		checkpointWork.Wait()
		logger.Error("dump failed", zap.Error(err))
		return err
	}
	// Make the dump durable while the checkpoint is prepared. It must be on
	// disk before old sources are retired or a checkpoint referencing it is
	// published.
	cacheSynced := make(chan error, 1)
	syncStart := time.Now()
	go func() { cacheSynced <- dump.Sync() }()
	logger.Info("cache dump finished", zap.Int64("records", int64(cacheSize)), zap.Int64("bytes", cacheFile.Size), zap.Stringer("input_format", walFormat), zap.Int("workers", dumpWorkers()), zap.Duration("encode_runtime", encoded), zap.Duration("runtime", time.Since(dumpStart)))

	// Input still flows into the WAL: finish classifying dump metrics, wait
	// for the overlay, and wait for the dump to reach disk before stopping
	// input. Each of these can take seconds on a large cache, and every second
	// input is closed, senders must buffer or drop.
	if builder != nil {
		prepareStart := time.Now()
		// The dump's cores are free now: finish classification on them.
		builder.SetWorkers(dumpWorkers())
		builder.Prepare()
		classified := time.Since(prepareStart)
		checkpointWork.Wait()
		logger.Info("pending read checkpoint prepared", zap.Duration("runtime", time.Since(prepareStart)), zap.Duration("classify_runtime", classified), zap.Duration("overlay_since_dump_start", overlayDone.Sub(dumpStart)), zap.Duration("since_dump_start", time.Since(dumpStart)), zap.Error(checkpointErr))
	}
	// A compact index does not depend on input arriving later: write it
	// while input still flows, alongside the dump's sync.
	var compactIndex recovery.File
	var compactIndexErr error
	var compactIndexTime time.Duration
	compactIndexDone := make(chan struct{})
	if walFormat == recovery.FormatCompact && checkpointErr == nil && readID != "" {
		go func() {
			defer close(compactIndexDone)
			started := time.Now()
			if format == recovery.FormatBlocks {
				compactIndex, compactIndexErr = recovery.WriteBlocksIndex(app.Config.Dump.Path, builder)
			} else {
				compactIndex, compactIndexErr = recovery.WriteCompactIndex(app.Config.Dump.Path, builder)
			}
			compactIndexTime = time.Since(started)
		}()
	} else {
		close(compactIndexDone)
	}
	waitStart := time.Now()
	if err = <-cacheSynced; err != nil {
		logger.Error("dump sync failed", zap.Error(err))
		<-compactIndexDone
		return err
	}
	syncWait := time.Since(waitStart)
	<-compactIndexDone
	if walFormat == recovery.FormatCompact {
		logger.Info("compact checkpoint index written with input active", zap.Duration("runtime", compactIndexTime),
			zap.Int64("bytes", compactIndex.Size), zap.Error(compactIndexErr))
	} else if builder != nil {
		// Names first seen in the WAL during the sync.
		builder.Prepare()
	}

	// Finish the flushes while input still flows into the WAL. The flushes
	// started earlier may still run, and the flush after input closes waits
	// for every one of them as well as for its own: on one host it waited
	// 1.26 s with input closed.
	// This runs before an input tail starts: a store input handoff's tail is
	// written point by point, and the successor replays all of it before it
	// reads input. With the tail started first, a 25.6 s flush on one host
	// outlasted inputHandoffTimeout, so input stopped before the successor
	// even started, and the successor then spent 24 s loading the tail
	// (13.1M points) with input closed: a 32 s gap instead of a handoff.
	if app.MetricStore != nil && !app.Config.Whisper.StoreMirrorFiles {
		settleStart := time.Now()
		settled := flush.settle(preCloseFlushTimeout)
		logger.Info("shared store flushed with input active", zap.Duration("runtime", time.Since(settleStart)),
			zap.Int("flushes", flush.count), zap.Bool("complete", settled), zap.Duration("since_stop", time.Since(flush.started)))
	}
	// A successor that takes over reads and input lets input continue into a
	// tail log until it reads the sockets itself, so senders never see input
	// closed. Otherwise input stops here, before the WAL is completed.
	readsHandoff := app.readHandoffPossible() && handoff.SuccessorSupported(app.readSuccessor)
	wantHandover := readsHandoff && builder != nil && app.inputHandoffPossible() && handoff.InputSupported(app.readSuccessor)
	// A shared store's reads need its database, which the successor opens
	// only once this instance closed it, so reads cannot move. Input can.
	storeInputConfigured := !readsHandoff && builder != nil && app.storeInputHandoffPossible() && app.inputHandoffPossible()
	storeInput := storeInputConfigured && handoff.StoreInputSupported(app.readSuccessor)
	if storeInputConfigured && !storeInput {
		logger.Info("next binary does not support store input handoff; stopping input", zap.String("binary", app.readSuccessor))
	}
	var handover *inputHandover
	if wantHandover || storeInput {
		handover, err = app.startInputHandover(path.Join(app.Config.Dump.Path, recovery.TailName(path.Base(xlogFilename))), divert, logger)
		if err != nil {
			logger.Warn("input handoff unavailable", zap.Error(err))
			storeInput = false
		}
	}
	inputStopped := time.Now()
	if handover == nil {
		stopped := make(chan struct{})
		go func() { defer close(stopped); app.stopInputListeners() }()
		select {
		case <-time.After(5 * time.Second):
			logger.Info("waiting for input cleanup with read listeners available")
			<-stopped
		case <-stopped:
		}
	}
	walFile, err := xlog.Close()
	if err != nil {
		if handover != nil {
			handover.stop("dump failed")
		}
		return err
	}
	// The durable dump and WAL hold every point the input log protects, and
	// no input reaches the cache anymore. The successor must not replay it.
	app.retireInputLog(logger)
	logger.Info("dump finished", zap.Duration("input_stop_runtime", time.Since(inputStopped)), zap.Duration("cache_sync_runtime", waitStart.Sub(syncStart)+syncWait), zap.Duration("cache_sync_wait", syncWait), zap.Bool("input_handoff", handover != nil))
	// Every cached and diverted point is now in the dump and WAL (later input
	// goes to the successor's tail log), and no successor has started yet.
	app.markHeartbeatClean()
	app.restart.mu.Lock()
	app.restart.out = &restartRecord{Version: 1, PID: os.Getpid(), Dump: path.Base(dumpFilename), StopStarted: stopStarted}
	if handover == nil {
		app.restart.out.InputClosed = inputStopped
	}
	app.restart.mu.Unlock()
	// The new dump holds every saved point not yet on disk, so a pending
	// generation from the previous restart is now redundant.
	if err = app.Cache.RetirePendingSources(); err != nil {
		logger.Warn("previous recovery sources not retired", zap.Error(err))
	}

	if builder != nil {
		// The ordinary .bin files remain usable even if the optional accelerator
		// cannot be published. Never advertise a partial point/catalogue pair.
		finalizeStart := time.Now()
		var indexWritten time.Time
		if checkpointErr == nil && readID != "" {
			var index recovery.File
			if walFormat == recovery.FormatCompact {
				index, checkpointErr = compactIndex, compactIndexErr
			} else {
				index, checkpointErr = recovery.WriteIndex(app.Config.Dump.Path, builder)
			}
			indexWritten = time.Now()
			if checkpointErr == nil {
				checkpointErr = recovery.Publish(app.Config.Dump.Path, app.Config.Whisper.DataDir, cacheFile, walFile, index, readID)
				checkpointPublished = checkpointErr == nil && readID != ""
			}
		}
		switch {
		case checkpointErr != nil:
			logger.Warn("pending read checkpoint unavailable; saved legacy dump", zap.Error(checkpointErr))
			if handover != nil {
				// The successor restores in order, which takes far longer
				// than input may stay in the tail.
				handover.stop("checkpoint unavailable")
			}
		case handover != nil:
			logger.Info("pending read checkpoint saved", zap.Duration("finalize_runtime", time.Since(finalizeStart)), zap.Duration("index_write_runtime", indexWritten.Sub(finalizeStart)))
		default:
			logger.Info("pending read checkpoint saved", zap.Duration("finalize_runtime", time.Since(finalizeStart)), zap.Duration("index_write_runtime", indexWritten.Sub(finalizeStart)), zap.Duration("input_closed_for", time.Since(inputStopped)))
		}
	}
	// The successor replays every unflushed memtable from the WAL before it
	// serves; one host replayed 17 GB for nearly seven minutes. Writers are
	// stopped, so flush now while reads are still served, bounded so a slow
	// device cannot hold the stop past the service manager's timeout.
	if app.MetricStore != nil && !app.Config.Whisper.StoreMirrorFiles {
		flushStart := time.Now()
		// Normally nothing was written since the flush before input closed.
		late := flush.dirty()
		if late {
			flush.again()
		}
		select {
		case <-flush.done():
			logger.Info("shared store flushed with reads available", zap.Duration("runtime", time.Since(flushStart)),
				zap.Int("flushes", flush.count), zap.Bool("late_flush", late), zap.Duration("since_stop", time.Since(flush.started)))
		case <-time.After(preStopFlushTimeout):
			logger.Warn("shared store flush still running; stopping reads", zap.Duration("runtime", time.Since(flushStart)))
		}
	}
	// Live-migration successors can serve the checkpoint and inherit input before
	// opening the shared database. Transfer those sockets before closing it:
	// releasing a large cache can take seconds and must not extend the input
	// tail or freeze checkpoint-only reads throughout that teardown.
	lateStoreClose = lateStoreClose && readsHandoff && checkpointPublished
	if app.Config.Whisper.StoreMirrorFiles && app.MetricStore != nil {
		if app.Buckyd != nil {
			if err := app.Buckyd.Stop(); err != nil {
				return fmt.Errorf("stop transfers before storage handoff: %w", err)
			}
			app.Buckyd = nil
		}
		if app.Carbonserver != nil {
			if err := app.Carbonserver.UseMirroredFilesForHandoff(); err != nil {
				return err
			}
		}
		if app.Collector != nil {
			app.Collector.Stop()
			app.Collector = nil
		}
		if !lateStoreClose {
			if err := app.closeMetricStore(); err != nil {
				return fmt.Errorf("close mirrored storage before handoff: %w", err)
			}
		}
		app.sharedStoreReads.Store(false)
	}
	// The successor may start during handOffReads: it finds this record and
	// waits for the final one, written just before this process exits.
	app.recordStop(func(*restartRecord) {})
	var readsTaken, readsStopped time.Time
	if storeInput && checkpointPublished {
		readsStopped = app.handOffStoreInput(logger, handover)
	} else {
		readsTaken = app.handOffReads(logger, readsHandoff, handover)
	}
	if app.Config.Whisper.StoreMirrorFiles && app.MetricStore != nil {
		closed := time.Now()
		if err := app.closeMetricStore(); err != nil {
			return fmt.Errorf("close mirrored storage after handoff: %w", err)
		}
		logger.Info("mirrored storage closed after socket handoff", zap.Duration("runtime", time.Since(closed)))
	}
	if handover != nil {
		handover.stop("successor did not take input")
	}
	app.recordStop(func(r *restartRecord) {
		r.ReadsStopped = readsTaken
		if r.ReadsStopped.IsZero() {
			r.ReadsStopped = readsStopped
		}
		if r.ReadsStopped.IsZero() {
			r.ReadsStopped = time.Now()
		}
		if handover != nil {
			r.InputClosed = handover.closedAt()
		}
	})
	if readsStopped.IsZero() {
		logger.Info("stop read listeners")
		<-app.stopReadListeners()
		logger.Info("listeners stopped")
	}

	// logger.Info("stop all")
	// app.stopAll()

	return nil
}

// stopFlush flushes the shared store during a dump stop. The successor
// replays every memtable that was not flushed, so the stop flushes before it
// stops reads; on a busy store a memtable holds up to a gigabyte and took
// 4.25 s to flush, all of it with input closed. Flushes now start when the
// stop begins (persistence still writing) and again once the persister
// stopped, each flushing the memtables written before it, and the stop waits
// for them before input closes. After input closes it flushes only what was
// written since, normally nothing. Flushing earlier is an ordinary flush and
// changes nothing a crash would recover.
type stopFlush struct {
	app     *App
	started time.Time
	last    <-chan struct{}
	count   int
	// walBytes is the store's WAL size when the last flush started; see
	// dirty.
	walBytes uint64
}

// stopFlusher is the store as stopFlush uses it.
type stopFlusher interface {
	AsyncFlush() (<-chan struct{}, error)
	WALBytes() uint64
}

// stopFlushStore returns the store stopFlush flushes; tests wrap it.
var stopFlushStore = func(db *store.Store) stopFlusher { return db }

// earlyStopFlush enables the flushes before input closes.
var earlyStopFlush = true

// earlyPersisterStop stops persistence alongside the checkpoint warmup.
var earlyPersisterStop = true

// checkpointWarmHook, if set, runs as the checkpoint warmup starts; tests use
// it to observe what runs alongside the warmup.
var checkpointWarmHook func()

// startStopFlush starts the first flush. Assumes we are holding app.Lock().
func (app *App) startStopFlush() *stopFlush {
	f := &stopFlush{app: app, started: time.Now()}
	if earlyStopFlush && app.Config.Dump.Enabled {
		f.again()
	}
	return f
}

// db returns the store to flush, or nil when there is none to flush.
func (f *stopFlush) db() stopFlusher {
	if f.app.MetricStore == nil || f.app.Config.Whisper.StoreMirrorFiles {
		return nil
	}
	return stopFlushStore(f.app.MetricStore)
}

// again flushes the memtables written so far, after the earlier flushes.
func (f *stopFlush) again() {
	db := f.db()
	if db == nil {
		return
	}
	written := db.WALBytes()
	done, err := db.AsyncFlush()
	if err != nil {
		zapwriter.Logger("dump").Warn("shared store flush failed", zap.Error(err))
		return
	}
	f.last, f.count, f.walBytes = done, f.count+1, written
}

// dirty reports whether the store may hold writes no flush started so far
// covers. A flush holds every write made before it started, so if the WAL
// has not grown since, nothing was written after.
func (f *stopFlush) dirty() bool {
	db := f.db()
	return db != nil && (f.last == nil || db.WALBytes() != f.walBytes)
}

// settle flushes what was written since the last flush, if anything, and
// waits up to timeout for every flush to finish. It reports whether they did.
func (f *stopFlush) settle(timeout time.Duration) bool {
	if f.dirty() {
		f.again()
	}
	select {
	case <-f.done():
		return true
	case <-time.After(timeout):
		return false
	}
}

// done is closed once every flush started so far completed.
func (f *stopFlush) done() <-chan struct{} {
	if f.last == nil {
		c := make(chan struct{})
		close(c)
		return c
	}
	return f.last
}

// stopStoreBackground stops the shared store's background scans, the
// catalog membership scan and the relayout, without waiting; the returned
// channel closes once they have stopped. A scan still running when the store
// closes holds up the close, with input closed: the membership scan of a
// young process took 1.54 s to stop there. Nothing needs them during a stop.
// Assumes we are holding app.Lock().
func (app *App) stopStoreBackground() <-chan struct{} {
	stopped := make(chan struct{})
	db := app.MetricStore
	if db == nil || !app.Config.Dump.Enabled {
		close(stopped)
		return stopped
	}
	go func() {
		defer close(stopped)
		started := time.Now()
		db.StopBackground()
		zapwriter.Logger("dump").Info("shared store background work stopped with input active", zap.Duration("runtime", time.Since(started)))
	}()
	return stopped
}

// preCloseFlushTimeout bounds how long the stop waits for its flushes before
// it closes input. Input diverted meanwhile grows the input file the
// successor loads before it serves, so the wait is shorter than the flush's
// own bound after input closes.
const preCloseFlushTimeout = 30 * time.Second

// preStopFlushTimeout bounds the flush a dump stop runs with reads available.
const preStopFlushTimeout = 2 * time.Minute

// readHandoffTimeout bounds how long a stopped instance keeps serving reads for
// a successor that is still restoring before it falls back to closing them.
const readHandoffTimeout = 30 * time.Minute

func readHandoffPath(conf *Config) string { return path.Join(conf.Dump.Path, "read-handoff.sock") }

// RegisterReadHandoff claims the read listener of a stopping instance, if one
// offers it. Call it as early as possible after parsing the config: a stopping
// instance waits only handoff.RegisterTimeout before closing its listener.
func (app *App) RegisterReadHandoff() {
	if app.Config.Dump.Path != "" && app.Config.Carbonserver.Enabled {
		app.handoffClaim = handoff.Register(readHandoffPath(app.Config))
	}
}

// SetReadHandoff enables read handoff on dump stop. release must let the next
// instance start (for example by ending a supervisor) and free every resource
// it would bind, except the read listener this instance keeps serving.
// successor is the binary the service manager will start next.
func (app *App) SetReadHandoff(successor string, release func()) {
	app.Lock()
	app.readSuccessor, app.readRelease = successor, release
	app.Unlock()
}

// SetSuccessorResolver lets DumpStop ask which binary the service manager
// starts next, when that may differ from the one given to SetReadHandoff (for
// example after a service override pointed the service at another binary).
// resolve returns "" when it cannot tell.
func (app *App) SetSuccessorResolver(resolve func() string) {
	app.Lock()
	app.successorResolver = resolve
	app.Unlock()
}

// resolveSuccessor replaces the next binary by the service manager's, if it
// can tell. Assumes we are holding app.Lock().
func (app *App) resolveSuccessor() {
	if app.successorResolver == nil || app.readRelease == nil {
		return
	}
	started := time.Now()
	next := app.successorResolver()
	if next == "" || next == app.readSuccessor {
		return
	}
	zapwriter.Logger("dump").Info("service manager starts another binary next", zap.String("invoked_as", app.readSuccessor),
		zap.String("next_binary", next), zap.Duration("runtime", time.Since(started)))
	app.readSuccessor = next
}

// readHandoffPossible reports whether this instance can hand reads over.
// Assumes we are holding app.Lock().
func (app *App) readHandoffPossible() bool {
	cs := app.Carbonserver
	// Mirrored sessions retain the original file index and update stock's files.
	// Stock can inherit that checkpoint without competing for the shared handle.
	storageCompatible := app.MetricStore == nil || app.Config.Whisper.StoreMirrorFiles
	return app.readRelease != nil && cs != nil && cs.HTTPListener() != nil && storageCompatible && app.Tags == nil
}

// handOffReads runs after the dump and checkpoint are durable. Data this
// instance serves is then frozen and equals what the next instance serves once
// ready, so both may accept on the same socket until the successor reports it
// serves. The successor opens intake only after this instance stopped
// accepting: input is stopped by then, or handover hands it over. On any
// failure reads stop normally. supported is whether the successor can take
// reads over.
// It returns when the successor took reads over, or zero.
func (app *App) handOffReads(logger *zap.Logger, supported bool, handover *inputHandover) time.Time {
	if !app.readHandoffPossible() {
		return time.Time{}
	}
	if !supported {
		logger.Info("next binary does not support read handoff", zap.String("binary", app.readSuccessor))
		return time.Time{}
	}
	offer, err := handoff.NewOffer(readHandoffPath(app.Config))
	if err != nil {
		logger.Warn("read handoff unavailable", zap.Error(err))
		return time.Time{}
	}
	logger.Info("serving reads until successor takes over")
	app.readRelease()
	started := time.Now()
	var input handoff.InputSource
	if handover != nil {
		input = handover
	}
	taken, err := offer.Serve(readHandoffTimeout, app.Carbonserver.HTTPListener(), app.Carbonserver.StopAccepting, input, inputHandoffTimeout)
	if taken {
		logger.Info("read listener handed off", zap.Duration("wait", time.Since(started)), zap.Error(err))
		return time.Now()
	}
	logger.Warn("read handoff not taken", zap.Duration("wait", time.Since(started)), zap.Error(err))
	return time.Time{}
}

// storeInputHandoffPossible reports whether this shared-store instance may
// hand its input over although its reads cannot move. The worker outlives the
// supervisor until the successor takes input, so it is opt-in.
// Assumes we are holding app.Lock().
func (app *App) storeInputHandoffPossible() bool {
	c := app.Config
	return c.Dump.StoreInputHandoff && app.readRelease != nil && app.MetricStore != nil && !c.Whisper.StoreMirrorFiles &&
		app.Tags == nil && app.Carbonserver != nil && c.Carbonserver.Enabled
}

// handOffStoreInput hands a shared store's input to its successor. Reads stop
// and the database closes first, since the successor serves reads only once
// it opened the database; then the successor is released, and input keeps
// flowing into the tail log until it takes the sockets. On any failure the
// caller stops input as without handoff. It returns when reads stopped, or
// zero if they are still open.
func (app *App) handOffStoreInput(logger *zap.Logger, handover *inputHandover) time.Time {
	offer, err := handoff.NewOffer(readHandoffPath(app.Config))
	if err != nil {
		logger.Warn("input handoff unavailable", zap.Error(err))
		return time.Time{}
	}
	logger.Info("stop read listeners")
	<-app.stopReadListeners()
	readsStopped := time.Now()
	logger.Info("listeners stopped")
	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
	}
	if err := app.closeMetricStoreForExit(); err != nil {
		// Leave the release to the ordinary path.
		logger.Error("close shared storage before input handoff", zap.Error(err))
		_ = offer.Close()
		return readsStopped
	}
	logger.Info("serving input until successor takes over", zap.String("next_binary", app.readSuccessor))
	// The successor starts only now: give it the full window from here.
	handover.released()
	app.readRelease()
	started := time.Now()
	stats, taken, err := offer.ServeInputStats(handover, inputHandoffTimeout)
	fields := []zap.Field{zap.Duration("wait", time.Since(started)), zap.String("successor_binary", stats.Peer),
		zap.Bool("successor_supported", stats.PeerSupported), zap.Bool("successor_checked", stats.PeerKnown)}
	if !stats.Registered.IsZero() {
		fields = append(fields, zap.Duration("register_wait", stats.Registered.Sub(started)))
	}
	switch {
	case taken:
		logger.Info("input handed off to successor", fields...)
	case errors.Is(err, handoff.ErrSuccessorCannotTakeInput):
		// The caller stops input at once: every second more is tail the
		// successor loads before it reads input itself.
		logger.Warn("input handoff not taken: successor cannot take input", append(fields, zap.Error(err))...)
	default:
		logger.Warn("input handoff not taken", append(fields, zap.Error(err))...)
	}
	return readsStopped
}

// ReleaseForHandoff closes state another instance would lock or treat as
// corrupt if still held, after a successful DumpStop. The process must then exit
// without touching persistence; its remaining teardown is memory only.
func (app *App) ReleaseForHandoff() {
	app.Lock()
	defer app.Unlock()
	if app.Tags != nil {
		app.Tags.Stop()
		app.Tags = nil
	}
	if app.MetricStore != nil {
		// The process exits after releasing to the supervisor; skipping the
		// table reader teardown shortens the window with reads refused.
		if err := app.closeMetricStoreForExit(); err != nil {
			zapwriter.Logger("dump").Error("close shared storage", zap.Error(err))
		}
	}
}

// dumpWorkers sets the dump fan-out. The dump is on the restart's critical
// path, so it may use half the cores; reads keep the rest.
func dumpWorkers() int {
	return min(max(runtime.GOMAXPROCS(0)/2, 1), 128)
}

// checkpointWorkers bounds catalogue classification so reads, which remain
// available during the checkpoint, keep most of the host's CPU.
func checkpointWorkers() int {
	return min(max(runtime.GOMAXPROCS(0)/4, 1), 32)
}

// RestoreFromFile read and parse data from single file
func (app *App) RestoreFromFile(filename string, storeFunc func(*points.Points)) error {
	phasecpu.Set(phasecpu.Restore)
	defer phasecpu.Set(phasecpu.Other)
	var pointsCount int
	startTime := time.Now()

	logger := zapwriter.Logger("restore").With(zap.String("filename", filename))
	logger.Info("restore started")

	defer func() {
		logger.Info("restore finished",
			zap.Int("points", pointsCount),
			zap.Duration("runtime", time.Since(startTime)),
		)
	}()

	visit := func(p *points.Points) {
		pointsCount += len(p.Data)
		storeFunc(p)
	}
	switch recovery.CacheFormat(path.Base(filename)) {
	case recovery.FormatCompact:
		return recovery.ReadCompactFile(filename, visit)
	case recovery.FormatBlocks:
		return recovery.ReadBlocksFile(filename, visit)
	}
	if recovery.InputFormat(path.Base(filename)) == recovery.FormatCompact {
		return recovery.ReadCompactInputFile(filename, visit)
	}
	return points.ReadFromFile(filename, visit)
}

// RestoreFromDir cache and input dumps from disk to memory
func (app *App) RestoreFromDir(dumpDir string, storeFunc func(*points.Points)) {
	startTime := time.Now()

	logger := zapwriter.Logger("restore").With(zap.String("dir", dumpDir))

	defer func() {
		logger.Info("restore finished",
			zap.Duration("runtime", time.Since(startTime)),
		)
	}()

	files, err := os.ReadDir(dumpDir)
	if err != nil {
		logger.Error("readdir failed", zap.Error(err))
		return
	}

	// read files and lazy sorting
	list := make([]string, 0)

FilesLoop:
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		r := strings.Split(file.Name(), ".")
		if len(r) < 3 { // {input,cache}.pid.nanotimestamp(.+)?
			continue
		}

		var fileWithSortPrefix string

		switch r[0] {
		case "cache", strings.TrimSuffix(recovery.CompactPrefix, "."), strings.TrimSuffix(recovery.BlocksPrefix, "."):
			fileWithSortPrefix = fmt.Sprintf("%s_%s:%s", r[2], "1", file.Name())
		case strings.TrimSuffix(recovery.CompactInputPrefix, "."):
			fileWithSortPrefix = fmt.Sprintf("%s_%s:%s", r[2], "2", file.Name())
		case "input":
			// A stopping instance still appends to its tail; input opens only
			// after the tail is complete and replayed (see loadInputTails).
			if recovery.IsTail(file.Name()) && handoff.Locked(path.Join(dumpDir, file.Name())) {
				continue FilesLoop
			}
			fileWithSortPrefix = fmt.Sprintf("%s_%s:%s", r[2], "2", file.Name())
		default:
			continue FilesLoop
		}

		list = append(list, fileWithSortPrefix)
	}

	if len(list) == 0 {
		logger.Info("nothing for restore")
		return
	}

	sort.Strings(list)

	for index, fileWithSortPrefix := range list {
		list[index] = strings.SplitN(fileWithSortPrefix, ":", 2)[1]
	}

	logger.Info("start restore", zap.Int("files", len(list)))

	var restoredBytes int64
	defer func() {
		app.setRestoredDump(restoredBytes)
		app.setRestoreDuration(time.Since(startTime))
	}()
	for _, fn := range list {
		filename := path.Join(dumpDir, fn)
		if info, err := os.Stat(filename); err == nil {
			restoredBytes += info.Size()
		}
		app.RestoreFromFile(filename, storeFunc)

		// The input log recorded the restored points; they must be durable
		// there before their only other copy goes.
		if err = app.syncInputLog(); err != nil {
			logger.Error("input log sync failed; keeping restored file", zap.String("filename", filename), zap.Error(err))
			continue
		}
		err = os.Remove(filename)
		if err != nil {
			logger.Error("remove failed", zap.String("filename", filename), zap.Error(err))
		}
	}
}

// Restore from dump.path
func (app *App) Restore(storeFunc func(*points.Points), path string, rps int) {
	if rps > 0 {
		ticker := helper.NewThrottleTicker(rps)
		defer ticker.Stop()

		throttledStoreFunc := func(p *points.Points) {
			for i := 0; i < len(p.Data); i++ {
				<-ticker.C
			}
			storeFunc(p)
		}

		app.RestoreFromDir(path, throttledStoreFunc)
	} else {
		app.RestoreFromDir(path, storeFunc)
	}
}
