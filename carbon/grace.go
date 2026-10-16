package carbon

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/handoff"
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
func (app *App) DumpStop() error {
	app.Lock()
	defer app.Unlock()

	if !app.Config.Dump.Enabled {
		return nil
	}
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
	// Freeze the read generation first, then warm its name classifications
	// while normal input and persistence still run. The later dump supplies
	// authoritative record counts; names evicted meanwhile need no recovery.
	var builder *recovery.Builder
	if cs := app.Carbonserver; cs != nil && cs.HasMappedIndex() && app.pendingReadsCompatible() {
		lookups, finishWarm := cs.WarmCheckpointMetricLookups()
		builder = recovery.NewConcurrentBuilder(lookups, checkpointWorkers())
		builder.Reserve(int(app.Cache.Len()) + int(app.Cache.NotConfirmedLength()))
		warmStart := time.Now()
		visited := 0
		warmErr := app.Cache.VisitMetricNames(func(name string) error {
			visited++
			_, err := builder.ID(name)
			return err
		})
		if warmErr != nil {
			logger.Warn("pending checkpoint warmup unavailable; using legacy dump", zap.Error(warmErr))
			builder = nil
		} else {
			builder.Prepare()
			logger.Info("pending checkpoint names warmed with input active", zap.Int("names_visited", visited), zap.Duration("runtime", time.Since(warmStart)))
		}
		finishWarm()
	}
	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}

	logger.Info("grace stop with dump inited")

	filenamePostfix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	dumpFilename := path.Join(app.Config.Dump.Path, fmt.Sprintf("cache.%s.bin", filenamePostfix))
	xlogFilename := path.Join(app.Config.Dump.Path, fmt.Sprintf("input.%s.bin", filenamePostfix))

	// start dumpers
	logger.Info("start cache dump", zap.String("filename", dumpFilename))
	logger.Info("start wal write", zap.String("filename", xlogFilename))

	// The read generation is frozen and listeners remain available. Checkpoint
	// that exact overlay alongside the dump; later input names are indexed by the
	// pending checkpoint because the stopped worker cannot add them to the overlay.
	dump, err := recovery.NewWriter(dumpFilename, 0, 1<<20, builder)
	if err != nil {
		return err
	}
	defer dump.Close()
	xlog, err := recovery.NewWriter(xlogFilename, 1, 4096, builder)
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
	logger.Info("cache dump finished", zap.Int64("records", int64(cacheSize)), zap.Int("workers", dumpWorkers()), zap.Duration("encode_runtime", encoded), zap.Duration("runtime", time.Since(dumpStart)))

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
	waitStart := time.Now()
	if err = <-cacheSynced; err != nil {
		logger.Error("dump sync failed", zap.Error(err))
		return err
	}
	syncWait := time.Since(waitStart)
	if builder != nil {
		// Names first seen in the WAL during the sync.
		builder.Prepare()
	}

	// A successor that takes over reads and input lets input continue into a
	// tail log until it reads the sockets itself, so senders never see input
	// closed. Otherwise input stops here, before the WAL is completed.
	readsHandoff := app.readHandoffPossible() && handoff.SuccessorSupported(app.readSuccessor)
	var handover *inputHandover
	if readsHandoff && builder != nil && app.inputHandoffPossible() && handoff.InputSupported(app.readSuccessor) {
		handover, err = app.startInputHandover(path.Join(app.Config.Dump.Path, recovery.TailName(path.Base(xlogFilename))), divert, logger)
		if err != nil {
			logger.Warn("input handoff unavailable", zap.Error(err))
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
	logger.Info("dump finished", zap.Duration("input_stop_runtime", time.Since(inputStopped)), zap.Duration("cache_sync_runtime", waitStart.Sub(syncStart)+syncWait), zap.Duration("cache_sync_wait", syncWait), zap.Bool("input_handoff", handover != nil))
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
			index, checkpointErr = recovery.WriteIndex(app.Config.Dump.Path, builder)
			indexWritten = time.Now()
			if checkpointErr == nil {
				checkpointErr = recovery.Publish(app.Config.Dump.Path, app.Config.Whisper.DataDir, cacheFile, walFile, index, readID)
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
	app.handOffReads(logger, readsHandoff, handover)
	if handover != nil {
		handover.stop("successor did not take input")
	}
	logger.Info("stop read listeners")
	<-app.stopReadListeners()
	logger.Info("listeners stopped")

	// logger.Info("stop all")
	// app.stopAll()

	return nil
}

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

// readHandoffPossible reports whether this instance can hand reads over.
// Assumes we are holding app.Lock().
func (app *App) readHandoffPossible() bool {
	cs := app.Carbonserver
	return app.readRelease != nil && cs != nil && cs.HTTPListener() != nil && app.MetricStore == nil && app.Tags == nil
}

// handOffReads runs after the dump and checkpoint are durable. Data this
// instance serves is then frozen and equals what the next instance serves once
// ready, so both may accept on the same socket until the successor reports it
// serves. The successor opens intake only after this instance stopped
// accepting: input is stopped by then, or handover hands it over. On any
// failure reads stop normally. supported is whether the successor can take
// reads over.
func (app *App) handOffReads(logger *zap.Logger, supported bool, handover *inputHandover) {
	if !app.readHandoffPossible() {
		return
	}
	if !supported {
		logger.Info("next binary does not support read handoff", zap.String("binary", app.readSuccessor))
		return
	}
	offer, err := handoff.NewOffer(readHandoffPath(app.Config))
	if err != nil {
		logger.Warn("read handoff unavailable", zap.Error(err))
		return
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
	} else {
		logger.Warn("read handoff not taken", zap.Duration("wait", time.Since(started)), zap.Error(err))
	}
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
		if err := app.MetricStore.Close(); err != nil {
			zapwriter.Logger("dump").Error("close shared storage", zap.Error(err))
		}
		app.MetricStore = nil
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

	err := points.ReadFromFile(filename, func(p *points.Points) {
		pointsCount += len(p.Data)
		storeFunc(p)
	})

	return err
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
		case "cache":
			fileWithSortPrefix = fmt.Sprintf("%s_%s:%s", r[2], "1", file.Name())
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

	for _, fn := range list {
		filename := path.Join(dumpDir, fn)
		app.RestoreFromFile(filename, storeFunc)

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
