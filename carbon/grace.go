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

// DumpStop implements gracefully stop:
// * Start writing all new data to xlogs
// * Stop cache worker
// * Dump all cache to file
// * Stop listeners
// * Close xlogs
// * Exit application
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
	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}

	logger := zapwriter.Logger("dump")

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
	var builder *recovery.Builder
	if cs := app.Carbonserver; cs != nil {
		if cs.HasMappedIndex() && app.pendingReadsCompatible() {
			builder = recovery.NewConcurrentBuilder(cs.SavedMetricLookups(), checkpointWorkers())
			builder.Reserve(int(app.Cache.Len()) + int(app.Cache.NotConfirmedLength()))
		}
	}
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
	app.Cache.DivertToPointWriter(xlog.WritePoints)

	// Checkpoint work overlaps the dump: the read-index overlay belongs to the
	// frozen read generation (diverted input cannot add names to it), and the
	// builder classifies metrics as dump segments register them. Only metrics
	// first seen afterwards and the final write remain once input stops.
	var readID string
	var checkpointErr error
	dumpDone := make(chan struct{})
	var checkpointWork sync.WaitGroup
	if builder != nil {
		checkpointWork.Go(func() { readID, checkpointErr = app.Carbonserver.CheckpointReadIndex() })
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
	if err == nil {
		_, err = dump.Close()
	}
	close(dumpDone)
	if err != nil {
		checkpointWork.Wait()
		logger.Error("dump failed", zap.Error(err))
		return err
	}
	cacheFile, _ := dump.Close()
	logger.Info("cache dump finished", zap.Int64("records", int64(cacheSize)), zap.Int("workers", dumpWorkers()), zap.Duration("runtime", time.Since(dumpStart)))

	// Input still flows into the WAL: finish classifying dump metrics and wait
	// for the overlay before stopping input.
	if builder != nil {
		prepareStart := time.Now()
		builder.Prepare()
		checkpointWork.Wait()
		logger.Info("pending read checkpoint prepared", zap.Duration("runtime", time.Since(prepareStart)), zap.Duration("since_dump_start", time.Since(dumpStart)), zap.Error(checkpointErr))
	}

	inputStopped := time.Now()
	stopped := make(chan struct{})
	go func() { defer close(stopped); app.stopInputListeners() }()
	select {
	case <-time.After(5 * time.Second):
		logger.Info("waiting for input cleanup with read listeners available")
		<-stopped
	case <-stopped:
	}
	walFile, err := xlog.Close()
	if err != nil {
		return err
	}
	logger.Info("dump finished", zap.Duration("input_stop_runtime", time.Since(inputStopped)))
	// The new dump holds every saved point not yet on disk, so a pending
	// generation from the previous restart is now redundant.
	if err = app.Cache.RetirePendingSources(); err != nil {
		logger.Warn("previous recovery sources not retired", zap.Error(err))
	}

	if builder != nil {
		// The ordinary .bin files remain usable even if the optional accelerator
		// cannot be published. Never advertise a partial point/catalogue pair.
		finalizeStart := time.Now()
		if checkpointErr == nil && readID != "" {
			var index recovery.File
			index, checkpointErr = recovery.WriteIndex(app.Config.Dump.Path, builder)
			if checkpointErr == nil {
				checkpointErr = recovery.Publish(app.Config.Dump.Path, app.Config.Whisper.DataDir, cacheFile, walFile, index, readID)
			}
		}
		if checkpointErr != nil {
			logger.Warn("pending read checkpoint unavailable; saved legacy dump", zap.Error(checkpointErr))
		} else {
			logger.Info("pending read checkpoint saved", zap.Duration("finalize_runtime", time.Since(finalizeStart)), zap.Duration("input_closed_for", time.Since(inputStopped)))
		}
	}
	app.handOffReads(logger)
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

// handOffReads runs after input stopped and the dump and checkpoint are
// durable. Data this instance serves is then frozen and equals what the next
// instance serves once ready, so both may accept on the same socket until the
// successor reports it serves. The successor opens intake only after this
// instance stopped accepting. On any failure reads stop normally.
func (app *App) handOffReads(logger *zap.Logger) {
	cs := app.Carbonserver
	if app.readRelease == nil || cs == nil || cs.HTTPListener() == nil || app.MetricStore != nil || app.Tags != nil {
		return
	}
	if !handoff.SuccessorSupported(app.readSuccessor) {
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
	taken, err := offer.Serve(readHandoffTimeout, cs.HTTPListener(), cs.StopAccepting)
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
