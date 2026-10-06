package carbon

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/helper"
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
	_ = app.Cache.SetWriteoutBatching(0, 0)

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

	// Freeze the read generation while its listeners remain available. The tiny
	// mutable overlay is checkpointed after all input notifications have drained.
	var builder *recovery.Builder
	if cs := app.Carbonserver; cs != nil {
		cs.PauseIndexUpdates()
		if cs.HasMappedIndex() && app.pendingReadsCompatible() {
			builder = recovery.NewBuilder(cs.SavedMetricExists)
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

	dumpStart := time.Now()
	cacheSize := app.Cache.Size()
	if err = app.Cache.DumpPoints(dump.WritePoints); err != nil {
		logger.Error("dump failed", zap.Error(err))
		return err
	}
	cacheFile, err := dump.Close()
	if err != nil {
		return err
	}
	logger.Info("cache dump finished", zap.Int64("records", int64(cacheSize)), zap.Duration("runtime", time.Since(dumpStart)))

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
	logger.Info("dump finished")

	if builder != nil {
		// The ordinary .bin files remain usable even if the optional accelerator
		// cannot be published. Never advertise a partial point/catalogue pair.
		readID, checkpointErr := app.Carbonserver.CheckpointReadIndex()
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
			logger.Info("pending read checkpoint saved")
		}
	}
	logger.Info("stop read listeners")
	<-app.stopReadListeners()
	logger.Info("listeners stopped")

	// logger.Info("stop all")
	// app.stopAll()

	return nil
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
