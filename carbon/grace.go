package carbon

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/go-carbon/persister"
	"github.com/lomik/go-carbon/points"
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

	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}

	logger := zapwriter.Logger("dump")

	logger.Info("grace stop with dump inited")

	filenamePostfix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	dumpFilename := path.Join(app.Config.Dump.Path, fmt.Sprintf("cache.%s.bin", filenamePostfix))
	xlogFilename := path.Join(app.Config.Dump.Path, fmt.Sprintf("input.%s", filenamePostfix))

	// start dumpers
	logger.Info("start cache dump", zap.String("filename", dumpFilename))
	logger.Info("start wal write", zap.String("filename", xlogFilename))

	// open dump file
	dump, err := os.Create(dumpFilename)
	if err != nil {
		return err
	}
	dumpWriter := bufio.NewWriterSize(dump, 1048576) // 1Mb

	// start input dumper
	xlog, err := os.Create(xlogFilename)
	if err != nil {
		return err
	}
	xlogWriter := &SyncWriter{w: bufio.NewWriterSize(xlog, 4096)} // 4kb

	app.Cache.DivertToXlog(xlogWriter)

	// stop cache
	dumpStart := time.Now()
	cacheSize := app.Cache.Size()

	// dump cache
	err = app.Cache.DumpBinary(dumpWriter)
	if err != nil {
		logger.Info("dump failed", zap.Error(err))
		return err
	}

	logger.Info("cache dump finished",
		zap.Int("records", int(cacheSize)),
		zap.Duration("runtime", time.Since(dumpStart)),
	)

	if err = dumpWriter.Flush(); err != nil {
		logger.Info("dump flush failed", zap.Error(err))
		return err
	}

	if err = dump.Close(); err != nil {
		logger.Info("dump close failed", zap.Error(err))
		return err
	}

	// cache dump finished

	logger.Info("dump finished")

	logger.Info("stop listeners")

	stopped := make(chan struct{})

	go func() {
		app.stopListeners()

		if err := xlogWriter.Flush(); err != nil {
			logger.Info("xlog flush failed", zap.Error(err))
			return
		}

		if err := xlog.Close(); err != nil {
			logger.Info("xlog close failed", zap.Error(err))
			return
		}

		close(stopped)
	}()

	select {
	case <-time.After(5 * time.Second):
		logger.Info("stop listeners timeout, force stop daemon")
	case <-stopped:
		logger.Info("listeners stopped")
	}

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

	files, err := ioutil.ReadDir(dumpDir)
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
		ticker := persister.NewThrottleTicker(rps)
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
