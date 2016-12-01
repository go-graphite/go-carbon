package carbon

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-carbon/persister"
	"github.com/lomik/go-carbon/points"
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

	logrus.Info("grace stop with dump inited")

	filenamePostfix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	dumpFilename := path.Join(app.Config.Dump.Path, fmt.Sprintf("cache.%s", filenamePostfix))
	xlogFilename := path.Join(app.Config.Dump.Path, fmt.Sprintf("input.%s", filenamePostfix))

	// start dumpers
	logrus.Infof("start cache dump to %s", dumpFilename)
	logrus.Infof("start input dump to %s", xlogFilename)

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
	xlogWriter := &SyncWriter{w: bufio.NewWriterSize(xlog, 1048576)} // 1Mb

	app.Cache.DivertToXlog(xlogWriter)

	// stop cache
	dumpStart := time.Now()

	logrus.WithFields(logrus.Fields{
		"size": app.Cache.Size(),
	}).Info("[cache] start dump")

	// dump cache
	err = app.Cache.Dump(dumpWriter)
	if err != nil {
		return err
	}

	dumpWorktime := time.Since(dumpStart)
	logrus.WithFields(logrus.Fields{
		"time": dumpWorktime.String(),
	}).Info("[cache] finish dump")

	if err = dumpWriter.Flush(); err != nil {
		return err
	}

	if err = dump.Close(); err != nil {
		return err
	}

	// cache dump finished
	logrus.Info("stop listeners")
	app.stopListeners()

	if err = xlogWriter.Flush(); err != nil {
		return err
	}

	if err = xlog.Close(); err != nil {
		return err
	}

	logrus.Info("dump finished")

	logrus.Info("stop all")
	app.stopAll()

	return nil
}

// RestoreFromFile read and parse data from single file
func RestoreFromFile(filename string, storeFunc func(*points.Points)) error {
	var pointsCount int
	startTime := time.Now()

	logrus.Infof("[restore] start %s", filename)

	defer func() {
		finishTime := time.Now()

		logrus.
			WithField("points", pointsCount).
			WithField("time", finishTime.Sub(startTime).String()).
			Infof("[restore] finish %s", filename)
	}()

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1024*1024)

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if len(line) > 0 {
			p, err := points.ParseText(string(line))

			if err != nil {
				logrus.Warnf("[restore] wrong message %#v", string(line))
			} else {
				pointsCount++
				storeFunc(p)
			}
		}
	}

	return nil
}

// RestoreFromDir cache and input dumps from disk to memory
func RestoreFromDir(dumpDir string, storeFunc func(*points.Points)) {
	startTime := time.Now()
	defer func() {
		finishTime := time.Now()
		logrus.WithField("time", finishTime.Sub(startTime).String()).Info("[restore] finished")
	}()

	files, err := ioutil.ReadDir(dumpDir)
	if err != nil {
		logrus.Errorf("readdir %s failed: %s", dumpDir, err.Error())
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
		logrus.Infof("[restore] nothing to do from %s", dumpDir)
		return
	}

	sort.Strings(list)

	for index, fileWithSortPrefix := range list {
		list[index] = strings.SplitN(fileWithSortPrefix, ":", 2)[1]
	}

	logrus.WithField("files", list).Infof("[restore] start from %s", dumpDir)

	for _, fn := range list {
		filename := path.Join(dumpDir, fn)
		err := RestoreFromFile(filename, storeFunc)
		if err != nil {
			logrus.Errorf("[restore] read %s failed: %s", filename, err.Error())
		}

		err = os.Remove(filename)
		if err != nil {
			logrus.Errorf("[restore] remove %s failed: %s", filename, err.Error())
		}
	}
}

// Restore from dump.path
func (app *App) Restore(storeFunc func(*points.Points), path string, rps int) {
	if rps > 0 {
		ticker := persister.NewThrottleTicker(rps)
		defer ticker.Stop()

		throttledStoreFunc := func(p *points.Points) {
			<-ticker.C
			storeFunc(p)
		}

		RestoreFromDir(path, throttledStoreFunc)
	} else {
		RestoreFromDir(path, storeFunc)
	}
}
