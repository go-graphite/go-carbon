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
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

// GraceStop implements gracefully stop. Close all listening sockets, flush cache, stop application
func (app *App) GraceStop() {

	// grace stop with dump cache and input
	if app.Config.Dump.Enabled {
		err := app.GraceStopDump()
		if err != nil {
			logrus.Fatal(err)
		}

		app.stopAll()

		return
	}

	app.Lock()
	defer app.Unlock()

	logrus.Info("grace stop inited")

	app.stopListeners()

	// Flush cache
	if app.Cache != nil && app.Persister != nil {

		if app.Persister.GetMaxUpdatesPerSecond() > 0 {
			logrus.Debug("[persister] stop old throttled persister, start new unlimited")
			app.Persister.Stop()
			logrus.Debug("[persister] old persister finished")
			app.Persister.SetMaxUpdatesPerSecond(0)
			app.Persister.Start()
			logrus.Debug("[persister] new persister started")
		}
		// @TODO: disable throttling in persister

		flushStart := time.Now()

		logrus.WithFields(logrus.Fields{
			"size": app.Cache.Size(),
		}).Info("[cache] start flush")

		checkTicker := time.NewTicker(10 * time.Millisecond)
		defer checkTicker.Stop()

		statTicker := time.NewTicker(time.Second)
		defer statTicker.Stop()

	FlushLoop:
		for {
			select {
			case <-checkTicker.C:
				if int(app.Cache.Size()) == 0 {
					break FlushLoop
				}
			case <-statTicker.C:
				logrus.WithFields(logrus.Fields{
					"size": app.Cache.Size(),
				}).Info("[cache] flush checkpoint")
			}
		}

		flushWorktime := time.Now().Sub(flushStart)
		logrus.WithFields(logrus.Fields{
			"time": flushWorktime.String(),
		}).Info("[cache] finish flush")
	}

	app.stopAll()
}

// GraceStopDump implements gracefully stop:
// * Start writing all new data to xlogs
// * Stop cache worker
// * Dump all cache to file
// * Stop listeners
// * Close xlogs
// * Exit application
func (app *App) GraceStopDump() error {
	app.Lock()
	defer app.Unlock()

	logrus.Info("grace stop with dump inited")

	filenamePostfix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	snapFilename := path.Join(app.Config.Dump.Path, fmt.Sprintf("cache.%s", filenamePostfix))
	xlogFilename := path.Join(app.Config.Dump.Path, fmt.Sprintf("input.%s", filenamePostfix))

	// start dumpers
	logrus.Infof("start cache dump to %s", snapFilename)
	logrus.Infof("start input dump to %s", xlogFilename)

	// open snap file
	snap, err := os.Create(snapFilename)
	if err != nil {
		return err
	}
	snapWriter := bufio.NewWriterSize(snap, 1048576) // 1Mb

	// start input dumper
	xlog, err := os.Create(xlogFilename)
	if err != nil {
		return err
	}
	xlogWriter := bufio.NewWriterSize(xlog, 1048576) // 1Mb
	app.Cache.DivertToXlog(xlogWriter)

	// stop cache
	logrus.Info("[cache] stop worker")
	dumpStart := time.Now()

	logrus.Info("stop listeners")
	app.stopListeners()

	app.Cache.Stop()
	cacheOut := app.Cache.Out()
	close(cacheOut)

	logrus.WithFields(logrus.Fields{
		"size":    app.Cache.Size(),
		"outSize": len(cacheOut),
	}).Info("[cache] start dump")

	// dump points from cache.out channel
	points.Glue(nil, cacheOut, 65536, time.Second, func(b []byte) {
		snapWriter.Write(b)
	})

	// dump cache
	err = app.Cache.Dump(snapWriter)
	if err != nil {
		return err
	}

	// cache dump finished
	logrus.Info("stop listeners")
	app.stopListeners()

	dumpWorktime := time.Now().Sub(dumpStart)
	logrus.WithFields(logrus.Fields{
		"time": dumpWorktime.String(),
	}).Info("[cache] finish dump")

	if err = snapWriter.Flush(); err != nil {
		return err
	}

	if err = snap.Close(); err != nil {
		return err
	}

	if err = xlogWriter.Flush(); err != nil {
		return err
	}

	if err = xlog.Close(); err != nil {
		return err
	}

	logrus.Info("dump finished")

	return nil
}

// RestoreFromFile read and parse data from single file
func RestoreFromFile(filename string, cache *cache.Cache, lim *helper.Ratelimiter, rps int) error {
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
				lim.TickSleep(rps)
				cache.AddSinglePoint(p)
			}
		}
	}

	return nil
}

// RestoreFromDir cache and input dumps from disk to memory
func RestoreFromDir(dumpDir string, cache *cache.Cache, rps int) {
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

	limiter := helper.NewRatelimiter()

	for _, fn := range list {
		filename := path.Join(dumpDir, fn)
		err := RestoreFromFile(filename, cache, &limiter, rps)
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
func (app *App) Restore(cache *cache.Cache, path string, rps int) {
	RestoreFromDir(path, cache, rps)
}
