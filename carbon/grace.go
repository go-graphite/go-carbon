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

	"github.com/Sirupsen/logrus"
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
			"size":     app.Cache.Size(),
			"inputLen": len(app.Cache.In()),
		}).Info("[cache] start flush")

		checkTicker := time.NewTicker(10 * time.Millisecond)
		defer checkTicker.Stop()

		statTicker := time.NewTicker(time.Second)
		defer statTicker.Stop()

	FlushLoop:
		for {
			select {
			case <-checkTicker.C:
				if app.Cache.Size()+len(app.Cache.In()) == 0 {
					break FlushLoop
				}
			case <-statTicker.C:
				logrus.WithFields(logrus.Fields{
					"size":     app.Cache.Size(),
					"inputLen": len(app.Cache.In()),
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

	xlogExit := make(chan bool)

	var xlogWg sync.WaitGroup
	xlogWg.Add(1)

	go func() {
		points.Glue(xlogExit, app.Cache.In(), 65536, time.Second, func(b []byte) {
			xlogWriter.Write(b)
		})
		xlogWg.Done()
	}()

	// stop cache
	logrus.Info("[cache] stop worker")
	dumpStart := time.Now()

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

	// cache dump finished
	logrus.Info("stop listeners")
	app.stopListeners()

	// wait for all data from input channel written
	checkTicker := time.NewTicker(10 * time.Millisecond)
	defer checkTicker.Stop()

	statTicker := time.NewTicker(time.Second)
	defer statTicker.Stop()

FlushLoop:
	for {
		select {
		case <-checkTicker.C:
			if len(app.Cache.In()) == 0 {
				break FlushLoop
			}
		case <-statTicker.C:
			logrus.WithFields(logrus.Fields{
				"inputLen": len(app.Cache.In()),
			}).Info("[cache] wait input")
		}
	}

	// stop xlog writer
	close(xlogExit)
	xlogWg.Wait()

	if err = xlogWriter.Flush(); err != nil {
		return err
	}

	if err = xlog.Close(); err != nil {
		return err
	}

	logrus.Info("dump finished")

	return nil
}

// recover cache and input dumps from disk to memory
func Recover(path string, out chan *points.Points) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		logrus.Errorf("readdir %s failed: %s", path, err.Error())
		return
	}

	// read files and lazy sorting
	list := make([]string, 0)

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
		}

		list = append(list, fileWithSortPrefix)
	}

	if len(list) == 0 {
		logrus.Infof("nothing to recover from %s", path)
		return
	}

	sort.Strings(list)

	for index, fileWithSortPrefix := range list {
		list[index] = strings.SplitN(fileWithSortPrefix, ":", 2)[1]
	}

	logrus.WithField("files", list).Infof("start recover from %s", path)
}
