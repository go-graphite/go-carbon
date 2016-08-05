package carbon

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/lomik/go-carbon/points"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/persister"
	"github.com/lomik/go-carbon/receiver"
)

type App struct {
	sync.RWMutex
	ConfigFilename string
	Config         *Config
	Cache          *cache.Cache
	UDP            *receiver.UDP
	TCP            *receiver.TCP
	Pickle         *receiver.TCP
	CarbonLink     *cache.CarbonlinkListener
	Persister      *persister.Whisper
	exit           chan bool
}

// New App instance
func New(configFilename string) *App {
	app := &App{
		ConfigFilename: configFilename,
		Config:         NewConfig(),
		exit:           make(chan bool),
	}
	return app
}

// configure loads config from config file, schemas.conf, aggregation.conf
func (app *App) configure() error {
	var err error

	cfg := NewConfig()
	if err := ParseConfig(app.ConfigFilename, cfg); err != nil {
		return err
	}

	// carbon-cache prefix
	if hostname, err := os.Hostname(); err == nil {
		hostname = strings.Replace(hostname, ".", "_", -1)
		cfg.Common.GraphPrefix = strings.Replace(cfg.Common.GraphPrefix, "{host}", hostname, -1)
	} else {
		cfg.Common.GraphPrefix = strings.Replace(cfg.Common.GraphPrefix, "{host}", "localhost", -1)
	}

	if cfg.Whisper.Enabled {
		cfg.Whisper.Schemas, err = persister.ReadWhisperSchemas(cfg.Whisper.SchemasFilename)
		if err != nil {
			return err
		}

		if cfg.Whisper.AggregationFilename != "" {
			cfg.Whisper.Aggregation, err = persister.ReadWhisperAggregation(cfg.Whisper.AggregationFilename)
			if err != nil {
				return err
			}
		} else {
			cfg.Whisper.Aggregation = persister.NewWhisperAggregation()
		}
	}
	if !(cfg.Cache.WriteStrategy == "max" ||
		cfg.Cache.WriteStrategy == "sorted" ||
		cfg.Cache.WriteStrategy == "noop") {
		return fmt.Errorf("go-carbon support only \"max\", \"sorted\"  or \"noop\" write-strategy")
	}

	app.Config = cfg

	return nil
}

// ParseConfig loads config from config file, schemas.conf, aggregation.conf
func (app *App) ParseConfig() error {
	app.Lock()
	defer app.Unlock()

	return app.configure()
}

// ReloadConfig reloads some settings from config
func (app *App) ReloadConfig() error {
	app.Lock()
	defer app.Unlock()

	var err error
	if err = app.configure(); err != nil {
		return err
	}

	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}
	app.startPersister()

	return nil
}

// Stop all socket listeners
func (app *App) stopListeners() {
	if app.TCP != nil {
		app.TCP.Stop()
		app.TCP = nil
		logrus.Debug("[tcp] finished")
	}

	if app.Pickle != nil {
		app.Pickle.Stop()
		app.Pickle = nil
		logrus.Debug("[pickle] finished")
	}

	if app.UDP != nil {
		app.UDP.Stop()
		app.UDP = nil
		logrus.Debug("[udp] finished")
	}

	if app.CarbonLink != nil {
		app.CarbonLink.Stop()
		app.CarbonLink = nil
		logrus.Debug("[carbonlink] finished")
	}
}

func (app *App) stopAll() {
	app.stopListeners()

	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
		logrus.Debug("[persister] finished")
	}

	if app.Cache != nil {
		app.Cache.Stop()
		app.Cache = nil
		logrus.Debug("[cache] finished")
	}

	if app.exit != nil {
		close(app.exit)
		app.exit = nil
		logrus.Debug("[app] close(exit)")
	}
}

// Stop force stop all components
func (app *App) Stop() {
	app.Lock()
	defer app.Unlock()
	app.stopAll()
}

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

	filenamePostfix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().Unix())
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

func (app *App) startPersister() {
	if app.Config.Whisper.Enabled {
		p := persister.NewWhisper(
			app.Config.Whisper.DataDir,
			app.Config.Whisper.Schemas,
			app.Config.Whisper.Aggregation,
			app.Cache.Out(),
			app.Cache.Confirm(),
		)
		p.SetGraphPrefix(app.Config.Common.GraphPrefix)
		p.SetMetricInterval(app.Config.Common.MetricInterval.Value())
		p.SetMaxUpdatesPerSecond(app.Config.Whisper.MaxUpdatesPerSecond)
		p.SetSparse(app.Config.Whisper.Sparse)
		p.SetWorkers(app.Config.Whisper.Workers)

		p.Start()

		app.Persister = p
	}
}

// Start starts
func (app *App) Start() (err error) {
	app.Lock()
	defer app.Unlock()

	defer func() {
		if err != nil {
			app.stopAll()
		}
	}()

	conf := app.Config

	core := cache.New()
	core.SetGraphPrefix(conf.Common.GraphPrefix)
	core.SetMetricInterval(conf.Common.MetricInterval.Value())
	core.SetMaxSize(conf.Cache.MaxSize)
	core.SetInputCapacity(conf.Cache.InputBuffer)
	core.SetWriteStrategy(conf.Cache.WriteStrategy)
	core.Start()

	app.Cache = core

	/* WHISPER start */
	app.startPersister()
	/* WHISPER end */

	/* UDP start */
	if conf.Udp.Enabled {
		var udpAddr *net.UDPAddr

		udpAddr, err = net.ResolveUDPAddr("udp", conf.Udp.Listen)
		if err != nil {
			return
		}

		udpListener := receiver.NewUDP(core.In())
		udpListener.SetGraphPrefix(fmt.Sprintf("%sudp.", conf.Common.GraphPrefix))
		udpListener.SetMetricInterval(conf.Common.MetricInterval.Value())

		if conf.Udp.LogIncomplete {
			udpListener.SetLogIncomplete(true)
		}

		err = udpListener.Listen(udpAddr)
		if err != nil {
			return
		}

		app.UDP = udpListener
	}
	/* UDP end */

	/* TCP start */
	if conf.Tcp.Enabled {
		var tcpAddr *net.TCPAddr
		tcpAddr, err = net.ResolveTCPAddr("tcp", conf.Tcp.Listen)
		if err != nil {
			return
		}

		tcpListener := receiver.NewTCP(core.In())
		tcpListener.SetGraphPrefix(fmt.Sprintf("%stcp.", conf.Common.GraphPrefix))
		tcpListener.SetMetricInterval(conf.Common.MetricInterval.Value())

		if err = tcpListener.Listen(tcpAddr); err != nil {
			return
		}

		app.TCP = tcpListener
	}
	/* TCP end */

	/* PICKLE start */

	if conf.Pickle.Enabled {
		var pickleAddr *net.TCPAddr
		pickleAddr, err = net.ResolveTCPAddr("tcp", conf.Pickle.Listen)
		if err != nil {
			return
		}

		pickleListener := receiver.NewPickle(core.In())
		pickleListener.SetGraphPrefix(fmt.Sprintf("%spickle.", conf.Common.GraphPrefix))
		pickleListener.SetMetricInterval(conf.Common.MetricInterval.Value())
		pickleListener.SetMaxPickleMessageSize(uint32(conf.Pickle.MaxMessageSize))

		if err = pickleListener.Listen(pickleAddr); err != nil {
			return
		}

		app.Pickle = pickleListener
	}
	/* PICKLE end */

	/* CARBONLINK start */
	if conf.Carbonlink.Enabled {
		var linkAddr *net.TCPAddr
		linkAddr, err = net.ResolveTCPAddr("tcp", conf.Carbonlink.Listen)
		if err != nil {
			return
		}

		carbonlink := cache.NewCarbonlinkListener(core.Query())
		carbonlink.SetReadTimeout(conf.Carbonlink.ReadTimeout.Value())
		carbonlink.SetQueryTimeout(conf.Carbonlink.QueryTimeout.Value())

		if err = carbonlink.Listen(linkAddr); err != nil {
			return
		}

		app.CarbonLink = carbonlink
	}
	/* CARBONLINK end */

	return
}

// Loop ...
func (app *App) Loop() {
	app.RLock()
	exitChan := app.exit
	app.RUnlock()

	if exitChan != nil {
		<-app.exit
	}
}
