package carbon

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/carbonserver"
	"github.com/lomik/go-carbon/persister"
	"github.com/lomik/go-carbon/receiver"
)

type App struct {
	sync.RWMutex
	ConfigFilename string
	Config         *Config
	Cache          *cache.Cache
	UDP            receiver.Receiver
	TCP            receiver.Receiver
	Pickle         receiver.Receiver
	CarbonLink     *cache.CarbonlinkListener
	Persister      *persister.Whisper
	Carbonserver   *carbonserver.CarbonserverListener
	Collector      *Collector // (!!!) Should be re-created on every change config/modules
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

	if cfg.Common.MetricEndpoint == "" {
		cfg.Common.MetricEndpoint = MetricEndpointLocal
	}

	if cfg.Common.MetricEndpoint != MetricEndpointLocal {
		u, err := url.Parse(cfg.Common.MetricEndpoint)

		if err != nil {
			return fmt.Errorf("common.metric-endpoint parse error: %s", err.Error())
		}

		if u.Scheme != "tcp" && u.Scheme != "udp" {
			return fmt.Errorf("common.metric-endpoint supports only tcp and udp protocols. %#v is unsupported", u.Scheme)
		}
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

	runtime.GOMAXPROCS(app.Config.Common.MaxCPU)

	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}
	app.startPersister()

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
	}

	app.Collector = NewCollector(app)

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

	if app.Carbonserver != nil {
		app.Carbonserver.Stop()
		app.Carbonserver = nil
		logrus.Debug("[carbonserver] finished")
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

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
		logrus.Debug("[stat] finished")
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

func (app *App) startPersister() {
	if app.Config.Whisper.Enabled {
		p := persister.NewWhisper(
			app.Config.Whisper.DataDir,
			app.Config.Whisper.Schemas,
			app.Config.Whisper.Aggregation,
			app.Cache.WriteoutQueue().GetNotConfirmed,
			app.Cache.Confirm,
		)
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

	runtime.GOMAXPROCS(conf.Common.MaxCPU)

	core := cache.New()
	core.SetMaxSize(conf.Cache.MaxSize)
	core.SetWriteStrategy(conf.Cache.WriteStrategy)

	app.Cache = core

	/* WHISPER start */
	app.startPersister()
	/* WHISPER end */

	/* UDP start */
	if conf.Udp.Enabled {
		app.UDP, err = receiver.New(
			"udp://"+conf.Udp.Listen,
			receiver.OutFunc(core.Add),
			receiver.UDPLogIncomplete(conf.Udp.LogIncomplete),
			receiver.BufferSize(conf.Udp.BufferSize),
		)

		if err != nil {
			return
		}
	}
	/* UDP end */

	/* TCP start */
	if conf.Tcp.Enabled {
		app.TCP, err = receiver.New(
			"tcp://"+conf.Tcp.Listen,
			receiver.OutFunc(core.Add),
			receiver.BufferSize(conf.Tcp.BufferSize),
		)

		if err != nil {
			return
		}
	}
	/* TCP end */

	/* PICKLE start */
	if conf.Pickle.Enabled {
		app.Pickle, err = receiver.New(
			"pickle://"+conf.Pickle.Listen,
			receiver.OutFunc(core.Add),
			receiver.PickleMaxMessageSize(uint32(conf.Pickle.MaxMessageSize)),
			receiver.BufferSize(conf.Pickle.BufferSize),
		)

		if err != nil {
			return
		}
	}
	/* PICKLE end */

	/* CARBONLINK start */
	if conf.Carbonserver.Enabled {
		if err != nil {
			return
		}

		carbonserver := carbonserver.NewCarbonserverListener(core.Get)
		carbonserver.SetWhisperData(conf.Whisper.DataDir)
		carbonserver.SetMaxGlobs(conf.Carbonserver.MaxGlobs)
		carbonserver.SetBuckets(conf.Carbonserver.Buckets)
		carbonserver.SetMetricsAsCounters(conf.Carbonserver.MetricsAsCounters)
		carbonserver.SetScanFrequency(conf.Carbonserver.ScanFrequency.Value())
		carbonserver.SetReadTimeout(conf.Carbonserver.ReadTimeout.Value())
		carbonserver.SetWriteTimeout(conf.Carbonserver.WriteTimeout.Value())
		// carbonserver.SetQueryTimeout(conf.Carbonserver.QueryTimeout.Value())

		if err = carbonserver.Listen(conf.Carbonserver.Listen); err != nil {
			return
		}

		app.Carbonserver = carbonserver
	}
	/* CARBONLINK end */

	/* CARBONLINK start */
	if conf.Carbonlink.Enabled {
		var linkAddr *net.TCPAddr
		linkAddr, err = net.ResolveTCPAddr("tcp", conf.Carbonlink.Listen)
		if err != nil {
			return
		}

		carbonlink := cache.NewCarbonlinkListener(core)
		carbonlink.SetReadTimeout(conf.Carbonlink.ReadTimeout.Value())
		// carbonlink.SetQueryTimeout(conf.Carbonlink.QueryTimeout.Value())

		if err = carbonlink.Listen(linkAddr); err != nil {
			return
		}

		app.CarbonLink = carbonlink
	}
	/* CARBONLINK end */

	/* RESTORE start */
	if conf.Dump.Enabled {
		go app.Restore(core.Add, conf.Dump.Path, conf.Dump.RestorePerSecond)
	}
	/* RESTORE end */

	/* COLLECTOR start */
	app.Collector = NewCollector(app)
	/* COLLECTOR end */

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
