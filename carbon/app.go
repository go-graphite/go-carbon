package carbon

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/api"
	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/tags"
	"github.com/lomik/zapwriter"

	// receivers
	http_receiver "github.com/go-graphite/go-carbon/receiver/http"
	kafka_receiver "github.com/go-graphite/go-carbon/receiver/kafka"
	pubsub_receiver "github.com/go-graphite/go-carbon/receiver/pubsub"
	tcp_receiver "github.com/go-graphite/go-carbon/receiver/tcp"
	udp_receiver "github.com/go-graphite/go-carbon/receiver/udp"
)

type NamedReceiver struct {
	receiver.Receiver
	Name string
}

type wspConfigRetriever struct {
	getRetentionFunc func(string) (int, bool)
	getAggrNameFunc  func(string) (string, float64, bool)
}

func (r *wspConfigRetriever) MetricRetentionPeriod(metric string) (int, bool) {
	return r.getRetentionFunc(metric)
}

func (r *wspConfigRetriever) MetricAggrConf(metric string) (string, float64, bool) {
	return r.getAggrNameFunc(metric)
}

type App struct {
	sync.RWMutex
	ConfigFilename string
	Config         *Config
	Api            *api.Api
	Cache          *cache.Cache
	Receivers      []*NamedReceiver
	CarbonLink     *cache.CarbonlinkListener
	Persister      *persister.Whisper
	Carbonserver   *carbonserver.CarbonserverListener
	MetricStore    *store.Store
	Buckyd         *buckyd.Service
	Tags           *tags.Tags
	Collector      *Collector // (!!!) Should be re-created on every change config/modules
	PromRegisterer prometheus.Registerer
	PromRegistry   *prometheus.Registry
	exit           chan bool
	FlushTraces    func()
	// readRelease, when set, lets the next instance start while this one keeps
	// serving reads after a dump stop (see SetReadHandoff).
	readRelease   func()
	pendingStop   chan struct{}
	pendingDone   chan struct{}
	readSuccessor string
	handoffClaim  *handoff.Claim

	quotaEstimateConfig atomic.Value // *Config, immutable estimator snapshot
	quotaReloader       *quotaReloader
	metricStoreIndex    *metricIndexRefresher
	expirer             *metricExpirer
	expirationStats     *expirationStats
	storeRestoreDone    <-chan struct{}
}

var registerPluginsOnce sync.Once

// New App instance
func New(configFilename string) *App {
	app := &App{
		ConfigFilename: configFilename,
		Config:         NewConfig(),
		PromRegistry:   prometheus.NewPedanticRegistry(),
		exit:           make(chan bool),
	}

	// Register all receivers explicitly
	registerPluginsOnce.Do(func() {
		http_receiver.Register()
		kafka_receiver.Register()
		pubsub_receiver.Register()
		tcp_receiver.Register()
		udp_receiver.Register()
	})

	return app
}

// configure loads config from config file, schemas.conf, aggregation.conf
func (app *App) configure() error {
	cfg, err := loadConfig(app.ConfigFilename)
	if err != nil {
		return err
	}
	if app.Cache != nil && storageSettingsChanged(app.Config, cfg) {
		return errors.New("storage and buckyd settings require a restart")
	}
	// Serialize publication with the file poller, which never takes app.Lock.
	if app.quotaReloader != nil {
		app.quotaReloader.mu.Lock()
		defer app.quotaReloader.mu.Unlock()
	}
	if app.Carbonserver != nil {
		if cfg.Carbonserver.QuotaUsageReportFrequency.Value() != app.Config.Carbonserver.QuotaUsageReportFrequency.Value() {
			return errors.New("changing quota-usage-report-frequency requires a restart")
		}
		if err := app.Carbonserver.ReloadQuotas(cfg.getCarbonserverQuotas(cfg.Carbonserver.QuotaUsageReportFrequency.Value())); err != nil {
			return err
		}
		app.quotaEstimateConfig.Store(cfg)
	}
	// All fallible policy validation is complete. Join the old expiration
	// worker before publishing the new policy, so reload cannot leave deletes
	// running under an obsolete rule. ReloadConfig restarts it below.
	app.stopExpiration()
	app.Config = cfg
	if app.quotaReloader != nil {
		app.quotaReloader.configureLocked(cfg)
	}

	return nil
}

func loadConfig(filename string) (*Config, error) {
	cfg, err := ReadConfig(filename)
	if err != nil {
		return nil, err
	}
	if err := validateStorageConfig(cfg); err != nil {
		return nil, err
	}
	if err := validateQuotaReloadConfig(cfg); err != nil {
		return nil, err
	}

	// carbon-cache prefix
	if hostname, err := os.Hostname(); err == nil {
		hostname = strings.ReplaceAll(hostname, ".", "_")
		cfg.Common.GraphPrefix = strings.ReplaceAll(cfg.Common.GraphPrefix, "{host}", hostname)
	} else {
		cfg.Common.GraphPrefix = strings.ReplaceAll(cfg.Common.GraphPrefix, "{host}", "localhost")
	}

	if err := loadWhisperConfig(cfg); err != nil {
		return nil, err
	}
	if err := loadExpirationConfig(cfg); err != nil {
		return nil, err
	}
	if err := validateCacheConfig(cfg); err != nil {
		return nil, err
	}
	if err := validateMetricEndpoint(cfg); err != nil {
		return nil, err
	}
	if err := validateStorageConfig(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadWhisperConfig(cfg *Config) error {
	var err error
	if cfg.Whisper.Enabled {
		cfg.Whisper.Schemas, err = persister.ReadWhisperSchemas(cfg.Whisper.SchemasFilename)
		if err != nil {
			return err
		}

		if cfg.Whisper.QuotasFilename != "" {
			cfg.Whisper.Quotas, err = persister.ReadWhisperQuotas(cfg.Whisper.QuotasFilename)
			if err != nil {
				return err
			}
		}

		if cfg.Whisper.AggregationFilename != "" {
			cfg.Whisper.Aggregation, err = persister.ReadWhisperAggregation(cfg.Whisper.AggregationFilename)
			if err != nil {
				return err
			}
		} else {
			cfg.Whisper.Aggregation = persister.NewWhisperAggregation()
		}

		if cfg.Whisper.OutOfOrder && cfg.Whisper.StorageBackend == "files" {
			return validateOutOfOrderConfig(cfg)
		}
	}
	return nil
}

func validateOutOfOrderConfig(cfg *Config) error {
	// the uncompressed format already writes points in any order
	if !cfg.Whisper.Compressed && !cfg.Whisper.Schemas.AnyCompressed() {
		return fmt.Errorf("whisper.out-of-order requires whisper.compressed, or a schema with compressed = true")
	}
	if cfg.Whisper.OutOfOrderCompactRate <= 0 {
		return fmt.Errorf("whisper.out-of-order-compact-rate must be positive, got %d", cfg.Whisper.OutOfOrderCompactRate)
	}
	if cfg.Whisper.OutOfOrderCompactMinPoints < 0 {
		return fmt.Errorf("whisper.out-of-order-compact-min-points must not be negative")
	}
	if cfg.Whisper.OutOfOrderCompactMinPoints > 0 &&
		(cfg.Whisper.OutOfOrderCompactMaxPointAge.Value() <= 0 || cfg.Whisper.OutOfOrderCompactRetentionMargin.Value() <= 0) {
		return fmt.Errorf("point-based out-of-order compaction requires positive max-point-age and retention-margin")
	}
	if cfg.Whisper.OutOfOrderCompactMinPoints == 0 && cfg.Whisper.OutOfOrderCompactThreshold <= 0 {
		return fmt.Errorf("whisper.out-of-order-compact-threshold must be positive, got %d", cfg.Whisper.OutOfOrderCompactThreshold)
	}
	return nil
}

func validateCacheConfig(cfg *Config) error {
	if !(cfg.Cache.WriteStrategy == "max" ||
		cfg.Cache.WriteStrategy == "sorted" ||
		cfg.Cache.WriteStrategy == "noop") {
		return fmt.Errorf("go-carbon support only \"max\", \"sorted\"  or \"noop\" write-strategy")
	}

	if cfg.Cache.WriteoutMinPoints < 0 || cfg.Cache.WriteoutMaxDelay.Value() < 0 ||
		(cfg.Cache.WriteoutMinPoints == 0) != (cfg.Cache.WriteoutMaxDelay.Value() == 0) {
		return fmt.Errorf("cache.writeout-min-points and cache.writeout-max-delay must both be positive or both zero")
	}

	return nil
}

func validateMetricEndpoint(cfg *Config) error {
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

	return nil
}

// ParseConfig loads config from config file, schemas.conf, aggregation.conf
func (app *App) ParseConfig() error {
	app.Lock()
	defer app.Unlock()

	err := app.configure()

	if app.PromRegisterer == nil {
		app.PromRegisterer = prometheus.WrapRegistererWith(
			prometheus.Labels(app.Config.Prometheus.Labels),
			app.PromRegistry,
		)
	}

	return err
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

	app.Cache.SetMaxSize(app.Config.Cache.MaxSize)
	app.Cache.SetWriteStrategy(app.Config.Cache.WriteStrategy)
	app.Cache.SetTagsEnabled(app.Config.Tags.Enabled)
	app.Cache.SetBloomSize(app.Config.Cache.BloomSize)
	if err := app.Cache.SetWriteoutBatching(app.Config.Cache.WriteoutMinPoints, app.Config.Cache.WriteoutMaxDelay.Value()); err != nil {
		return err
	}

	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
	}

	if app.Tags != nil {
		app.Tags.Stop()
		app.Tags = nil
	}

	app.startPersister()
	app.startExpiration()

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
	}

	app.Collector = NewCollector(app)

	return nil
}

// stopListeners stops input and then starts draining reads. The caller must wait for
// the returned channel before exiting or closing storage used by active reads.
// Assumes we are holding app.Lock().
func (app *App) stopListeners() <-chan struct{} {
	app.stopInputListeners()
	return app.stopReadListeners()
}

// Keep reads available while receivers and other input cleanup finish.
// Assumes we are holding app.Lock().
func (app *App) stopInputListeners() {
	logger := zapwriter.Logger("app")
	if app.Receivers != nil {
		for i := 0; i < len(app.Receivers); i++ {
			app.Receivers[i].Stop()
			logger.Debug("receiver stopped", zap.String("name", app.Receivers[i].Name))
		}
		app.Receivers = nil
	}
	if app.FlushTraces != nil {
		app.FlushTraces()
		logger.Debug("traces flushed")
	}
}

// Assumes we are holding app.Lock().
func (app *App) stopReadListeners() <-chan struct{} {
	logger := zapwriter.Logger("app")
	readsStopped := make(chan struct{})
	app.stopExpiration()
	if app.quotaReloader != nil {
		app.quotaReloader.close()
		app.quotaReloader = nil
	}

	if app.Api != nil {
		app.Api.Stop()
		app.Api = nil
		logger.Debug("api stopped")
	}

	if app.CarbonLink != nil {
		app.CarbonLink.Stop()
		app.CarbonLink = nil
		logger.Debug("carbonlink stopped")
	}

	// Finish transfers and the catalog scan worker before closing the index.
	if app.Buckyd != nil {
		if err := app.Buckyd.Stop(); err != nil {
			logger.Error("stop buckyd", zap.Error(err))
		}
		app.Buckyd = nil
	}
	if app.metricStoreIndex != nil {
		app.metricStoreIndex.close()
		app.metricStoreIndex = nil
	}
	if app.Carbonserver != nil {
		carbonserver := app.Carbonserver
		go func() {
			defer close(readsStopped)
			carbonserver.Stop()
			logger.Debug("carbonserver stopped")
		}()
		app.Carbonserver = nil
	} else {
		close(readsStopped)
	}
	return readsStopped
}

func (app *App) stopAll() {
	if app.Cache != nil {
		_ = app.Cache.SetWriteoutBatching(0, 0)
	}
	<-app.stopListeners()

	logger := zapwriter.Logger("app")

	if app.Persister != nil {
		app.Persister.Stop()
		app.Persister = nil
		logger.Debug("persister stopped")
	}

	if app.Tags != nil {
		app.Tags.Stop()
		app.Tags = nil
		logger.Debug("tags stopped")
	}

	if app.Cache != nil {
		app.Cache.Stop()
		app.Cache = nil
		logger.Debug("cache stopped")
	}

	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
		logger.Debug("collector stopped")
	}
	if app.MetricStore != nil {
		if err := app.MetricStore.Close(); err != nil {
			logger.Error("close shared storage", zap.Error(err))
		}
		app.MetricStore = nil
	}

	if app.exit != nil {
		close(app.exit)
		app.exit = nil
		logger.Debug("close(exit)")
	}
}

// Stop force stop all components
func (app *App) Stop() {
	app.Lock()
	defer app.Unlock()
	app.stopAll()
}

func (app *App) startPersister() {
	if app.Config.Tags.Enabled {
		app.Tags = tags.New(&tags.Options{
			LocalPath:           app.Config.Tags.LocalDir,
			TagDB:               app.Config.Tags.TagDB,
			TagDBTimeout:        app.Config.Tags.TagDBTimeout.Value(),
			TagDBChunkSize:      app.Config.Tags.TagDBChunkSize,
			TagDBUpdateInterval: app.Config.Tags.TagDBUpdateInterval,
		})
	}

	if app.Config.Whisper.Enabled {
		p := persister.NewWhisper(
			app.Config.Whisper.DataDir,
			app.Config.Whisper.Schemas,
			app.Config.Whisper.Aggregation,
			app.Cache.WriteoutQueue().Get,
			app.Cache.PopNotConfirmed,
			app.Cache.Confirm,
			app.Cache.Pop,
		)
		p.SetRequeue(app.Cache.Requeue)
		p.SetWriteoutReady(app.Cache.WriteoutReady)
		if app.MetricStore != nil {
			p.SetMetricStore(app.MetricStore)
		}
		p.SetMaxUpdatesPerSecond(app.Config.Whisper.MaxUpdatesPerSecond)
		p.SetSparse(app.Config.Whisper.Sparse)
		p.SetFLock(app.Config.Whisper.FLock)
		p.SetCompressed(app.Config.Whisper.Compressed)
		p.SetRemoveEmptyFile(app.Config.Whisper.RemoveEmptyFile)
		p.SetWorkers(app.Config.Whisper.Workers)
		p.SetHashFilenames(app.Config.Whisper.HashFilenames)
		if app.Config.Prometheus.Enabled {
			p.InitPrometheus(app.PromRegisterer)
		}
		if app.Tags != nil {
			p.SetTagsEnabled(true)
			p.SetTaggedFn(app.Tags.Add)
		}

		if cfg := app.Config.Whisper; cfg.OnlineMigration {
			scope := strings.Split(strings.TrimSpace(cfg.OnlineMigrationGlobalScope), ",")
			p.EnableOnlineMigration(cfg.OnlineMigrationRate, scope)
		}

		if cfg := app.Config.Whisper; cfg.OutOfOrder {
			p.EnableOutOfOrder(cfg.OutOfOrderCompactRate, cfg.OutOfOrderCompactThreshold)
			p.SetOutOfOrderCompactionPolicy(cfg.OutOfOrderCompactMinPoints,
				cfg.OutOfOrderCompactMaxPointAge.Value(), cfg.OutOfOrderCompactRetentionMargin.Value())
		}

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
	if err = validateStorageConfig(conf); err != nil {
		return err
	}
	if err = validateQuotaReloadConfig(conf); err != nil {
		return err
	}

	runtime.GOMAXPROCS(conf.Common.MaxCPU)

	core, err := app.startStorage()
	if err != nil {
		return err
	}
	if err = app.startAPI(core); err != nil {
		return err
	}
	app.startPersister()
	restoreBeforeReceivers := conf.Dump.Enabled && conf.Whisper.Enabled &&
		(conf.Whisper.Compressed || conf.Whisper.Schemas.AnyCompressed())
	newMetricsChan, err := app.configureCarbonserver(core)
	if err != nil {
		return err
	}
	// Live input cannot advance compressed block watermarks before old history
	// drains. A validated checkpoint can serve reads throughout that drain.
	readsStarted := false
	if restoreBeforeReceivers {
		readsStarted, err = app.restoreWithPendingReads(core, newMetricsChan)
		if err != nil {
			return err
		}
		if !readsStarted {
			app.restoreBeforeReceivers(core)
		}
	}
	if !readsStarted {
		if err = app.listenCarbonserver(core, newMetricsChan); err != nil {
			return err
		}
	}
	if err = app.startReceivers(core); err != nil {
		return err
	}
	if err = app.startCarbonlink(core); err != nil {
		return err
	}
	return app.startBackgroundWorkers(core, restoreBeforeReceivers)
}

func (app *App) startBackgroundWorkers(core *cache.Cache, restoreBeforeReceivers bool) error {
	conf := app.Config
	if conf.Dump.Enabled && !restoreBeforeReceivers {
		restored := make(chan struct{})
		app.storeRestoreDone = restored
		go func() {
			defer close(restored)
			app.Restore(core.AddRestored, conf.Dump.Path, conf.Dump.RestorePerSecond)
		}()
	}
	if app.MetricStore != nil && app.Carbonserver != nil {
		app.metricStoreIndex = startMetricIndexRefresher(app.Carbonserver, metricStoreIndexRefreshInterval)
	}
	if err := app.startBuckyd(); err != nil {
		return err
	}
	app.startExpiration()
	app.Collector = NewCollector(app)
	if app.Carbonserver != nil {
		app.quotaReloader = startQuotaReloader(app.Config, app.Carbonserver)
	}
	return nil
}

func (app *App) startStorage() (core *cache.Cache, err error) {
	conf := app.Config
	app.storeRestoreDone = nil
	core = cache.New()
	core.SetMaxSize(conf.Cache.MaxSize)
	core.SetWriteStrategy(conf.Cache.WriteStrategy)
	core.SetTagsEnabled(conf.Tags.Enabled)
	core.SetBloomSize(conf.Cache.BloomSize)
	if err = core.SetWriteoutBatching(conf.Cache.WriteoutMinPoints, conf.Cache.WriteoutMaxDelay.Value()); err != nil {
		return nil, err
	}

	app.Cache = core
	if conf.Whisper.StorageBackend == "pebble-chunk" {
		app.MetricStore, err = store.Open(sharedStorePath(conf), store.Options{
			CacheSize: conf.Whisper.StoreCacheSize, MemTableSize: conf.Whisper.StoreMemTableSize,
			SyncInterval: conf.Whisper.StoreSyncInterval.Value(),
		})
		if err != nil {
			return nil, fmt.Errorf("open shared storage: %w", err)
		}
		app.expirationStats = &expirationStats{}
	}

	return core, nil
}

func (app *App) startAPI(core *cache.Cache) (err error) {
	conf := app.Config
	/* API start */
	if conf.Grpc.Enabled {
		var grpcAddr *net.TCPAddr
		grpcAddr, err = net.ResolveTCPAddr("tcp", conf.Grpc.Listen)
		if err != nil {
			return
		}

		grpcApi := api.New(core)

		if err = grpcApi.Listen(grpcAddr); err != nil {
			return
		}

		app.Api = grpcApi
	}
	/* API end */

	return nil
}

func (app *App) configureCarbonserver(core *cache.Cache) (newMetricsChan chan string, err error) {
	conf := app.Config
	if !conf.Carbonserver.Enabled {
		return nil, nil
	}

	if app.MetricStore == nil && (conf.Carbonserver.TrigramIndex || conf.Carbonserver.TrieIndex) {
		if fi, err := os.Lstat(conf.Whisper.DataDir); err != nil {
			return nil, fmt.Errorf("failed to stat whisper data directory: %w", err)
		} else if fi.Mode()&os.ModeSymlink == 1 {
			return nil, fmt.Errorf("whisper data directory is a symlink")
		}
	}

	apiPerPathRateLimiters, globQueryRateLimiters, err := app.carbonserverRateLimiters()
	if err != nil {
		return nil, err
	}

	// TODO: refactor: do not use var name the same as pkg name
	carbonserver := carbonserver.NewCarbonserverListener(core.Get)
	carbonserver.SetWhisperData(conf.Whisper.DataDir)
	if app.MetricStore != nil {
		carbonserver.SetWhisperData(sharedStorePath(conf))
	}
	carbonserver.SetMetricStore(app.MetricStore)
	carbonserver.SetMaxGlobs(conf.Carbonserver.MaxGlobs)
	carbonserver.SetEmptyResultOk(conf.Carbonserver.EmptyResultOk)
	carbonserver.SetDoNotLog404s(conf.Carbonserver.DoNotLog404s)
	carbonserver.SetFLock(app.Config.Whisper.FLock)
	if claim := app.handoffClaim; claim != nil {
		// A stopping instance offered its listener: take it, or, if that
		// fails, wait while it closes its copy instead of failing to bind.
		app.handoffClaim = nil
		carbonserver.SetListenerInheritor(func(addr *net.TCPAddr) (net.Listener, func() error, error) {
			return claim.Take(addr)
		})
	}
	carbonserver.SetCompressed(app.Config.Whisper.Compressed)
	carbonserver.SetRemoveEmptyFile(app.Config.Whisper.RemoveEmptyFile)
	carbonserver.SetFailOnMaxGlobs(conf.Carbonserver.FailOnMaxGlobs)
	carbonserver.SetMaxMetricsGlobbed(conf.Carbonserver.MaxMetricsGlobbed)
	carbonserver.SetMaxMetricsRendered(conf.Carbonserver.MaxMetricsRendered)
	carbonserver.SetMaxFetchDataGoroutines(conf.Carbonserver.MaxFetchDataGoroutines)
	carbonserver.SetBuckets(conf.Carbonserver.Buckets)
	carbonserver.SetMetricsAsCounters(conf.Carbonserver.MetricsAsCounters)
	carbonserver.SetScanFrequency(conf.Carbonserver.ScanFrequency.Value())
	carbonserver.SetQuotaUsageReportFrequency(conf.Carbonserver.QuotaUsageReportFrequency.Value())
	carbonserver.SetMaxCreatesPerSecond(conf.Carbonserver.MaxCreatesPerSecond)
	carbonserver.SetReadTimeout(conf.Carbonserver.ReadTimeout.Value())
	carbonserver.SetIdleTimeout(conf.Carbonserver.IdleTimeout.Value())
	carbonserver.SetWriteTimeout(conf.Carbonserver.WriteTimeout.Value())
	carbonserver.SetQueryCacheEnabled(conf.Carbonserver.QueryCacheEnabled)
	carbonserver.SetQueryCacheSizeMB(conf.Carbonserver.QueryCacheSizeMB)
	carbonserver.SetStreamingQueryCacheEnabled(conf.Carbonserver.StreamingQueryCacheEnabled)
	carbonserver.SetFindCacheEnabled(conf.Carbonserver.FindCacheEnabled)
	carbonserver.SetFindCacheSizeMB(conf.Carbonserver.FindCacheSizeMB)
	carbonserver.SetGlobCacheEnabled(conf.Carbonserver.GlobCacheEnabled)
	carbonserver.SetGlobCacheSizeMB(conf.Carbonserver.GlobCacheSizeMB)
	carbonserver.SetTrigramIndex(conf.Carbonserver.TrigramIndex)
	carbonserver.SetTrieIndex(conf.Carbonserver.TrieIndex)
	carbonserver.SetConcurrentIndex(conf.Carbonserver.ConcurrentIndex)
	carbonserver.SetFileListCache(conf.Carbonserver.FileListCache)
	carbonserver.SetFileListCacheVersion(conf.Carbonserver.FileListCacheVersion)
	carbonserver.SetInternalStatsDir(conf.Carbonserver.InternalStatsDir)
	carbonserver.SetPercentiles(conf.Carbonserver.Percentiles)
	// carbonserver.SetQueryTimeout(conf.Carbonserver.QueryTimeout.Value())

	carbonserver.SetMaxInflightRequests(conf.Carbonserver.MaxInflightRequests)
	carbonserver.SetNoServiceWhenIndexIsNotReady(conf.Carbonserver.NoServiceWhenIndexIsNotReady)
	carbonserver.SetRenderTraceLoggingEnabled(conf.Carbonserver.RenderTraceLoggingEnabled)

	if conf.Carbonserver.RequestTimeout != nil {
		carbonserver.SetRequestTimeout(conf.Carbonserver.RequestTimeout.Value())
	}
	if len(apiPerPathRateLimiters) > 0 {
		carbonserver.SetAPIPerPathRateLimiter(apiPerPathRateLimiters)
	}
	if len(globQueryRateLimiters) > 0 {
		carbonserver.SetHeavyGlobQueryRateLimiters(globQueryRateLimiters)
	}

	if err := app.configureCarbonserverQuotas(carbonserver); err != nil {
		return nil, err
	}

	newMetricsChan = app.configureCarbonserverCache(core, carbonserver)

	if conf.Prometheus.Enabled {
		carbonserver.InitPrometheus(app.PromRegisterer)
	}
	if conf.Tracing.Enabled {
		log.Printf("Otel tracing is removed in current verion, ignoring tracing.enabled=true")
	}

	carbonserver.RegisterInternalInfoHandler("cache", core.GetInfo)
	carbonserver.RegisterInternalInfoHandler("config", app.configInfo)

	app.Carbonserver = carbonserver
	return newMetricsChan, nil
}

func (app *App) carbonserverRateLimiters() (map[string]*carbonserver.ApiPerPathRatelimiter, []*carbonserver.GlobQueryRateLimiter, error) {
	conf := app.Config
	apiPerPathRateLimiters := map[string]*carbonserver.ApiPerPathRatelimiter{}
	for _, rl := range conf.Carbonserver.APIPerPathRateLimiters {
		var timeout time.Duration
		if rl.RequestTimeout != nil {
			timeout = rl.RequestTimeout.Value()
		}
		apiPerPathRateLimiters[rl.Path] = carbonserver.NewApiPerPathRatelimiter(rl.MaxInflightRequests, timeout)
	}
	var globQueryRateLimiters []*carbonserver.GlobQueryRateLimiter
	for _, rl := range conf.Carbonserver.HeavyGlobQueryRateLimiters {
		gqrl, err := carbonserver.NewGlobQueryRateLimiter(rl.Pattern, rl.MaxInflightRequests)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to init Carbonserver.HeavyGlobQueryRateLimiters %s: %w", rl.Pattern, err)
		}
		globQueryRateLimiters = append(globQueryRateLimiters, gqrl)
	}

	return apiPerPathRateLimiters, globQueryRateLimiters, nil
}

func (app *App) configureCarbonserverQuotas(listener *carbonserver.CarbonserverListener) error {
	conf := app.Config
	if app.Config.Whisper.Quotas != nil {
		if !conf.Carbonserver.ConcurrentIndex || conf.Carbonserver.RealtimeIndex <= 0 {
			return errors.New("concurrent-index and realtime-index needs to be enabled for quota control.")
		}

		app.quotaEstimateConfig.Store(conf)
		sharedStore := app.MetricStore != nil
		listener.SetEstimateSize(func(metric string) (logicalSize, physicalSize, dataPoints int64) {
			cfg := app.quotaEstimateConfig.Load().(*Config)
			schema, ok := cfg.Whisper.Schemas.Match(metric)

			if !ok {
				// Why not configurable: go-carbon users
				// should always make sure that there is a default retention policy.
				return 4096 + 172800*12, 4096 + 172800*12, 172800 // 2 days of secondly data
			}

			for _, r := range schema.Retentions {
				dataPoints += int64(r.NumberOfPoints())
			}
			if sharedStore {
				return int64(16+12*len(schema.Retentions)) + dataPoints*12, 0, dataPoints
			}
			logicalSize = 4096 + dataPoints*12
			if cfg.Whisper.Sparse { // we assume that physical size for sparse metrics takes only a part of the logical size
				physicalSize = int64(cfg.Whisper.PhysicalSizeFactor * float32(logicalSize))
			} else {
				physicalSize = logicalSize
			}

			return logicalSize, physicalSize, dataPoints
		})

		listener.SetQuotas(app.Config.getCarbonserverQuotas(conf.Carbonserver.QuotaUsageReportFrequency.Value()))
	}

	return nil
}

func (app *App) configureCarbonserverCache(core *cache.Cache, listener *carbonserver.CarbonserverListener) (newMetricsChan chan string) {
	conf := app.Config
	var setConfigRetriever bool
	if conf.Carbonserver.CacheScan {
		listener.SetCacheGetMetricsFunc(core.GetRecentNewMetrics)

		setConfigRetriever = true
	}

	if conf.Carbonserver.RealtimeIndex > 0 {
		newMetricsChan = listener.SetRealtimeIndex(conf.Carbonserver.RealtimeIndex)

		setConfigRetriever = true
	}
	if setConfigRetriever {
		retriever := &wspConfigRetriever{
			getRetentionFunc: app.Persister.GetRetentionPeriod,
			getAggrNameFunc:  app.Persister.GetAggrConf,
		}
		listener.SetConfigRetriever(retriever)
	}

	return newMetricsChan
}

func (app *App) restoreBeforeReceivers(core *cache.Cache) {
	conf := app.Config

	logger := zapwriter.Logger("app")
	logger.Info("restoring dump before starting receivers",
		zap.String("path", conf.Dump.Path),
		zap.Int("restorePerSecond", conf.Dump.RestorePerSecond),
	)
	restoreStart := time.Now()
	// Build the saved index alongside both dump loading and disk drain. Neither
	// read nor input listeners open until restored history has been persisted.
	if app.Carbonserver != nil {
		app.Carbonserver.WarmupIndex()
	}
	app.Restore(core.AddRestored, conf.Dump.Path, conf.Dump.RestorePerSecond)
	restoreLoaded := time.Now()
	for !core.IsEmpty() {
		time.Sleep(10 * time.Millisecond)
	}
	// The collector has not started yet. Separate the accumulated restore
	// counters from live interval statistics instead of reporting minutes of
	// creates/updates as the first single collection interval.
	restoreStats := make(map[string]float64)
	app.Persister.Stat(func(name string, value float64) { restoreStats[name] = value })
	logger.Info("dump restored, starting receivers",
		zap.Duration("load_seconds", restoreLoaded.Sub(restoreStart)),
		zap.Duration("drain_seconds", time.Since(restoreLoaded)),
		zap.Any("persister_stats", restoreStats),
	)
}

func (app *App) listenCarbonserver(core *cache.Cache, newMetricsChan chan string) (err error) {
	conf := app.Config
	if cs := app.Carbonserver; cs != nil {
		// Restore bypasses live quotas and notifications, just as before warmup.
		// Start the filesystem scan only after restored files have been written.
		if app.Config.Whisper.Quotas != nil {
			core.SetThrottle(cs.ShouldThrottleMetric)
		}
		if conf.Carbonserver.CacheScan {
			core.InitCacheScanAdds()
		}
		core.SetNewMetricsChan(newMetricsChan)
		core.SetMetricExists(cs.MetricExists)
		if err = cs.Listen(conf.Carbonserver.Listen); err != nil {
			return
		}
		if conf.Carbonserver.Grpc.Enabled {
			if err = cs.ListenGRPC(conf.Carbonserver.Grpc.Listen); err != nil {
				return
			}
		}
	}

	return nil
}

func (app *App) startReceivers(core *cache.Cache) (err error) {
	conf := app.Config
	app.Receivers = make([]*NamedReceiver, 0)
	var rcv receiver.Receiver
	var rcvOptions map[string]interface{}

	/* UDP start */
	if conf.Udp.Enabled {
		if rcvOptions, err = receiver.WithProtocol(conf.Udp, "udp"); err != nil {
			return
		}

		if rcv, err = receiver.New("udp", rcvOptions, core.Add); err != nil {
			return
		}

		app.Receivers = append(app.Receivers, &NamedReceiver{
			Receiver: rcv,
			Name:     "udp",
		})
	}
	/* UDP end */

	/* TCP start */
	if conf.Tcp.Enabled {
		if rcvOptions, err = receiver.WithProtocol(conf.Tcp, "tcp"); err != nil {
			return
		}

		if rcv, err = receiver.New("tcp", rcvOptions, core.Add); err != nil {
			return
		}

		if conf.Prometheus.Enabled {
			rcv.InitPrometheus(app.PromRegisterer)
		}

		app.Receivers = append(app.Receivers, &NamedReceiver{
			Receiver: rcv,
			Name:     "tcp",
		})
	}
	/* TCP end */

	/* PICKLE start */
	if conf.Pickle.Enabled {
		if rcvOptions, err = receiver.WithProtocol(conf.Pickle, "pickle"); err != nil {
			return
		}

		if rcv, err = receiver.New("pickle", rcvOptions, core.Add); err != nil {
			return
		}

		app.Receivers = append(app.Receivers, &NamedReceiver{
			Receiver: rcv,
			Name:     "pickle",
		})
	}
	/* PICKLE end */

	/* CUSTOM RECEIVERS start */
	for receiverName, receiverOptions := range conf.Receiver {
		if rcv, err = receiver.New(receiverName, receiverOptions, core.Add); err != nil {
			return
		}

		app.Receivers = append(app.Receivers, &NamedReceiver{
			Receiver: rcv,
			Name:     receiverName,
		})
	}
	/* CUSTOM RECEIVERS end */

	return nil
}

func (app *App) startCarbonlink(core *cache.Cache) (err error) {
	conf := app.Config
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

	return nil
}

func (app *App) startBuckyd() (err error) {
	conf := app.Config
	if conf.Buckyd.Enabled {
		app.Buckyd, err = buckyd.New(conf.Buckyd, app.MetricStore)
		if err != nil {
			return fmt.Errorf("configure buckyd: %w", err)
		}
		if app.metricStoreIndex != nil {
			app.Buckyd.SetOnChange(app.metricStoreIndex.notify)
		}
		if err = app.Buckyd.Start(); err != nil {
			return fmt.Errorf("start buckyd: %w", err)
		}
	}

	return nil
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

func (app *App) CheckPersisterPolicyConsistencies(rate int, printInconsistentMetrics bool) {
	if app.Config.Whisper.StorageBackend == "pebble-chunk" {
		log.Print("the file policy consistency checker is unavailable for shared storage; inspect metric policies through carbonserver info")
		return
	}
	p := persister.NewWhisper(
		app.Config.Whisper.DataDir,
		app.Config.Whisper.Schemas,
		app.Config.Whisper.Aggregation,
		nil, nil, nil, nil,
	)
	err := p.CheckPolicyConsistencies(rate, printInconsistentMetrics)
	if err != nil {
		log.Printf("failed to check policy consistencies: %s\n", err)
		return
	}
}

func (app *App) configInfo() map[string]interface{} {
	infos := map[string]interface{}{}
	for name, file := range map[string]string{
		"app":         app.ConfigFilename,
		"schema":      app.Config.Whisper.SchemasFilename,
		"aggregation": app.Config.Whisper.AggregationFilename,
		"quota":       app.Config.Whisper.QuotasFilename,
	} {
		if data, err := os.ReadFile(file); err != nil {
			infos[name] = err.Error()
		} else {
			infos[name] = string(data)
		}
	}
	return infos
}
