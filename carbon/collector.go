package carbon

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"runtime"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
)

// A report may wait for the sender, but never holds up ingestion.
const statReportSendTimeout = 5 * time.Second

type statFunc func(context.Context, <-chan bool)

type statModule interface {
	Stat(send helper.StatCallback)
}

type Collector struct {
	helper.Stoppable
	graphPrefix    string
	metricInterval time.Duration
	endpoint       string
	data           chan *points.Points
	stats          []statFunc
	logger         *zap.Logger
	dropped        prometheus.Counter
}

func RuntimeStat(send helper.StatCallback) {
	send("GOMAXPROCS", float64(runtime.GOMAXPROCS(-1)))
	send("NumGoroutine", float64(runtime.NumGoroutine()))
	runtimeWaitStat(send)
}

func NewCollector(app *App) *Collector {
	// app locked by caller
	// The collector is recreated on reload; keep the counter on App so it
	// remains cumulative and does not register twice. Scraping bypasses data.
	if app.internalStatsDropped == nil {
		app.internalStatsDropped = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "internal_stats_dropped_total",
			Help: "Internal stat points dropped because the collector send queue is full.",
		})
		if app.PromRegisterer != nil {
			app.PromRegisterer.MustRegister(app.internalStatsDropped)
		}
	}

	c := &Collector{
		graphPrefix:    app.Config.Common.GraphPrefix,
		metricInterval: app.Config.Common.MetricInterval.Value(),
		data:           make(chan *points.Points, 4096),
		endpoint:       app.Config.Common.MetricEndpoint,
		stats:          make([]statFunc, 0),
		dropped:        app.internalStatsDropped,
	}

	c.Start()

	logger := zapwriter.Logger("stat")

	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		logger.Error("metric-endpoint parse error", zap.Error(err))
		c.endpoint = MetricEndpointLocal
	}

	logger = logger.With(zap.String("endpoint", c.endpoint))
	c.logger = logger

	c.startSender(app.Cache.Add, endpoint, logger)

	sendCallback := c.statCallback

	// Modules may have counted input throughout a long startup (or reload).
	// Establish their baseline without publishing that unbounded window as one
	// metric interval. Stat still updates cumulative Prometheus counters.
	discardStat := func(string, float64) {}
	moduleCallback := func(moduleName string, moduleObj statModule) statFunc {
		moduleObj.Stat(discardStat)
		return func(ctx context.Context, exit <-chan bool) {
			moduleObj.Stat(sendCallback(ctx, exit, moduleName))
		}
	}

	c.stats = append(c.stats, func(ctx context.Context, exit <-chan bool) {
		RuntimeStat(sendCallback(ctx, exit, "runtime"))
	})

	if app.SocketStats != nil {
		c.stats = append(c.stats, moduleCallback("tcp", app.SocketStats))
	}

	if app.Cache != nil {
		c.stats = append(c.stats, moduleCallback("cache", app.Cache))
	}
	if l := app.inputLog.Load(); l != nil {
		c.stats = append(c.stats, moduleCallback("inputLog", inputLogStats{l}))
	}

	if app.Carbonserver != nil {
		c.stats = append(c.stats, moduleCallback("carbonserver", app.Carbonserver))
	}

	if app.Receivers != nil {
		for i := 0; i < len(app.Receivers); i++ {
			c.stats = append(c.stats, moduleCallback(app.Receivers[i].Name, app.Receivers[i]))
		}
	}

	if app.Persister != nil {
		c.stats = append(c.stats, moduleCallback("persister", app.Persister))
	}
	if app.MetricStore != nil {
		c.stats = append(c.stats, moduleCallback("storage", &storeStats{db: app.MetricStore}))
	}
	if app.expirationStats != nil {
		c.stats = append(c.stats, moduleCallback("storage.expiration", app.expirationStats))
	}

	if app.Api != nil {
		c.stats = append(c.stats, moduleCallback("grpc", app.Api))
	}

	if app.Tags != nil {
		c.stats = append(c.stats, moduleCallback("tags", app.Tags))
	}

	// collector worker
	c.Go(c.run)

	return c
}

func (c *Collector) statCallback(ctx context.Context, exit <-chan bool, moduleName string) helper.StatCallback {
	return func(metric string, value float64) {
		key := fmt.Sprintf("%s.%s.%s", c.graphPrefix, moduleName, metric)
		c.logger.Info("collect", zap.String("metric", key), zap.Float64("value", value))
		p := points.NowPoint(key, value)
		select {
		case c.data <- p:
			return
		default:
		}
		// Share one deadline across the entire report, rather than waiting
		// once per metric. A healthy sender can drain arbitrarily large quota
		// bursts through the fixed-size queue. Shutdown cancels the wait.
		select {
		case c.data <- p:
			return
		case <-ctx.Done():
		case <-exit:
		}
		c.dropped.Inc()
		c.logger.Warn("send queue is full. metric dropped",
			zap.String("key", key), zap.Float64("value", value))
	}
}

func (c *Collector) collect(exit <-chan bool) {
	ctx, cancel := context.WithTimeout(context.Background(), statReportSendTimeout)
	defer cancel()
	for _, stat := range c.stats {
		stat(ctx, exit)
	}
}

func (c *Collector) startSender(storeFunc func(*points.Points), endpoint *url.URL, logger *zap.Logger) {
	if c.endpoint == MetricEndpointLocal {
		// sender worker

		c.Go(func(exit chan bool) {
			for {
				select {
				case <-exit:
					return
				case p := <-c.data:
					storeFunc(p)
				}
			}
		})
	} else {
		chunkSize := 32768
		if endpoint.Scheme == "udp" {
			chunkSize = 1000 // nc limitation (1024 for udp) and mtu friendly
		}

		c.Go(func(exit chan bool) {
			points.Glue(exit, c.data, chunkSize, time.Second, func(chunk []byte) {

				sendMetricChunk(exit, chunk, endpoint, logger)
			})
		})
	}

}

func sendMetricChunk(exit chan bool, chunk []byte, endpoint *url.URL, logger *zap.Logger) {
	var conn net.Conn
	var err error
	defaultTimeout := 5 * time.Second

	// send data to endpoint
SendLoop:
	for {

		// check exit
		select {
		case <-exit:
			break SendLoop
		default:
			// pass
		}

		// close old broken connection
		if conn != nil {
			conn.Close()
			conn = nil
		}

		conn, err = net.DialTimeout(endpoint.Scheme, endpoint.Host, defaultTimeout)
		if err != nil {
			logger.Error("dial failed", zap.Error(err))
			time.Sleep(time.Second)
			continue SendLoop
		}

		err = conn.SetDeadline(time.Now().Add(defaultTimeout))
		if err != nil {
			logger.Error("conn.SetDeadline failed", zap.Error(err))
			time.Sleep(time.Second)
			continue SendLoop
		}

		_, err := conn.Write(chunk)
		if err != nil {
			logger.Error("conn.Write failed", zap.Error(err))
			time.Sleep(time.Second)
			continue SendLoop
		}

		break SendLoop
	}

	if conn != nil {
		conn.Close()
		conn = nil
	}
}

func (c *Collector) run(exit chan bool) {
	ticker := time.NewTicker(c.metricInterval)
	defer ticker.Stop()

	for {
		select {
		case <-exit:
			return
		case <-ticker.C:
			c.collect(exit)
		}
	}
}
