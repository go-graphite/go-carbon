package carbon

import (
	"fmt"
	"net"
	"net/url"
	"runtime"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

type statFunc func()

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
	logger         *zap.Logger //nolint:unused
}

func RuntimeStat(send helper.StatCallback) {
	send("GOMAXPROCS", float64(runtime.GOMAXPROCS(-1)))
	send("NumGoroutine", float64(runtime.NumGoroutine()))
	runtimeWaitStat(send)
}

func NewCollector(app *App) *Collector {
	// app locked by caller

	c := &Collector{
		graphPrefix:    app.Config.Common.GraphPrefix,
		metricInterval: app.Config.Common.MetricInterval.Value(),
		data:           make(chan *points.Points, 4096),
		endpoint:       app.Config.Common.MetricEndpoint,
		stats:          make([]statFunc, 0),
	}

	c.Start()

	logger := zapwriter.Logger("stat")

	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		logger.Error("metric-endpoint parse error", zap.Error(err))
		c.endpoint = MetricEndpointLocal
	}

	// v2 has no classic persister draining Cache. Feed local telemetry through
	// its ordinary TCP intake, preserving the same owner/journal boundary.
	if c.endpoint == MetricEndpointLocal && app.v2 != nil && app.v2.server != nil {
		host, port, e := net.SplitHostPort(app.v2.server.Addr().String())
		if e == nil {
			if ip := net.ParseIP(host); host == "" || ip != nil && ip.IsUnspecified() {
				host = "127.0.0.1"
				if ip != nil && ip.To4() == nil {
					host = "::1"
				}
			}
			endpoint = &url.URL{Scheme: "tcp", Host: net.JoinHostPort(host, port)}
			c.endpoint = endpoint.String()
		}
	}
	logger = logger.With(zap.String("endpoint", c.endpoint))

	c.startSender(app.Cache.Add, endpoint, logger)

	sendCallback := func(moduleName string) func(metric string, value float64) {
		return func(metric string, value float64) {
			key := fmt.Sprintf("%s.%s.%s", c.graphPrefix, moduleName, metric)
			logger.Info("collect", zap.String("metric", key), zap.Float64("value", value))
			select {
			case c.data <- points.NowPoint(key, value):
				// pass
			default:
				logger.Warn("send queue is full. metric dropped",
					zap.String("key", key), zap.Float64("value", value))
			}
		}
	}

	// Modules may have counted input throughout a long startup (or reload).
	// Establish their baseline without publishing that unbounded window as one
	// metric interval. Stat still updates cumulative Prometheus counters.
	discardStat := func(string, float64) {}
	moduleCallback := func(moduleName string, moduleObj statModule) statFunc {
		moduleObj.Stat(discardStat)
		return func() {
			moduleObj.Stat(sendCallback(moduleName))
		}
	}

	c.stats = append(c.stats, func() {
		RuntimeStat(sendCallback("runtime"))
	})

	if app.v2 != nil && app.v2.moves != nil {
		c.stats = append(c.stats, moduleCallback("v2.move", v2MoveStats{app.v2.moves}))
	}
	if app.v2 != nil {
		c.stats = append(c.stats, moduleCallback("v2.catalog", &app.v2.catalog))
		c.stats = append(c.stats, moduleCallback("v2.intake", app.v2.metrics))
	}
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

func (c *Collector) collect() {
	for _, stat := range c.stats {
		stat()
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
			c.collect()
		}
	}
}
