package carbon

import (
	"fmt"
	"net"
	"net/url"
	"runtime"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
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
	logger         *zap.Logger
}

func RuntimeStat(send helper.StatCallback) {
	send("GOMAXPROCS", float64(runtime.GOMAXPROCS(-1)))
	send("NumGoroutine", float64(runtime.NumGoroutine()))
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

	logger = logger.With(zap.String("endpoint", c.endpoint))

	if c.endpoint == MetricEndpointLocal {
		// sender worker
		storeFunc := app.Cache.Add

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
			})
		})

	}

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

	moduleCallback := func(moduleName string, moduleObj statModule) statFunc {
		return func() {
			moduleObj.Stat(sendCallback(moduleName))
		}
	}

	c.stats = append(c.stats, func() {
		RuntimeStat(sendCallback("runtime"))
	})

	if app.Cache != nil {
		c.stats = append(c.stats, moduleCallback("cache", app.Cache))
	}

	if app.Carbonserver != nil {
		c.stats = append(c.stats, moduleCallback("carbonserver", app.Carbonserver))
	}

	if app.UDP != nil {
		c.stats = append(c.stats, moduleCallback("udp", app.UDP))
	}

	if app.TCP != nil {
		c.stats = append(c.stats, moduleCallback("tcp", app.TCP))
	}

	if app.Pickle != nil {
		c.stats = append(c.stats, moduleCallback("pickle", app.Pickle))
	}

	if app.Persister != nil {
		c.stats = append(c.stats, moduleCallback("persister", app.Persister))
	}

	// collector worker
	c.Go(func(exit chan bool) {
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
	})

	return c
}

func (c *Collector) collect() {
	for _, stat := range c.stats {
		stat()
	}
}
