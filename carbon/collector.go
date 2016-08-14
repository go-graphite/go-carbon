package carbon

import (
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

type Collector struct {
	helper.Stoppable
	app            *App
	graphPrefix    string
	metricInterval time.Duration
	endpoint       string
	data           chan *points.Points
}

func NewCollector(app *App) *Collector {
	c := &Collector{
		app:            app,
		graphPrefix:    app.Config.Common.GraphPrefix,
		metricInterval: app.Config.Common.MetricInterval.Value(),
		data:           make(chan *points.Points, 4096),
		endpoint:       app.Config.Common.MetricEndpoint,
	}

	c.Start()

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

	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		logrus.Errorf("[stat] metric-endpoint parse error: %s", err.Error())
		c.endpoint = MetricEndpointLocal
	}

	if c.endpoint == MetricEndpointLocal {
		cache := app.Cache

		c.Go(func(exit chan bool) {
			for {
				select {
				case <-exit:
					return
				case p := <-c.data:
					cache.Add(p)
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
						logrus.Errorf("[stat] dial %s failed: %s", c.endpoint, err.Error())
						time.Sleep(time.Second)
						continue SendLoop
					}

					err = conn.SetDeadline(time.Now().Add(defaultTimeout))
					if err != nil {
						logrus.Errorf("[stat] conn.SetDeadline failed: %s", err.Error())
						time.Sleep(time.Second)
						continue SendLoop
					}

					_, err := conn.Write(chunk)
					if err != nil {
						logrus.Errorf("[stat] conn.Write failed: %s", err.Error())
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

	return c
}

func (c *Collector) collect() {
	app := c.app

	app.Lock()
	defer app.Unlock()

	statModule := func(module string) func(metric string, value float64) {
		return func(metric string, value float64) {
			key := fmt.Sprintf("%s.%s.%s", c.graphPrefix, module, metric)
			logrus.Infof("[stat] %s=%#v", key, value)
			select {
			case c.data <- points.NowPoint(key, value):
				// pass
			default:
				logrus.WithField("key", key).WithField("value", value).
					Warn("[stat] send queue is full. Metric dropped")
			}
		}
	}

	if app.Cache != nil {
		app.Cache.Stat(statModule("cache"))
	}
	if app.UDP != nil {
		app.UDP.Stat(statModule("udp"))
	}
	if app.TCP != nil {
		app.TCP.Stat(statModule("tcp"))
	}
	if app.Pickle != nil {
		app.Pickle.Stat(statModule("pickle"))
	}
	if app.Persister != nil {
		app.Persister.Stat(statModule("persister"))
	}
}
