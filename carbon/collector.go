package carbon

import (
	"bytes"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

type statCb func(module string) func(metric string, value float64)

type Collector struct {
	helper.Stoppable
	app             *App
	metricInterval  time.Duration
	statModule      statCb
	statModuleFlush func()
}

func NewCollector(app *App) *Collector {
	var statModule statCb
	var statModuleFlush func()

	if u, err := url.Parse(app.Config.Common.MetricEndpoint); app.Config.Common.MetricEndpoint == MetricEndpointLocal || err != nil {
		if err != nil {
			logrus.Errorf("[stat] metric-endpoint parse error: %s", err.Error())
		}
		statModule, statModuleFlush = func(module string) func(metric string, value float64) {
			return func(metric string, value float64) {
				key := fmt.Sprintf("%s.%s.%s", app.Config.Common.GraphPrefix, module, metric)
				logrus.Infof("[stat] %s=%#v", key, value)
				app.Cache.Add(points.NowPoint(key, value))
			}
		}, nil
	} else {
		statModule, statModuleFlush = sendMetricFactory(u)
	}

	c := &Collector{
		app:             app,
		metricInterval:  app.Config.Common.MetricInterval.Value(),
		statModule:      statModule,
		statModuleFlush: statModuleFlush,
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

	return c
}

func sendMetricFactory(endpoint *url.URL) (statCb, func()) {
	var conn net.Conn

	getConnection := func() bool {
		var err error
		defaultTimeout := 5 * time.Second

		conn, err = net.DialTimeout(endpoint.Scheme, endpoint.Host, defaultTimeout)
		if err != nil {
			logrus.Errorf("[stat] dial %s failed: %s", endpoint.String(), err.Error())
			conn.Close()
			conn = nil
			return false
		}

		err = conn.SetDeadline(time.Now().Add(defaultTimeout))
		if err != nil {
			conn.Close()
			conn = nil
			logrus.Errorf("[stat] conn.SetDeadline failed: %s", err.Error())
			return false
		}

		return true
	}

	chunkSize := 32768
	if endpoint.Scheme == "udp" {
		chunkSize = 1000 // nc limitation (1024 for udp) and mtu friendly
	}

	chunkBuf := bytes.NewBuffer(make([]byte, chunkSize))

	flush := func() {
		if !getConnection() {
			return
		}
		_, err := conn.Write(chunkBuf.Bytes())
		chunkBuf.Reset()
		if err != nil {
			logrus.Errorf("[stat] conn.Write failed: %s", err.Error())
			conn.Close()
			conn = nil
		}
	}

	return func(module string) func(metric string, value float64) {
		return func(metric string, value float64) {
			s := fmt.Sprintf("%s %v %v\n", metric, value, time.Now().UTC().Second())

			if chunkBuf.Len()+len(s) > chunkSize {
				flush()
			}
			chunkBuf.WriteString(s)
		}
	}, flush
}

func (c *Collector) collect() {
	app := c.app

	app.Lock()
	defer app.Unlock()

	statModule, flush := c.statModule, c.statModuleFlush

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

	if flush != nil {
		flush()
	}
}
