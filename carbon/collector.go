package carbon

import (
	"fmt"
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
	data           chan *points.Points
}

func NewCollector(app *App) *Collector {
	c := &Collector{
		app:            app,
		graphPrefix:    app.Config.Common.GraphPrefix,
		metricInterval: app.Config.Common.MetricInterval.Value(),
		data:           make(chan *points.Points, 4096),
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

	// sender worker
	out := app.Cache.In()

	c.Go(func(exit chan bool) {
		for {
			select {
			case <-exit:
				return
			case p := <-c.data:
				select {
				case out <- p:
				// pass
				case <-exit:
					return
				}
			}
		}
	})

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
