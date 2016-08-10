package carbon

import (
	"fmt"

	"github.com/Sirupsen/logrus"

	"github.com/lomik/go-carbon/points"
)

// CollectStat collects stat data from all modules
func (app *App) CollectStat() {
	app.Lock()
	graphPrefix := app.Config.Common.GraphPrefix

	data := make(chan *points.Points, 1024) // buffer for collected internal metrics

	statModule := func(module string) func(metric string, value float64) {
		return func(metric string, value float64) {
			key := fmt.Sprintf("%s.%s.%s", graphPrefix, module, metric)
			logrus.Infof("[stat] %s=%s", key, value)
			data <- points.NowPoint(key, value)
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

	app.Unlock()
}
