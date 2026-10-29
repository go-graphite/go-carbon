package carbon

import (
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper/malloc"
)

// applyMallocTrim starts, restarts or stops the periodic malloc_trim
// (common.malloc-trim-interval) and registers the C heap metrics once. It is
// called on start and on SIGHUP, with the app locked.
func (app *App) applyMallocTrim() {
	if !malloc.Supported() {
		return
	}
	if app.mallocCollector == nil && app.PromRegisterer != nil {
		c := malloc.NewCollector()
		if err := app.PromRegisterer.Register(c); err != nil {
			zapwriter.Logger("malloc").Warn("malloc metrics not registered", zap.Error(err))
		} else {
			app.mallocCollector = c
		}
	}
	var interval time.Duration
	if d := app.Config.Common.MallocTrimInterval; d != nil {
		interval = d.Value()
	}
	withStats := app.Config.Common.MallocStats
	if app.mallocTrimmer != nil && interval == app.mallocTrimEvery && withStats == app.mallocStats {
		return
	}
	app.stopMallocTrim()
	app.mallocTrimmer, app.mallocTrimEvery, app.mallocStats = malloc.Start(interval, withStats), interval, withStats
	if app.mallocCollector != nil {
		app.mallocCollector.SetTrimmer(app.mallocTrimmer)
	}
	if app.mallocTrimmer != nil {
		zapwriter.Logger("malloc").Info("periodic malloc_trim", zap.Duration("interval", interval))
	}
}

func (app *App) stopMallocTrim() {
	app.mallocTrimmer.Stop()
	app.mallocTrimmer, app.mallocTrimEvery, app.mallocStats = nil, 0, false
}
