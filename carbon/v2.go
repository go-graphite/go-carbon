package carbon

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type v2OpenFunc = daemonstore.OpenFunc
type v2Error struct{ err error }

// openV2Backend is intentionally closed until the durable, exact engine adapter
// is linked here. A benchmark command is not a daemon storage implementation.
func openV2Backend(daemonstore.Options) (daemonstore.Backend, error) {
	return nil, daemonstore.ErrUnavailable
}

func (app *App) ExitError() error {
	if failure := app.v2Failure.Load(); failure != nil {
		return failure.err
	}
	return nil
}

func (app *App) startV2() (err error) {
	conf := app.Config
	startupErrors := prometheus.NewCounter(prometheus.CounterOpts{Name: "go_carbon_v2_startup_errors_total", Help: "Immutable-parts startup failures."})
	defer func() {
		if err != nil {
			startupErrors.Inc()
			zapwriter.Logger("v2").Error("v2 startup failed", zap.Error(err))
		}
	}()
	if conf.Prometheus.Enabled && app.PromRegisterer != nil {
		if err := app.PromRegisterer.Register(startupErrors); err != nil {
			return err
		}
	}
	open := app.v2Open
	if open == nil {
		open = openV2Backend
	}
	b, err := open(daemonstore.Options{
		DataDir: conf.V2.DataDir, Shards: conf.V2.Shards, Owners: conf.V2.Owners,
		LegacyDataDir: conf.Whisper.DataDir, LegacyStoreDir: sharedStorePath(conf), LegacyBackend: conf.Whisper.StorageBackend,
		PartInterval: conf.V2.PartInterval.Value(),
		Policy: func(name string) (daemonstore.Metric, error) {
			schema, ok := conf.Whisper.Schemas.Match(name)
			if !ok {
				return daemonstore.Metric{}, fmt.Errorf("no storage schema for %q", name)
			}
			aggr := conf.Whisper.Aggregation.Match(name)
			m := daemonstore.Metric{AggregationMethod: aggr.AggregationMethod().String(), XFilesFactor: float32(aggr.XFilesFactor())}
			for _, r := range schema.Retentions {
				m.Archives = append(m.Archives, daemonstore.Archive{Step: r.SecondsPerPoint(), Points: r.NumberOfPoints()})
			}
			return m, nil
		},
	})
	if err != nil {
		return fmt.Errorf("start v2: %w", err)
	}
	if b == nil {
		return errors.New("v2 backend returned no storage handle")
	}
	app.v2 = newV2Runtime(app, b)
	if conf.Prometheus.Enabled && app.PromRegisterer != nil {
		if err := app.PromRegisterer.Register(app.v2); err != nil {
			return err
		}
	}
	runtime.GOMAXPROCS(conf.Common.MaxCPU)
	app.applyRuntimeProfiling()
	// Cache supplies the existing server's policy/index setup only. Every
	// receiver and query is redirected below; no legacy persister is started.
	core := cache.New()
	app.Cache = core
	if err := app.startAPI(core); err != nil {
		return err
	}
	queue, err := app.configureCarbonserver(core)
	if err != nil {
		return err
	}
	if err := app.listenCarbonserver(core, queue); err != nil {
		return err
	}
	if err := app.startCarbonlink(core); err != nil {
		return err
	}
	if err := app.startReceivers(core); err != nil {
		return err
	}
	app.Collector = NewCollector(app)
	return nil
}

func (app *App) reloadV2() error {
	runtime.GOMAXPROCS(app.Config.Common.MaxCPU)
	app.applyRuntimeProfiling()
	if app.Collector != nil {
		app.Collector.Stop()
	}
	app.Collector = NewCollector(app)
	return nil
}

// The caller holds app.Lock. Stop every producer before the durability barrier;
// drain reads before Close can release their immutable snapshot resources.
func (app *App) stopV2() {
	v := app.v2
	app.stopInputListeners()
	if app.Collector != nil {
		app.Collector.Stop()
		app.Collector = nil
	}
	if err := v.Backend.Flush(); err != nil {
		v.failure("flush", err, false)
	}
	<-app.stopReadListeners()
	if err := v.Backend.Close(); err != nil {
		v.failure("close", err, false)
	}
	close(v.stopErrors)
	<-v.errorsDone
	if app.PromRegisterer != nil {
		app.PromRegisterer.Unregister(v)
	}
	if app.Cache != nil {
		app.Cache.Stop()
		app.Cache = nil
	}
	app.v2 = nil
	if app.exit != nil {
		close(app.exit)
		app.exit = nil
	}
}

func (v *v2Runtime) failure(op string, err error, stop bool) {
	v.failures.WithLabelValues(op).Inc()
	zapwriter.Logger("v2").Error("v2 operation failed", zap.String("operation", op), zap.Error(err))
	if stop && v.app.v2Failure.CompareAndSwap(nil, &v2Error{fmt.Errorf("v2 %s: %w", op, err)}) {
		go v.app.Stop()
	} else if !stop {
		v.app.v2Failure.CompareAndSwap(nil, &v2Error{fmt.Errorf("v2 %s: %w", op, err)})
	}
}

func (v *v2Runtime) append(p *points.Points) {
	if v.app.ExitError() != nil {
		return
	}
	if err := v.Backend.Append(p); err != nil {
		v.failure("append", err, true)
	}
}

func (v *v2Runtime) queryFailure(op string, err error) {
	if err != nil && !errors.Is(err, daemonstore.ErrNotFound) && !errors.Is(err, context.Canceled) {
		v.failures.WithLabelValues(op).Inc()
		zapwriter.Logger("v2").Error("v2 query failed", zap.String("operation", op), zap.Error(err))
	}
}

func (v *v2Runtime) Fetch(ctx context.Context, name string, from, until int) (*daemonstore.Series, error) {
	started := time.Now()
	s, err := v.Backend.Fetch(ctx, name, from, until)
	v.readLatency.Observe(time.Since(started).Seconds())
	v.queryFailure("fetch", err)
	return s, err
}
func (v *v2Runtime) Info(ctx context.Context, name string) (daemonstore.Metric, error) {
	m, err := v.Backend.Info(ctx, name)
	v.queryFailure("info", err)
	return m, err
}
func (v *v2Runtime) Find(ctx context.Context, pattern string) ([]daemonstore.Match, error) {
	m, err := v.Backend.Find(ctx, pattern)
	v.queryFailure("find", err)
	return m, err
}
func (v *v2Runtime) CacheQuery(ctx context.Context, name string) ([]points.Point, error) {
	p, err := v.Backend.CacheQuery(ctx, name)
	if errors.Is(err, daemonstore.ErrNotFound) {
		return nil, nil
	}
	v.queryFailure("cache", err)
	return p, err
}
