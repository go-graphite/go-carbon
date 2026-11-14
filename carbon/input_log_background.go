package carbon

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type backgroundInputReplay struct {
	history     *cache.Cache
	source      *replayLogSource
	read        func(func(*points.Points), <-chan struct{}) (inputlog.ReplayStats, error)
	stop        chan struct{}
	done        chan struct{}
	started     bool
	batchPoints int
	err         error // published by closing done
}

type replayLogSource struct {
	*cache.Cache
	persisted atomic.Bool
}

func (s *replayLogSource) LogLowWater() uint64 {
	if !s.persisted.Load() {
		return 0
	}
	return s.Cache.LogLowWater()
}

func (app *App) prepareInputReplay(core *cache.Cache, l *inputlog.Log) {
	history := cache.New()
	// Reserve a finite recovery window in addition to the ordinary live cache.
	// It must drain independently of live cache capacity and changing host
	// headroom: otherwise live input can consume that headroom and deadlock
	// the old frontier. The fixed window uses the existing recovery admission.
	window := uint64(1 << 20)
	if limit := app.Config.Cache.MaxSize; limit > 0 {
		window = min(window, max(1, limit/16))
	}
	history.SetMaxSize(window)
	history.SetMaxBytes(64 << 20)
	_ = history.SetWriteStrategy("sorted")
	core.BeginInputReplay(history)
	source := &replayLogSource{Cache: core}
	app.inputReplay = &backgroundInputReplay{history: history, source: source,
		read: l.StartWithReplay(source, cache.ShardCount), stop: make(chan struct{}), done: make(chan struct{}), batchPoints: int(min(1024, max(1, window/2)))}
	app.storeRestoreDone = app.inputReplay.done
	app.storeRestoreLoaded = app.inputReplay.done
	app.inputReplayActive.Store(true)
	app.inputReplayFailed.Store(false)
	app.restart.noteReplayed()
}

func (app *App) startInputReplay() {
	r := app.inputReplay
	if r == nil {
		return
	}
	r.started = true
	core, l := app.Cache, app.inputLog.Load()
	go func() {
		defer close(r.done)
		logger := zapwriter.Logger("input-log")
		logger.Info("replaying input log behind live receivers")
		if app.inputReplayBeforeRead != nil {
			app.inputReplayBeforeRead()
		}
		stats, err := r.read(func(p *points.Points) {
			// Bound the history window even for a frame larger than that window.
			data := p.Data
			for len(data) > 0 {
				n := min(len(data), r.batchPoints)
				for !r.history.HasRecoveryCapacityFor(p.Metric, n) {
					select {
					case <-r.stop:
						return
					case <-time.After(time.Millisecond):
					}
				}
				select {
				case <-r.stop:
					return
				default:
				}
				r.history.AddRecovered(&points.Points{Metric: p.Metric, Data: data[:n]})
				data = data[n:]
			}
		}, r.stop)
		select {
		case <-r.stop:
			return
		default:
		}
		if err == nil && stats.Corrupt != 0 {
			err = errors.New("background replay encountered corrupt frames")
		}
		if err == nil {
			for !r.history.IsEmpty() {
				select {
				case <-r.stop:
					return
				case <-time.After(time.Millisecond):
				}
			}
			r.source.persisted.Store(true)
			// Retention and generation zero survive cancellation and replay
			// errors. Only a completely confirmed prefix releases live writes.
			err = l.ReleaseCopy()
		}
		r.err = err
		if err != nil {
			app.inputReplayFailed.Store(true)
			logger.Error("background input log replay failed; retaining history and writeout fence", zap.Error(err))
			return
		}
		core.FinishInputReplay()
		app.inputReplayActive.Store(false)
		logger.Info("background input log replay persisted", zap.Int64("points", stats.Points), zap.Duration("decode_seconds", stats.Duration))
	}()
}

func (app *App) stopInputReplay() {
	if r := app.inputReplay; r != nil && r.started {
		select {
		case <-r.stop:
		default:
			close(r.stop)
		}
		<-r.done
	}
}
