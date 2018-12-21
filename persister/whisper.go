package persister

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	whisper "github.com/go-graphite/go-whisper"
	"go.uber.org/zap"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/tags"
	"github.com/lomik/zapwriter"
)

const storeMutexCount = 32768

type StoreFunc func(metric string)

// Whisper write data to *.wsp files
type Whisper struct {
	helper.Stoppable
	recv                    func(chan bool) string
	pop                     func(string) (*points.Points, bool)
	confirm                 func(*points.Points)
	tagsEnabled             bool
	taggedFn                func(string, bool)
	schemas                 WhisperSchemas
	aggregation             *WhisperAggregation
	workersCount            int
	rootPath                string
	created                 uint32 // counter
	throttledCreates        uint32 // counter
	updateOperations        uint32 // counter
	committedPoints         uint32 // counter
	sparse                  bool
	flock                   bool
	hashFilenames           bool
	maxUpdatesPerSecond     int
	maxCreatesPerSecond     int
	hardMaxCreatesPerSecond bool
	throttleTicker          *ThrottleTicker
	maxCreatesTicker        *ThrottleTicker
	storeMutex              [storeMutexCount]sync.Mutex
	mockStore               func() (StoreFunc, func())
	logger                  *zap.Logger
	createLogger            *zap.Logger
	// blockThrottleNs        uint64 // sum ns counter
	// blockQueueGetNs        uint64 // sum ns counter
	// blockAvoidConcurrentNs uint64 // sum ns counter
	// blockUpdateManyNs      uint64 // sum ns counter
}

// NewWhisper create instance of Whisper
func NewWhisper(
	rootPath string,
	schemas WhisperSchemas,
	aggregation *WhisperAggregation,
	recv func(chan bool) string,
	pop func(string) (*points.Points, bool),
	confirm func(*points.Points)) *Whisper {

	return &Whisper{
		recv:                recv,
		pop:                 pop,
		confirm:             confirm,
		schemas:             schemas,
		aggregation:         aggregation,
		workersCount:        1,
		rootPath:            rootPath,
		maxUpdatesPerSecond: 0,
		logger:              zapwriter.Logger("persister"),
		createLogger:        zapwriter.Logger("whisper:new"),
	}
}

// SetMaxUpdatesPerSecond enable throttling
func (p *Whisper) SetMaxUpdatesPerSecond(maxUpdatesPerSecond int) {
	p.maxUpdatesPerSecond = maxUpdatesPerSecond
}

// SetMaxCreatesPerSecond enable throttling
func (p *Whisper) SetMaxCreatesPerSecond(maxCreatesPerSecond int) {
	p.maxCreatesPerSecond = maxCreatesPerSecond
}

// SetHardMaxCreatesPerSecond enable throttling
func (p *Whisper) SetHardMaxCreatesPerSecond(hardMaxCreatesPerSecond bool) {
	p.hardMaxCreatesPerSecond = hardMaxCreatesPerSecond
}

// GetMaxUpdatesPerSecond returns current throttling speed
func (p *Whisper) GetMaxUpdatesPerSecond() int {
	return p.maxUpdatesPerSecond
}

// SetWorkers count
func (p *Whisper) SetWorkers(count int) {
	if count >= 1 {
		p.workersCount = count
	} else {
		p.workersCount = 1
	}
}

// SetSparse creation
func (p *Whisper) SetSparse(sparse bool) {
	p.sparse = sparse
}

// SetFLock on create and open
func (p *Whisper) SetFLock(flock bool) {
	p.flock = flock
}

func (p *Whisper) SetHashFilenames(v bool) {
	p.hashFilenames = v
}

func (p *Whisper) SetMockStore(fn func() (StoreFunc, func())) {
	p.mockStore = fn
}

func (p *Whisper) SetTagsEnabled(v bool) {
	p.tagsEnabled = v
}

func (p *Whisper) SetTaggedFn(fn func(string, bool)) {
	p.taggedFn = fn
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (p *Whisper) updateMany(w *whisper.Whisper, path string, points []*whisper.TimeSeriesPoint) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("UpdateMany panic recovered",
				zap.String("path", path),
				zap.String("traceback", fmt.Sprint(r)),
			)
		}
	}()

	// start = time.Now()
	w.UpdateMany(points)
}

func (p *Whisper) store(metric string) {
	// avoid concurrent store same metric
	// @TODO: may be flock?
	// start := time.Now()
	mutexIndex := fnv32(metric) % storeMutexCount
	p.storeMutex[mutexIndex].Lock()
	// atomic.AddUint64(&p.blockAvoidConcurrentNs, uint64(time.Since(start).Nanoseconds()))
	defer p.storeMutex[mutexIndex].Unlock()

	var path string
	if p.tagsEnabled && strings.IndexByte(metric, ';') >= 0 {
		path = tags.FilePath(p.rootPath, metric, p.hashFilenames) + ".wsp"
	} else {
		path = filepath.Join(p.rootPath, strings.Replace(metric, ".", "/", -1)+".wsp")
	}

	w, err := whisper.OpenWithOptions(path, &whisper.Options{
		FLock: p.flock,
	})
	if err != nil {
		// create new whisper if file not exists
		if !os.IsNotExist(err) {
			p.logger.Error("failed to open whisper file", zap.String("path", path), zap.Error(err))
			if pathErr, isPathErr := err.(*os.PathError); isPathErr && pathErr.Err == syscall.ENAMETOOLONG {
				p.pop(metric)
			}
			return
		}

		if t := p.maxCreatesThrottling(); t != throttlingOff {
			if t == throttlingHard {
				p.pop(metric)
			}

			atomic.AddUint32(&p.throttledCreates, 1)
			p.logger.Error("metric creation throttled",
				zap.String("name", metric),
				zap.String("operation", "create"),
				zap.Bool("dropped", t == throttlingHard),
			)

			return
		}

		schema, ok := p.schemas.Match(metric)
		if !ok {
			p.logger.Error("no storage schema defined for metric", zap.String("metric", metric))
			return
		}

		aggr := p.aggregation.match(metric)
		if aggr == nil {
			p.logger.Error("no storage aggregation defined for metric", zap.String("metric", metric))
			return
		}

		if err = os.MkdirAll(filepath.Dir(path), os.ModeDir|os.ModePerm); err != nil {
			p.logger.Error("mkdir failed",
				zap.String("dir", filepath.Dir(path)),
				zap.Error(err),
				zap.String("path", path),
			)
			return
		}

		w, err = whisper.CreateWithOptions(path, schema.Retentions, aggr.aggregationMethod, float32(aggr.xFilesFactor), &whisper.Options{
			Sparse: p.sparse,
			FLock:  p.flock,
		})
		if err != nil {
			p.logger.Error("create new whisper file failed",
				zap.String("path", path),
				zap.Error(err),
				zap.String("retention", schema.RetentionStr),
				zap.String("schema", schema.Name),
				zap.String("aggregation", aggr.name),
				zap.Float64("xFilesFactor", aggr.xFilesFactor),
				zap.String("method", aggr.aggregationMethodStr),
			)
			return
		}

		if p.tagsEnabled && p.taggedFn != nil && strings.IndexByte(metric, ';') >= 0 {
			p.taggedFn(metric, true)
		}

		p.createLogger.Debug("created",
			zap.String("path", path),
			zap.String("retention", schema.RetentionStr),
			zap.String("schema", schema.Name),
			zap.String("aggregation", aggr.name),
			zap.Float64("xFilesFactor", aggr.xFilesFactor),
			zap.String("method", aggr.aggregationMethodStr),
		)

		atomic.AddUint32(&p.created, 1)
	}

	values, exists := p.pop(metric)
	if !exists {
		return
	}
	if p.confirm != nil {
		defer p.confirm(values)
	}

	points := make([]*whisper.TimeSeriesPoint, len(values.Data))
	for i, r := range values.Data {
		points[i] = &whisper.TimeSeriesPoint{Time: int(r.Timestamp), Value: r.Value}
	}

	atomic.AddUint32(&p.committedPoints, uint32(len(values.Data)))
	atomic.AddUint32(&p.updateOperations, 1)

	// start = time.Now()
	p.updateMany(w, path, points)
	w.Close()
	// atomic.AddUint64(&p.blockUpdateManyNs, uint64(time.Since(start).Nanoseconds()))

	if p.tagsEnabled && p.taggedFn != nil && strings.IndexByte(metric, ';') >= 0 {
		p.taggedFn(metric, false)
	}
}

type throttleMode int

const (
	throttlingOff  throttleMode = iota
	throttlingSoft throttleMode = iota
	throttlingHard throttleMode = iota
)

// For test error messages
func (t throttleMode) String() string {
	if t == throttlingHard {
		return "hard"
	}

	if t == throttlingSoft {
		return "soft"
	}

	return "off"
}

func (p *Whisper) maxCreatesThrottling() throttleMode {
	if p.hardMaxCreatesPerSecond {
		keep, open := <-p.maxCreatesTicker.C
		if !open {
			return throttlingHard
		}

		if keep {
			return throttlingOff
		}

		return throttlingHard
	} else {
		select {
		case <-p.maxCreatesTicker.C:
			return throttlingOff

		default:
			return throttlingSoft
		}
	}
}

func (p *Whisper) worker(exit chan bool) {
	storeFunc := p.store
	var doneCb func()
	if p.mockStore != nil {
		storeFunc, doneCb = p.mockStore()
	}

LOOP:
	for {
		// start := time.Now()
		select {
		case <-p.throttleTicker.C:
			// atomic.AddUint64(&p.blockThrottleNs, uint64(time.Since(start).Nanoseconds()))
			// pass
		case <-exit:
			return
		}

		// start = time.Now()
		metric := p.recv(exit)
		// atomic.AddUint64(&p.blockQueueGetNs, uint64(time.Since(start).Nanoseconds()))
		if metric == "" {
			// exit closed
			break LOOP
		}
		storeFunc(metric)
		if doneCb != nil {
			doneCb()
		}
	}
}

// Stat callback
func (p *Whisper) Stat(send helper.StatCallback) {
	updateOperations := atomic.LoadUint32(&p.updateOperations)
	committedPoints := atomic.LoadUint32(&p.committedPoints)
	atomic.AddUint32(&p.updateOperations, -updateOperations)
	atomic.AddUint32(&p.committedPoints, -committedPoints)

	created := atomic.LoadUint32(&p.created)
	atomic.AddUint32(&p.created, -created)

	throttledCreates := atomic.LoadUint32(&p.throttledCreates)
	atomic.AddUint32(&p.throttledCreates, -throttledCreates)

	send("updateOperations", float64(updateOperations))
	send("committedPoints", float64(committedPoints))
	if updateOperations > 0 {
		send("pointsPerUpdate", float64(committedPoints)/float64(updateOperations))
	} else {
		send("pointsPerUpdate", 0.0)
	}

	send("created", float64(created))
	send("throttledCreates", float64(throttledCreates))
	send("maxCreatesPerSecond", float64(p.maxCreatesPerSecond))

	send("maxUpdatesPerSecond", float64(p.maxUpdatesPerSecond))
	send("workers", float64(p.workersCount))

	// helper.SendAndSubstractUint64("blockThrottleNs", &p.blockThrottleNs, send)
	// helper.SendAndSubstractUint64("blockQueueGetNs", &p.blockQueueGetNs, send)
	// helper.SendAndSubstractUint64("blockAvoidConcurrentNs", &p.blockAvoidConcurrentNs, send)
	// helper.SendAndSubstractUint64("blockUpdateManyNs", &p.blockUpdateManyNs, send)

}

// Start worker
func (p *Whisper) Start() error {
	return p.StartFunc(func() error {

		p.throttleTicker = NewThrottleTicker(p.maxUpdatesPerSecond)
		if p.hardMaxCreatesPerSecond {
			p.maxCreatesTicker = NewHardThrottleTicker(p.maxCreatesPerSecond)
		} else {
			p.maxCreatesTicker = NewThrottleTicker(p.maxCreatesPerSecond)
		}

		for i := 0; i < p.workersCount; i++ {
			p.Go(p.worker)
		}

		return nil
	})
}

func (p *Whisper) Stop() {
	p.StopFunc(func() {
		p.throttleTicker.Stop()
		p.maxCreatesTicker.Stop()
	})
}
