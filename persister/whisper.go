package persister

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Sirupsen/logrus"
	whisper "github.com/lomik/go-whisper"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

const storeMutexCount = 32768

type StoreFunc func(p *Whisper, values *points.Points)

// Whisper write data to *.wsp files
type Whisper struct {
	helper.Stoppable
	updateOperations    uint32
	committedPoints     uint32
	recv                func(chan bool) *points.Points
	confirm             func(*points.Points)
	schemas             WhisperSchemas
	aggregation         *WhisperAggregation
	workersCount        int
	rootPath            string
	created             uint32 // counter
	sparse              bool
	maxUpdatesPerSecond int
	throttleTicker      *ThrottleTicker
	storeMutex          [storeMutexCount]sync.Mutex
	mockStore           func() (StoreFunc, func())
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
	recv func(chan bool) *points.Points,
	confirm func(*points.Points)) *Whisper {

	return &Whisper{
		recv:                recv,
		confirm:             confirm,
		schemas:             schemas,
		aggregation:         aggregation,
		workersCount:        1,
		rootPath:            rootPath,
		maxUpdatesPerSecond: 0,
	}
}

// SetMaxUpdatesPerSecond enable throttling
func (p *Whisper) SetMaxUpdatesPerSecond(maxUpdatesPerSecond int) {
	p.maxUpdatesPerSecond = maxUpdatesPerSecond
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

func (p *Whisper) SetMockStore(fn func() (StoreFunc, func())) {
	p.mockStore = fn
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

func store(p *Whisper, values *points.Points) {
	// avoid concurrent store same metric
	// @TODO: may be flock?
	// start := time.Now()
	mutexIndex := fnv32(values.Metric) % storeMutexCount
	p.storeMutex[mutexIndex].Lock()
	// atomic.AddUint64(&p.blockAvoidConcurrentNs, uint64(time.Since(start).Nanoseconds()))
	defer p.storeMutex[mutexIndex].Unlock()

	path := filepath.Join(p.rootPath, strings.Replace(values.Metric, ".", "/", -1)+".wsp")

	w, err := whisper.Open(path)
	if err != nil {
		// create new whisper if file not exists
		if !os.IsNotExist(err) {
			logrus.Errorf("[persister] Failed to open whisper file %s: %s", path, err.Error())
			return
		}

		schema, ok := p.schemas.Match(values.Metric)
		if !ok {
			logrus.Errorf("[persister] No storage schema defined for %s", values.Metric)
			return
		}

		aggr := p.aggregation.match(values.Metric)
		if aggr == nil {
			logrus.Errorf("[persister] No storage aggregation defined for %s", values.Metric)
			return
		}

		logrus.WithFields(logrus.Fields{
			"retention":    schema.RetentionStr,
			"schema":       schema.Name,
			"aggregation":  aggr.name,
			"xFilesFactor": aggr.xFilesFactor,
			"method":       aggr.aggregationMethodStr,
		}).Debugf("[persister] Creating %s", path)

		if err = os.MkdirAll(filepath.Dir(path), os.ModeDir|os.ModePerm); err != nil {
			logrus.Error(err)
			return
		}

		w, err = whisper.CreateWithOptions(path, schema.Retentions, aggr.aggregationMethod, float32(aggr.xFilesFactor), &whisper.Options{
			Sparse: p.sparse,
		})
		if err != nil {
			logrus.Errorf("[persister] Failed to create new whisper file %s: %s", path, err.Error())
			return
		}

		atomic.AddUint32(&p.created, 1)
	}

	points := make([]*whisper.TimeSeriesPoint, len(values.Data))
	for i, r := range values.Data {
		points[i] = &whisper.TimeSeriesPoint{Time: int(r.Timestamp), Value: r.Value}
	}

	atomic.AddUint32(&p.committedPoints, uint32(len(values.Data)))
	atomic.AddUint32(&p.updateOperations, 1)

	defer w.Close()

	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("[persister] UpdateMany %s recovered: %s", path, r)
		}
	}()

	// start = time.Now()
	w.UpdateMany(points)
	// atomic.AddUint64(&p.blockUpdateManyNs, uint64(time.Since(start).Nanoseconds()))
}

func (p *Whisper) worker(recv func(chan bool) *points.Points, confirm func(*points.Points), exit chan bool) {
	storeFunc := store
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
		points := recv(exit)
		// atomic.AddUint64(&p.blockQueueGetNs, uint64(time.Since(start).Nanoseconds()))
		if points == nil {
			// exit closed
			break LOOP
		}
		storeFunc(p, points)
		if doneCb != nil {
			doneCb()
		}
		if confirm != nil {
			confirm(points)
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

	send("updateOperations", float64(updateOperations))
	send("committedPoints", float64(committedPoints))
	if updateOperations > 0 {
		send("pointsPerUpdate", float64(committedPoints)/float64(updateOperations))
	} else {
		send("pointsPerUpdate", 0.0)
	}

	send("created", float64(created))

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

		for i := 0; i < p.workersCount; i++ {
			p.Go(func(exit chan bool) {
				p.worker(p.recv, p.confirm, exit)
			})
		}

		return nil
	})
}

func (p *Whisper) Stop() {
	p.StopFunc(func() {
		p.throttleTicker.Stop()
	})
}
