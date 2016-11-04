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

const storeMutexCount = 2048

type StoreFunc func(p *Whisper, values *points.Points)

// Whisper write data to *.wsp files
type Whisper struct {
	helper.Stoppable
	updateOperations    uint32
	committedPoints     uint32
	recv                func(chan bool) *points.Points
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
}

// NewWhisper create instance of Whisper
func NewWhisper(rootPath string, schemas WhisperSchemas, aggregation *WhisperAggregation, recv func(chan bool) *points.Points) *Whisper {
	return &Whisper{
		recv:                recv,
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
	p.workersCount = count
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
	mutexIndex := fnv32(values.Metric) % storeMutexCount
	p.storeMutex[mutexIndex].Lock()
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
	w.UpdateMany(points)
}

func (p *Whisper) worker(recv func(chan bool) *points.Points, exit chan bool) {
	storeFunc := store
	var doneCb func()
	if p.mockStore != nil {
		storeFunc, doneCb = p.mockStore()
	}

LOOP:
	for {
		select {
		case <-p.throttleTicker.C:
			// pass
		case <-exit:
			return
		}

		points := recv(exit)
		if points == nil {
			// exit closed
			break LOOP
		}
		storeFunc(p, points)
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

	send("updateOperations", float64(updateOperations))
	send("committedPoints", float64(committedPoints))
	if updateOperations > 0 {
		send("pointsPerUpdate", float64(committedPoints)/float64(updateOperations))
	} else {
		send("pointsPerUpdate", 0.0)
	}

	send("created", float64(created))

}

// Start worker
func (p *Whisper) Start() error {
	return p.StartFunc(func() error {
		p.WithExit(func(exitChan chan bool) {
			p.throttleTicker = NewThrottleTicker(p.maxUpdatesPerSecond)

			p.Go(func(exit chan bool) {
				p.worker(p.recv, exit)
			})

		})

		return nil
	})
}

func (p *Whisper) Stop() {
	p.StopFunc(func() {
		p.throttleTicker.Stop()
	})
}
