package persister

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-whisper"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

type StoreFunc func(p *Whisper, metric string, values []*whisper.TimeSeriesPoint) error

// Whisper write data to *.wsp files
type Whisper struct {
	helper.Stoppable
	updateOperations    uint32
	committedPoints     uint32
	schemas             WhisperSchemas
	aggregation         *WhisperAggregation
	workersCount        int
	rootPath            string
	created             uint32 // counter
	sparse              bool
	maxUpdatesPerSecond int
	mockStore           func() (StoreFunc, func())
	dispatchChan        <-chan []*points.Points
	process             points.BatchProcessFunc
}

// NewWhisper create instance of Whisper
func NewWhisper(rootPath string, schemas WhisperSchemas, aggregation *WhisperAggregation, dispatchChan <-chan []*points.Points, process points.BatchProcessFunc) *Whisper {
	return &Whisper{
		schemas:             schemas,
		aggregation:         aggregation,
		workersCount:        1,
		rootPath:            rootPath,
		maxUpdatesPerSecond: 0,
		dispatchChan:        dispatchChan,
		process:             process,
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

// SeMockStore
func (p *Whisper) SetMockStore(fn func() (StoreFunc, func())) {
	p.mockStore = fn
}

func store(p *Whisper, metric string, values []*whisper.TimeSeriesPoint) (err error) {
	path := filepath.Join(p.rootPath, strings.Replace(metric, ".", "/", -1)+".wsp")

	w, err := whisper.Open(path)
	if err != nil {
		// create new whisper if file not exists
		if !os.IsNotExist(err) {
			logrus.Errorf("[persister] Failed to open whisper file %s: %s", path, err.Error())
			return err
		}

		schema, ok := p.schemas.Match(metric)
		if !ok {
			logrus.Errorf("[persister] No storage schema defined for %s", metric)
			return err
		}

		aggr := p.aggregation.match(metric)
		if aggr == nil {
			logrus.Errorf("[persister] No storage aggregation defined for %s", metric)
			return err
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
			return err
		}

		w, err = whisper.CreateWithOptions(path, schema.Retentions, aggr.aggregationMethod, float32(aggr.xFilesFactor), &whisper.Options{
			Sparse: p.sparse,
		})
		if err != nil {
			logrus.Errorf("[persister] Failed to create new whisper file %s: %s", path, err.Error())
			return err
		}

		atomic.AddUint32(&p.created, 1)
	}
	defer w.Close()

	atomic.AddUint32(&p.committedPoints, uint32(len(values)))
	atomic.AddUint32(&p.updateOperations, 1)

	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("[persister] UpdateMany %s recovered: %s", path, r)
		}
	}()
	return w.UpdateMany(values)
}

func (p *Whisper) worker(exit chan bool) {
	storeFunc := store
	var batchDoneCb func()

	if p.mockStore != nil {
		storeFunc, batchDoneCb = p.mockStore()
	}

	limiter := helper.NewRatelimiter()
	var rps int = 0
	if p.maxUpdatesPerSecond > 0 {
		rps = p.maxUpdatesPerSecond/p.workersCount + 1
	}

	processCallback := func(v *points.Points) error {
		// v is passed locked
		// under lock copy all the points we are going to writeout
		numPoints := len(v.Data)
		whisperPoints := make([]*whisper.TimeSeriesPoint, numPoints)

		for i := 0; i < numPoints; i++ {
			whisperPoints[i] = (*whisper.TimeSeriesPoint)(unsafe.Pointer(&v.Data[i]))
		}
		v.Unlock()

		if rps != 0 {
			limiter.TickSleep(rps)
		}
		return storeFunc(p, v.Metric, whisperPoints)
	}
LOOP:
	for {
		select {
		case batch := <-p.dispatchChan:
			p.process(batch, processCallback)
			if batchDoneCb != nil {
				batchDoneCb()
			}
		case <-exit:
			break LOOP
		}
	}
}

// Stat callback
func (p *Whisper) Stat(send func(metric string, value float64)) {
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
		for i := 0; i < p.workersCount; i++ {
			p.Go(func(e chan bool) {
				p.worker(e)
			})
		}

		return nil
	})
}
