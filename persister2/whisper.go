package persister

import (
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

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
	mockStore           func() (StoreFunc, func())
}

// NewWhisper create instance of Whisper
func NewWhisper(rootPath string, schemas WhisperSchemas, aggregation *WhisperAggregation, recv func(chan bool) *points.Points) *Whisper {
	return &Whisper{
		recv:                in,
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

func store(p *Whisper, values *points.Points) {
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

func (p *Whisper) worker(in chan *points.Points, exit chan bool) {
	storeFunc := store
	var doneCb func()
	if p.mockStore != nil {
		storeFunc, doneCb = p.mockStore()
	}

LOOP:
	for {
		select {
		case <-exit:
			break LOOP
		case values, ok := <-in:
			if !ok {
				break LOOP
			}
			storeFunc(p, values)
			if doneCb != nil {
				doneCb()
			}
		}
	}
}

func (p *Whisper) shuffler(in chan *points.Points, out [](chan *points.Points), exit chan bool) {
	workers := uint32(len(out))

LOOP:
	for {
		select {
		case <-exit:
			break LOOP
		case values, ok := <-in:
			if !ok {
				break LOOP
			}
			index := crc32.ChecksumIEEE([]byte(values.Metric)) % workers
			out[index] <- values
		}
	}

	for _, ch := range out {
		close(ch)
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

func ThrottleChan(in chan *points.Points, ratePerSec int, exit chan bool) chan *points.Points {
	out := make(chan *points.Points, cap(in))

	delimeter := ratePerSec
	chunk := 1

	if ratePerSec > 1000 {
		minRemainder := ratePerSec

		for i := 100; i < 1000; i++ {
			if ratePerSec%i < minRemainder {
				delimeter = i
				minRemainder = ratePerSec % delimeter
			}
		}

		chunk = ratePerSec / delimeter
	}

	step := time.Duration(1e9/delimeter) * time.Nanosecond

	var onceClose sync.Once

	throttleWorker := func() {
		var p *points.Points
		var ok bool

		defer onceClose.Do(func() { close(out) })

		// start flight
		throttleTicker := time.NewTicker(step)
		defer throttleTicker.Stop()

	LOOP:
		for {
			select {
			case <-throttleTicker.C:
				for i := 0; i < chunk; i++ {
					select {
					case p, ok = <-in:
						if !ok {
							break LOOP
						}
					case <-exit:
						break LOOP
					}
					out <- p
				}
			case <-exit:
				break LOOP
			}
		}
	}

	go throttleWorker()

	return out
}

// Start worker
func (p *Whisper) Start() error {

	return p.StartFunc(func() error {

		p.WithExit(func(exitChan chan bool) {

			inChan := p.in

			readerExit := exitChan

			if p.maxUpdatesPerSecond > 0 {
				inChan = ThrottleChan(inChan, p.maxUpdatesPerSecond, exitChan)
				readerExit = nil // read all before channel is closed
			}

			if p.workersCount <= 1 { // solo worker
				p.Go(func(e chan bool) {
					p.worker(inChan, readerExit)
				})
			} else {
				var channels [](chan *points.Points)

				for i := 0; i < p.workersCount; i++ {
					ch := make(chan *points.Points, 32)
					channels = append(channels, ch)
					p.Go(func(e chan bool) {
						p.worker(ch, nil)
					})
				}

				p.Go(func(e chan bool) {
					p.shuffler(inChan, channels, readerExit)
				})
			}

		})

		return nil
	})
}
