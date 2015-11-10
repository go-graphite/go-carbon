package persister

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-whisper"

	"github.com/lomik/go-carbon/points"
)

// Whisper write data to *.wsp files
type Whisper struct {
	updateOperations    uint32
	commitedPoints      uint32
	in                  chan *points.Points
	exit                chan bool
	schemas             *WhisperSchemas
	aggregation         *WhisperAggregation
	metricInterval      time.Duration // checkpoint interval
	workersCount        int
	rootPath            string
	graphPrefix         string
	created             uint32 // counter
	maxUpdatesPerSecond int
}

// NewWhisper create instance of Whisper
func NewWhisper(rootPath string, schemas *WhisperSchemas, aggregation *WhisperAggregation, in chan *points.Points) *Whisper {
	return &Whisper{
		in:                  in,
		exit:                make(chan bool),
		schemas:             schemas,
		aggregation:         aggregation,
		metricInterval:      time.Minute,
		workersCount:        1,
		rootPath:            rootPath,
		maxUpdatesPerSecond: 0,
	}
}

// WhisperFactory - holder for factory functions
type WhisperFactory struct{}

// Create creates a new underlying whisperdb file
func (WhisperFactory) Create(path string, retentions whisper.Retentions, aggregationMethod whisper.AggregationMethod, xFilesFactor float32) (*whisper.Whisper, error) {
	return whisper.Create(path, retentions, aggregationMethod, xFilesFactor)
}

// Open opens an existing underlying whisperdb file
func (WhisperFactory) Open(path string) (*whisper.Whisper, error) {
	return whisper.Open(path)
}

// SetGraphPrefix for internal cache metrics
func (p *Whisper) SetGraphPrefix(prefix string) {
	p.graphPrefix = prefix
}

// SetMaxUpdatesPerSecond enable throttling
func (p *Whisper) SetMaxUpdatesPerSecond(maxUpdatesPerSecond int) {
	p.maxUpdatesPerSecond = maxUpdatesPerSecond
}

// SetWorkers count
func (p *Whisper) SetWorkers(count int) {
	p.workersCount = count
}

// SetMetricInterval sets doChekpoint interval
func (p *Whisper) SetMetricInterval(interval time.Duration) {
	p.metricInterval = interval
}

// Stat sends internal statistics to cache
func (p *Whisper) Stat(metric string, value float64) {
	p.in <- points.OnePoint(
		fmt.Sprintf("%spersister.%s", p.graphPrefix, metric),
		value,
		time.Now().Unix(),
	)
}

func (p *Whisper) store(values *points.Points) {
	path := filepath.Join(p.rootPath, strings.Replace(values.Metric, ".", "/", -1)+".wsp")

	w, err := whisper.Open(path)
	if err != nil {
		schema := p.schemas.match(values.Metric)
		if schema == nil {
			logrus.Errorf("[persister] No storage schema defined for %s", values.Metric)
			return
		}

		aggr := p.aggregation.match(values.Metric)
		if aggr == nil {
			logrus.Errorf("[persister] No storage aggregation defined for %s", values.Metric)
			return
		}

		logrus.WithFields(logrus.Fields{
			"retention":    schema.retentionStr,
			"schema":       schema.name,
			"aggregation":  aggr.name,
			"xFilesFactor": aggr.xFilesFactor,
			"method":       aggr.aggregationMethodStr,
		}).Debugf("[persister] Creating %s", path)

		if err = os.MkdirAll(filepath.Dir(path), os.ModeDir|os.ModePerm); err != nil {
			logrus.Error(err)
			return
		}

		w, err = whisper.Create(path, schema.retentions, aggr.aggregationMethod, float32(aggr.xFilesFactor))
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

	atomic.AddUint32(&p.commitedPoints, uint32(len(values.Data)))
	atomic.AddUint32(&p.updateOperations, 1)

	defer w.Close()

	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("[persister] UpdateMany %s recovered: %s", path, r)
		}
	}()
	w.UpdateMany(points)
}

func (p *Whisper) worker(in chan *points.Points) {
LOOP:
	for {
		select {
		case <-p.exit:
			break LOOP
		case values := <-in:
			p.store(values)
		}
	}
}

func (p *Whisper) shuffler(in chan *points.Points, out [](chan *points.Points)) {
	workers := uint32(len(out))

LOOP:
	for {
		select {
		case <-p.exit:
			break LOOP
		case values := <-in:
			index := crc32.ChecksumIEEE([]byte(values.Metric)) % workers
			out[index] <- values
		}
	}
}

// save stat
func (p *Whisper) doCheckpoint() {
	updateOperations := atomic.LoadUint32(&p.updateOperations)
	commitedPoints := atomic.LoadUint32(&p.commitedPoints)
	atomic.AddUint32(&p.updateOperations, -updateOperations)
	atomic.AddUint32(&p.commitedPoints, -commitedPoints)

	created := atomic.LoadUint32(&p.created)
	atomic.AddUint32(&p.created, -created)

	logrus.WithFields(logrus.Fields{
		"updateOperations": float64(updateOperations),
		"commitedPoints":   float64(commitedPoints),
		"created":          created,
	}).Info("[persister] doCheckpoint()")

	p.Stat("updateOperations", float64(updateOperations))
	p.Stat("commitedPoints", float64(commitedPoints))
	if updateOperations > 0 {
		p.Stat("pointsPerUpdate", float64(commitedPoints)/float64(updateOperations))
	} else {
		p.Stat("pointsPerUpdate", 0.0)
	}

	p.Stat("created", float64(created))

}

// stat timer
func (p *Whisper) statWorker() {
	ticker := time.NewTicker(p.metricInterval)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-p.exit:
			break LOOP
		case <-ticker.C:
			go p.doCheckpoint()
		}
	}
}

// Start worker
func (p *Whisper) Start() {
	go p.statWorker()

	inChan := p.in
	if p.maxUpdatesPerSecond > 0 {
		inChan = points.ThrottleChan(inChan, p.maxUpdatesPerSecond)
	}

	if p.workersCount <= 1 { // solo worker
		go p.worker(inChan)
	} else {
		var channels [](chan *points.Points)

		for i := 0; i < p.workersCount; i++ {
			ch := make(chan *points.Points, 32)
			channels = append(channels, ch)
			go p.worker(ch)
		}

		go p.shuffler(inChan, channels)
	}
}

// Stop worker
func (p *Whisper) Stop() {
	close(p.exit)
}
