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
	"github.com/benbjohnson/clock"
	"github.com/lomik/go-whisper"

	"github.com/lomik/go-carbon/points"
)

type CreateOpener interface {
	Create(string, whisper.Retentions, whisper.AggregationMethod, float32) (*whisper.Whisper, error)
	Open(string) (*whisper.Whisper, error)
}

// Persister is a struct to hold dependencies via interface
type Persister struct {
	Clock   clock.Clock
	Whisper CreateOpener
}

var app = Persister{
	Clock:   clock.New(),
	Whisper: WhisperFactory{},
}

// Whisper write data to *.wsp files
type Whisper struct {
	in           chan *points.Points
	exit         chan bool
	schemas      *WhisperSchemas
	aggregation  *WhisperAggregation
	workersCount int
	rootPath     string
	graphPrefix  string
	commited     uint64 // (updateOperations << 32) + commitedPoints
	created      uint32 // counter
}

// NewWhisper create instance of Whisper
func NewWhisper(rootPath string, schemas *WhisperSchemas, aggregation *WhisperAggregation, in chan *points.Points) *Whisper {
	return &Whisper{
		in:           in,
		exit:         make(chan bool),
		schemas:      schemas,
		aggregation:  aggregation,
		workersCount: 1,
		rootPath:     rootPath,
	}
}

// Holder for factory functions
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

// SetWorkers count
func (p *Whisper) SetWorkers(count int) {
	p.workersCount = count
}

// Stat sends internal statistics to cache
func (p *Whisper) Stat(metric string, value float64) {
	p.in <- points.OnePoint(
		fmt.Sprintf("%spersister.%s", p.graphPrefix, metric),
		value,
		app.Clock.Now().Unix(),
	)
}

func (p *Whisper) store(values *points.Points) {
	// @TODO: lock or shard by hash(metricName), no thread safe
	path := filepath.Join(p.rootPath, strings.Replace(values.Metric, ".", "/", -1)+".wsp")

	w, err := app.Whisper.Open(path)
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

		w, err = app.Whisper.Create(path, schema.retentions, aggr.aggregationMethod, float32(aggr.xFilesFactor))
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

	atomic.AddUint64(&p.commited, (uint64(1)<<32)+uint64(len(values.Data)))

	defer w.Close()

	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("[persister] UpdateMany %s recovered: %s", path, r)
		}
	}()
	w.UpdateMany(points)
}

func (p *Whisper) worker(in chan *points.Points) {
	for {
		select {
		case <-p.exit:
			break
		case values := <-in:
			p.store(values)
		}
	}
}

func (p *Whisper) shuffler(in chan *points.Points, out [](chan *points.Points)) {
	workers := len(out)
	for {
		select {
		case <-p.exit:
			break
		case values := <-in:
			index := int(crc32.ChecksumIEEE([]byte(values.Metric))) % workers
			out[index] <- values
		}
	}
}

// save stat
func (p *Whisper) doCheckpoint() {
	cnt := atomic.LoadUint64(&p.commited)
	atomic.AddUint64(&p.commited, -cnt)

	created := atomic.LoadUint32(&p.created)
	atomic.AddUint32(&p.created, -created)

	logrus.WithFields(logrus.Fields{
		"updateOperations": float64(cnt >> 32),
		"commitedPoints":   float64(cnt % (1 << 32)),
		"created":          created,
	}).Info("[persister] doCheckpoint()")

	p.Stat("updateOperations", float64(cnt>>32))
	p.Stat("commitedPoints", float64(cnt%(1<<32)))
	if float64(cnt>>32) > 0 {
		p.Stat("pointsPerUpdate", float64(cnt%(1<<32))/float64(cnt>>32))
	} else {
		p.Stat("pointsPerUpdate", 0.0)
	}

	p.Stat("created", float64(created))

}

// stat timer
func (p *Whisper) statWorker() {
	ticker := app.Clock.Ticker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.exit:
			break
		case <-ticker.C:
			go p.doCheckpoint()
		}
	}
}

// Start worker
func (p *Whisper) Start() {
	go p.statWorker()
	if p.workersCount <= 1 { // solo worker
		go p.worker(p.in)
	} else {
		var channels [](chan *points.Points)

		for i := 0; i < p.workersCount; i++ {
			ch := make(chan *points.Points, 32)
			channels = append(channels, ch)
			go p.worker(ch)
		}

		go p.shuffler(p.in, channels)
	}
}

// Stop worker
func (p *Whisper) Stop() {
	close(p.exit)
}
