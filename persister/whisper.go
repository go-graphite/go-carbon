package persister

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/go-carbon/points"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-whisper"
)

// Whisper write data to *.wsp files
type Whisper struct {
	in           chan *points.Points
	exit         chan bool
	schemas      *WhisperSchemas
	workersCount int
	rootPath     string
	graphPrefix  string
	commited     uint64 // (updateOperations << 32) + commitedPoints
	created      uint32 // counter
}

// NewWhisper create instance of Whisper
func NewWhisper(rootPath string, schemas *WhisperSchemas, in chan *points.Points) *Whisper {
	return &Whisper{
		in:           in,
		exit:         make(chan bool),
		schemas:      schemas,
		rootPath:     rootPath,
		workersCount: 1,
	}
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
		time.Now().Unix(),
	)
}

func (p *Whisper) store(values *points.Points) {
	// @TODO: lock or shard by hash(metricName), no thread safe
	path := filepath.Join(p.rootPath, strings.Replace(values.Metric, ".", "/", -1)+".wsp")

	w, err := whisper.Open(path)
	if err != nil {
		schema := p.schemas.match(values.Metric)
		if schema == nil {
			logrus.Errorf("[persister] No storage schema defined for %s", values.Metric)
			return
		}

		logrus.Infof("[persister] Creating %s: retention %#v (section %#v)",
			path, schema.retentionStr, schema.name)

		if err = os.MkdirAll(filepath.Dir(path), os.ModeDir|os.ModePerm); err != nil {
			logrus.Error(err)
			return
		}

		w, err = whisper.Create(path, schema.retentions, whisper.Average, 0.5)
		if err != nil {
			logrus.Warningf("[persister] Failed to create new whisper file %s: %s", path, err.Error())
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
			logrus.Warningf("[persister] UpdateMany %s recovered: %s", path, r)
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
	ticker := time.NewTicker(time.Minute)
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
