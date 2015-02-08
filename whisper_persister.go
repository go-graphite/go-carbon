package carbon

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/grobian/go-whisper"
)

// WhisperPersister receive metrics from TCP and UDP sockets
type WhisperPersister struct {
	in          chan *CacheValues
	exit        chan bool
	schemas     *WhisperSchemas
	rootPath    string
	graphPrefix string
	commited    uint64 // (updateOperations << 32) + commitedPoints
	created     uint32 // counter
}

// NewWhisperPersister create instance of WhisperPersister
func NewWhisperPersister(rootPath string, schemas *WhisperSchemas, in chan *CacheValues) *WhisperPersister {
	return &WhisperPersister{
		in:       in,
		exit:     make(chan bool),
		schemas:  schemas,
		rootPath: rootPath,
	}
}

// SetGraphPrefix for internal cache metrics
func (persister *WhisperPersister) SetGraphPrefix(prefix string) {
	persister.graphPrefix = prefix
}

// Stat sends internal statistics to cache
func (persister *WhisperPersister) Stat(metric string, value float64) {
	values := &CacheValues{}
	values.Append(value, time.Now().Unix())
	values.Metric = fmt.Sprintf("%s%s", persister.graphPrefix, metric)

	persister.in <- values
}

func (persister *WhisperPersister) store(values *CacheValues) {
	// @TODO: lock or shard by hash(metricName), no thread safe
	path := filepath.Join(persister.rootPath, strings.Replace(values.Metric, ".", "/", -1)+".wsp")

	w, err := whisper.Open(path)
	if err != nil {
		schema := persister.schemas.match(values.Metric)
		if schema == nil {
			logrus.Errorf("No storage schema defined for %s", values.Metric)
			return
		}

		logrus.Infof("Creating %s: %s, retention %#v (section %#v)",
			values.Metric, path, schema.retentionStr, schema.name)

		if err = os.MkdirAll(filepath.Dir(path), os.ModeDir|os.ModePerm); err != nil {
			logrus.Error(err)
			return
		}

		w, err = whisper.Create(path, schema.retentions, whisper.Last, 0.5)
		if err != nil {
			logrus.Warningf("Failed to create new whisper file %s: %s", path, err.Error())
			return
		}

		atomic.AddUint32(&persister.created, 1)
	}

	points := make([]*whisper.TimeSeriesPoint, len(values.Data))
	for i, r := range values.Data {
		points[i] = &whisper.TimeSeriesPoint{Time: int(r.Timestamp), Value: r.Value}
	}

	w.UpdateMany(points)

	atomic.AddUint64(&persister.commited, (uint64(1)<<32)+uint64(len(values.Data)))

	w.Close()
}

func (persister *WhisperPersister) worker(in chan *CacheValues) {
	for {
		select {
		case <-persister.exit:
			break
		case values := <-in:
			persister.store(values)
		}
	}
}

func (persister *WhisperPersister) statWorker() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-persister.exit:
			break
		case <-ticker.C:
			// @send stat
			cnt := atomic.LoadUint64(&persister.commited)
			atomic.AddUint64(&persister.commited, -cnt)
			persister.Stat("updateOperations", float64(cnt>>32))
			persister.Stat("commitedPoints", float64(cnt%(1<<32)))
			if float64(cnt>>32) > 0 {
				persister.Stat("pointsPerUpdate", float64(cnt%(1<<32))/float64(cnt>>32))
			} else {
				persister.Stat("pointsPerUpdate", 0.0)
			}

			created := atomic.LoadUint32(&persister.created)
			atomic.AddUint32(&persister.created, -created)

			persister.Stat("created", float64(created))
		}
	}
}

// Start worker
func (persister *WhisperPersister) Start() {
	go persister.statWorker()
	go persister.worker(persister.in)
}

// Stop worker
func (persister *WhisperPersister) Stop() {
	close(persister.exit)
}
