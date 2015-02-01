package carbon

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/grobian/go-whisper"
)

// WhisperPersister receive metrics from TCP and UDP sockets
type WhisperPersister struct {
	in       chan *CacheValues
	exit     chan bool
	schemas  *WhisperSchemas
	rootPath string
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

func (persister *WhisperPersister) store(values *CacheValues) {
	// @TODO: lock, no thread safe
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
	}

	for _, r := range values.Data {
		w.Update(r.Value, int(r.Timestamp))
	}
	w.Close()
	//persister.schemas.Match(values.Metric)
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

// Start worker
func (persister *WhisperPersister) Start() {
	go persister.worker(persister.in)
}

// Stop worker
func (persister *WhisperPersister) Stop() {
	close(persister.exit)
}
