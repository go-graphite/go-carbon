package tags

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"go.uber.org/zap"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/zapwriter"
)

type Queue struct {
	helper.Stoppable
	db        *leveldb.DB
	logger    *zap.Logger
	changed   chan struct{}
	send      func([]string) error
	sendChunk int

	stat struct {
		putErrors    uint32
		putCount     uint32
		deleteErrors uint32
		deleteCount  uint32
		sendFail     uint32
		sendSuccess  uint32
	}
}

// exists returns whether the given file or directory exists or not
func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func NewQueue(rootPath string, send func([]string) error, sendChunk int) (*Queue, error) {
	if send == nil {
		return nil, fmt.Errorf("send callback not set")
	}

	o := &opt.Options{}

	queueDir := filepath.Join(rootPath, "queue")

	db, err := leveldb.OpenFile(queueDir, o)

	logger := zapwriter.Logger("tags").With(zap.String("path", queueDir))

	if err != nil {
		logger.Error("can't open queue database",
			zap.Error(err),
		)

		if !exists(queueDir) {
			return nil, err
		}

		// try to recover
		logger.Info("queue directory exists, try to recover")
		moveTo := filepath.Join(rootPath, fmt.Sprintf("queue_corrupted_%d", time.Now().UnixNano()))
		err = os.Rename(queueDir, moveTo)
		if err != nil {
			logger.Error("move corrupted queue failed",
				zap.String("dst", moveTo),
				zap.Error(err),
			)

			return nil, err
		}

		// corrupted database moved, open new
		db, err = leveldb.OpenFile(queueDir, o)
		if err != nil {
			logger.Error("can't create new queue database",
				zap.Error(err),
			)
			return nil, err
		}
	}

	if sendChunk < 1 {
		sendChunk = 1
	}

	q := &Queue{
		db:        db,
		logger:    logger,
		changed:   make(chan struct{}, 1),
		send:      send,
		sendChunk: sendChunk,
	}

	q.Start()

	q.Go(q.sendWorker)

	return q, nil
}

func (q *Queue) Stop() {
	q.StopFunc(func() {})
	q.db.Close()
}

func (q *Queue) Add(metric string) {
	// skip not tagged data
	if strings.IndexByte(metric, ';') < 0 {
		return
	}

	key := make([]byte, len(metric)+8)

	binary.BigEndian.PutUint64(key[:8], uint64(time.Now().UnixNano()))
	copy(key[8:], metric)

	err := q.db.Put(key, []byte("{}"), nil)
	atomic.AddUint32(&q.stat.putCount, 1)

	if err != nil {
		atomic.AddUint32(&q.stat.putErrors, 1)
		q.logger.Error("write to queue database failed", zap.Error(err))
	}

	select {
	case q.changed <- struct{}{}:
		// pass
	default:
		// pass
	}
}

func (q *Queue) Lag() time.Duration {
	iter := q.db.NewIterator(nil, nil)

	var res time.Duration

	if iter.Next() {
		key := iter.Key()
		if len(key) >= 8 {
			t := int64(binary.BigEndian.Uint64(key[:8]))
			tm := time.Unix(t/1000000000, t%1000000000)
			res = time.Since(tm)
		}
	}

	iter.Release()

	return res
}

func (q *Queue) sendAll(exit chan bool) {
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()

	keys := make([][]byte, q.sendChunk)
	series := make([]string, q.sendChunk)
	used := 0

	flush := func() error {
		if used <= 0 {
			return nil
		}

		for i := 0; i < used; i++ {
			series[i] = string(keys[i][8:])
		}

		err := q.send(series[:used])

		if err != nil {
			atomic.AddUint32(&q.stat.sendFail, uint32(used))
			used = 0
			return err
		}

		atomic.AddUint32(&q.stat.sendSuccess, uint32(used))

		for i := 0; i < used; i++ {
			q.delete(keys[i])
		}
		used = 0
		return nil
	}

	for iter.Next() {
		select {
		case <-exit:
			return
		default:
		}

		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		// value := iter.Value()
		if len(key) < 9 {
			q.delete(key)
			continue
		}

		keys[used] = make([]byte, len(key))
		copy(keys[used], key)
		used++

		if used >= q.sendChunk {
			err := flush()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
	}

	flush()
}

func (q *Queue) sendWorker(exit chan bool) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-exit:
			return
		case <-q.changed:
			// pass
		case <-t.C:
			//pass
		}
		q.sendAll(exit)
	}
}

// Remove key from queue
func (q *Queue) delete(key []byte) {
	err := q.db.Delete([]byte(key), nil)
	atomic.AddUint32(&q.stat.deleteCount, 1)
	if err != nil {
		atomic.AddUint32(&q.stat.deleteErrors, 1)
		q.logger.Error("delete from queue database failed", zap.Error(err))
	}
}
