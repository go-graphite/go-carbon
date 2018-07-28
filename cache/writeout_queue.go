package cache

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/zapwriter"
)

type WriteoutQueue struct {
	sync.RWMutex
	cache *Cache

	// Writeout queue. Usage:
	// q := <- queue
	// p := cache.Pop(q.Metric)
	queue   chan string
	rebuild func(abort chan bool) chan bool // return chan waiting for complete
}

func NewWriteoutQueue(cache *Cache) *WriteoutQueue {
	q := &WriteoutQueue{
		cache: cache,
		queue: nil,
	}
	q.rebuild = q.makeRebuildCallback(time.Time{})
	return q
}

func (q *WriteoutQueue) makeRebuildCallback(nextRebuildTime time.Time) func(chan bool) chan bool {
	var nextRebuildOnce sync.Once
	nextRebuildComplete := make(chan bool)

	nextRebuild := func(abort chan bool) chan bool {
		// next rebuild
		nextRebuildOnce.Do(func() {
			now := time.Now()
			logger := zapwriter.Logger("cache")

			logger.Debug("WriteoutQueue.nextRebuildOnce.Do",
				zap.String("now", now.String()),
				zap.String("next", nextRebuildTime.String()),
			)
			if now.Before(nextRebuildTime) {
				sleepTime := nextRebuildTime.Sub(now)
				logger.Debug("WriteoutQueue sleep before rebuild",
					zap.String("sleepTime", sleepTime.String()),
				)

				select {
				case <-time.After(sleepTime):
					// pass
				case <-abort:
					// pass
				}
			}
			q.update()
			close(nextRebuildComplete)
		})

		return nextRebuildComplete
	}

	return nextRebuild
}

func (q *WriteoutQueue) update() {
	queue := q.cache.makeQueue()

	q.Lock()
	q.queue = queue
	q.rebuild = q.makeRebuildCallback(time.Now().Add(100 * time.Millisecond))
	q.Unlock()
}

func (q *WriteoutQueue) get(abort chan bool) string {
QueueLoop:
	for {
		q.RLock()
		queue := q.queue
		rebuild := q.rebuild
		q.RUnlock()

		select {
		case metric := <-queue:
			// pop from cache
			return metric
		case <-abort:
			return ""
		default:
			// queue is empty, create new
			select {
			case <-rebuild(abort):
				// wait for rebuild
				continue QueueLoop
			case <-abort:
				return ""
			}
		}
	}
}

func (q *WriteoutQueue) Get(abort chan bool) string {
	return q.get(abort)
}
