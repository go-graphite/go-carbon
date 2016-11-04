package cache

import (
	"sync"

	"github.com/lomik/go-carbon/points"
)

type WriteoutQueue struct {
	sync.RWMutex
	cache *Cache

	// Writeout queue. Usage:
	// q := <- queue
	// p := cache.Pop(q.Metric)
	queue   chan *points.Points
	rebuild func() chan bool // return chan waiting for complete
}

func NewWriteoutQueue(cache *Cache) *WriteoutQueue {
	q := &WriteoutQueue{
		cache: cache,
		queue: nil,
	}
	q.rebuild = q.makeRebuildCallback()
	return q
}

func (q *WriteoutQueue) makeRebuildCallback() func() chan bool {
	var nextRebuildOnce sync.Once
	nextRebuildComplete := make(chan bool)

	nextRebuild := func() chan bool {
		// next rebuild
		nextRebuildOnce.Do(func() {
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
	q.rebuild = q.makeRebuildCallback()
	q.Unlock()
}

func (q *WriteoutQueue) Get(abort chan bool) *points.Points {
QueueLoop:
	for {
		q.RLock()
		queue := q.queue
		rebuild := q.rebuild
		q.RUnlock()

	FetchLoop:
		for {
			select {
			case qp := <-queue:
				// pop from cache
				if p, exists := q.cache.Pop(qp.Metric); exists {
					return p
				}
				continue FetchLoop
			case <-abort:
				return nil
			default:
				// queue is empty, create new
				select {
				case <-rebuild():
					// wait for rebuild
					continue QueueLoop
				case <-abort:
					return nil
				}
			}
		}
	}
}
