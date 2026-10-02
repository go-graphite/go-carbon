package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

type WriteoutQueue struct {
	sync.RWMutex
	cache *Cache

	// Writeout queue. Usage:
	// q := <- queue
	// p := cache.Pop(q.Metric)
	queue   chan string
	rebuild func(abort chan bool) chan bool // return chan waiting for complete
	wake    chan struct{}
	// Monotonic nanoseconds relative to epoch, plus one (zero means unset).
	wakeAt atomic.Int64
	epoch  time.Time
}

func NewWriteoutQueue(cache *Cache) *WriteoutQueue {
	q := &WriteoutQueue{
		cache: cache,
		queue: nil,
		wake:  make(chan struct{}, 1),
		epoch: time.Now(),
	}
	q.rebuild = q.makeRebuildCallback(time.Now(), time.Time{})
	return q
}

// notifyAt coalesces arrivals into one shared wake-up. Sparse arrivals only
// advance the timer; they do not cause repeated full-cache scans.
func (q *WriteoutQueue) notifyAt(deadline time.Time) {
	next := deadline.Sub(q.epoch).Nanoseconds() + 1
	for {
		old := q.wakeAt.Load()
		if old != 0 && old <= next {
			return
		}
		if q.wakeAt.CompareAndSwap(old, next) {
			select {
			case q.wake <- struct{}{}:
			default:
			}
			return
		}
	}
}

func (q *WriteoutQueue) makeRebuildCallback(nextRebuildTime, notBefore time.Time) func(chan bool) chan bool {
	var nextRebuildOnce sync.Once
	nextRebuildComplete := make(chan bool)

	nextRebuild := func(abort chan bool) chan bool {
		// next rebuild
		nextRebuildOnce.Do(func() {
			go func() {
				q.waitForRebuild(nextRebuildTime, notBefore, abort)
				q.update()
				close(nextRebuildComplete)
			}()
		})

		return nextRebuildComplete
	}

	return nextRebuild
}

func (q *WriteoutQueue) waitForRebuild(next, notBefore time.Time, abort chan bool) {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		deadline := next
		if hint := q.wakeAt.Load(); hint != 0 {
			wake := q.epoch.Add(time.Duration(hint - 1))
			if deadline.IsZero() || wake.Before(deadline) {
				deadline = wake
			}
		}
		var tick <-chan time.Time
		if !deadline.IsZero() {
			if deadline.Before(notBefore) {
				deadline = notBefore
			}
			delay := time.Until(deadline)
			if delay <= 0 {
				return
			}
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				timer.Reset(delay)
			}
			tick = timer.C
		}
		select {
		case <-tick:
			return
		case <-abort:
			return
		case <-q.wake:
			if timer != nil {
				timer.Stop()
			}
		}
	}
}

func (q *WriteoutQueue) update() {
	// Clear hints before scanning so concurrent arrivals cannot lose a wake-up.
	q.wakeAt.Store(0)
	queue, next := q.cache.makeQueueAt(time.Now())
	notBefore := time.Now().Add(100 * time.Millisecond)
	if queue != nil || !q.cache.settings.Load().(*cacheSettings).batching() {
		next = notBefore
	}

	q.Lock()
	q.queue = queue
	q.rebuild = q.makeRebuildCallback(next, notBefore)
	q.Unlock()
}

func (q *WriteoutQueue) get(abort chan bool) string {
QueueLoop:
	for {
		select {
		case <-abort:
			return ""
		default:
		}
		q.RLock()
		queue := q.queue
		rebuild := q.rebuild
		q.RUnlock()

		select {
		case metric := <-queue:
			if !q.cache.WriteoutReady(metric) {
				continue QueueLoop
			}
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

func (q *WriteoutQueue) Get(abort chan bool) string { // skipcq: RVV-B0001
	return q.get(abort)
}
